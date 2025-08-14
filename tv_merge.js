// tv_merge.js — channels-first; programmes sorted by channel+start with dedupe
// - Streams .xml and .xml.gz inputs (folder or explicit list)
// - Channels are output first, then per-channel programmes sorted by start
// - Preserves empty elements as self-closing (<new /> etc.) with pretty indent
// - Low-memory per-channel external sort (chunked JSONL + k-way merge)
// - Dedupe by (channel,start): --dedupe first|last
// - Robust options for Windows/Linux paths, temp location, and malformed inputs
// - Progress bar driven by BYTES READ (moves immediately), logs each file

'use strict';

const fs = require('fs');
const os = require('os');
const path = require('path');
const { createReadStream } = require('fs');
const zlib = require('zlib');
const sax = require('sax');
const readline = require('readline');
const { Command } = require('commander');
const cliProgress = require('cli-progress');

// ---------- Tunables ----------
const CHUNK_LINES = 50000; // per-channel sort chunk size (lower = lower RAM, more temp files)

// ---------- CLI ----------
const program = new Command();
program
  .option('-i, --input <file...>', 'Input XMLTV file(s), space-separated or use --folder')
  .option('-f, --folder <dir>', 'Directory to merge all .xml/.xml.gz files from')
  .requiredOption('-o, --output <file>', 'Output XMLTV file (.xml or .xml.gz)')
  .option('-t, --doctype', 'Add DOCTYPE to output file')
  .option('--gzip', 'Force gzip output regardless of extension')
  .option('--dedupe <mode>', 'Deduplicate programmes per channel by start: first|last', 'first')
  .option('--tmp-dir <dir>', 'Directory for temporary files (defaults to os.tmpdir())')
  .option('--lenient', 'Tolerate minor XML errors (resume parsing on recoverable errors)')
  .option('--skip-bad', 'On fatal XML parse error in a file, skip the rest of that file instead of aborting')
  .option('--debug-scan', 'Log first 20 element names per file (for diagnosing parsing)')
  .option('-q, --quiet', 'Suppress console output')
  .parse(process.argv);

const options = program.opts();

// ---------- Temp base ----------
const TMP_BASE = (() => {
  const cand = options.tmpDir || process.env.TV_MERGE_TMP || process.env.TEMP || process.env.TMP || os.tmpdir();
  try { if (!fs.existsSync(cand)) fs.mkdirSync(cand, { recursive: true }); } catch (_) {}
  return cand;
})();

// ---------- State ----------
const channelXml = new Map();       // channelId -> raw <channel> xml
const channelProgFiles = new Map(); // channelId -> { path, ws }
let totalProgrammes = 0;            // pre-dedupe for info
let totalChannels = 0;
let bytesReadTotal = 0;             // for byte-accurate progress

// ---------- Helpers ----------
function escapeXml(str) {
  if (str == null) return '';
  return String(str).replace(/[<>&'"\u0000-\u001F]/g, (c) => {
    switch (c) {
      case '<': return '&lt;';
      case '>': return '&gt;';
      case '&': return '&amp;';
      case '"': return '&quot;';
      case "'": return '&apos;';
      default: return '';
    }
  });
}

function localName(qname) { // strip namespace prefix if present
  if (!qname) return '';
  const i = qname.indexOf(':');
  return i === -1 ? qname : qname.slice(i + 1);
}

function san(id) { return String(id ?? '').replace(/[^A-Za-z0-9_.-]/g, '_'); }

function getProgWriter(channelId) {
  if (!channelProgFiles.has(channelId)) {
    const tmpPath = path.join(TMP_BASE, `tvmerge-${process.pid}-${Date.now()}-${san(channelId)}.jsonl`);
    const ws = fs.createWriteStream(tmpPath, { encoding: 'utf8' });
    channelProgFiles.set(channelId, { path: tmpPath, ws });
  }
  return channelProgFiles.get(channelId).ws;
}

function looksGzip(file) {
  try {
    const fd = fs.openSync(file, 'r');
    const buf = Buffer.alloc(2);
    const r = fs.readSync(fd, buf, 0, 2, 0);
    fs.closeSync(fd);
    return r === 2 && buf[0] === 0x1f && buf[1] === 0x8b; // gzip magic
  } catch { return false; }
}

// ---------- Streaming parse per file ----------
function streamFile(file, progressBar) {
  return new Promise((resolve, reject) => {
    const strict = !options.lenient;
    const parser = sax.createStream(strict, { trim: true });

    let capType = null; // 'channel' | 'programme' | null
    let rootOpen = '';
    let pieces = [];    // array of strings (parts of XML)
    let stack = [];     // [{ name, idx, hasContent }]
    let currentChannelId = '';
    let currentProgChannel = '';
    let currentProgStart = '';

    let seenAny = false;
    let debugLeft = options.debugScan ? 20 : 0;

    function openTag(name, attrs) {
      const attrStr = Object.entries(attrs || {})
        .map(([k, v]) => ` ${k}="${escapeXml(String(v))}` + '"')
        .join('');
      return `<${name}${attrStr}>`;
    }

    function pushOpen(name, attrs) {
      const indent = '\n' + '    ' + '  '.repeat(stack.length);
      const s = indent + openTag(name, attrs);
      pieces.push(s);
      stack.push({ name, idx: pieces.length - 1, hasContent: false });
    }

    function pushClose(name) { pieces.push(`</${name}>`); }
    function markParentHasContent() { if (stack.length > 1) stack[stack.length - 2].hasContent = true; }

    function resetCapture() {
      capType = null; rootOpen = ''; pieces = []; stack = [];
      currentChannelId = ''; currentProgChannel = ''; currentProgStart = '';
    }

    parser.on('opentag', (node) => {
      const lname = localName(node.name);
      if (debugLeft > 0) { console.log(`[debug] element: ${node.name}`); debugLeft--; }

      if (capType === null && lname === 'channel') {
        seenAny = true;
        capType = 'channel';
        currentChannelId = node.attributes.id || '';
        rootOpen = `  ${openTag('channel', { id: currentChannelId })}`;
        pieces = []; stack = []; return;
      }
      if (capType === null && lname === 'programme') {
        seenAny = true;
        capType = 'programme';
        currentProgChannel = node.attributes.channel || '';
        currentProgStart = node.attributes.start || '';
        rootOpen = `  ${openTag('programme', { start: currentProgStart, stop: node.attributes.stop || '', channel: currentProgChannel })}`;
        pieces = []; stack = []; return;
      }
      if (capType) { pushOpen(node.name, node.attributes); markParentHasContent(); }
    });

    parser.on('text', (text) => {
      if (!capType) return;
      if (text && text.length) { if (stack.length) stack[stack.length - 1].hasContent = true; pieces.push(escapeXml(text)); }
    });

    parser.on('closetag', () => {
      if (!capType) return;
      if (stack.length) {
        const el = stack.pop();
        if (!el.hasContent) pieces[el.idx] = pieces[el.idx].replace(/>$/, ' />'); else pushClose(el.name);
        return;
      }
      if (capType === 'channel') {
        const xml = `${rootOpen}${pieces.join('')}\n  </channel>\n`;
        if (!channelXml.has(currentChannelId)) { channelXml.set(currentChannelId, xml); totalChannels++; }
      } else if (capType === 'programme') {
        const xml = `${rootOpen}${pieces.join('')}\n  </programme>\n`;
        const ws = getProgWriter(currentProgChannel);
        ws.write(JSON.stringify({ start: currentProgStart, xml }) + '\n');
        totalProgrammes++;
        if (progressBar) progressBar.update(bytesReadTotal, { value: totalProgrammes, mb: (bytesReadTotal/1048576).toFixed(1) });
      }
      resetCapture();
    });

    parser.on('error', (err) => {
      const msg = `[sax] ${file}: ${err && err.message ? err.message : String(err)}`;
      if (!options.quiet) console.warn(msg);
      resetCapture();
      if (!strict) {
        try { parser._parser.error = null; parser._parser.resume(); } catch (_) {}
        return; // continue this file
      }
      if (options.skipBad) {
        try { src && src.destroy && src.destroy(); } catch (_) {}
        resolve();
      } else {
        reject(err);
      }
    });

    parser.on('end', () => {
      if (!seenAny && !options.quiet) console.warn(`[warn] No channels/programmes found in: ${file}`);
      resolve();
    });

    // raw source stream (for byte-accurate progress)
    const src = createReadStream(file);
    src.on('data', (chunk) => {
      bytesReadTotal += chunk.length;
      if (progressBar) progressBar.update(bytesReadTotal, { value: totalProgrammes, mb: (bytesReadTotal/1048576).toFixed(1) });
    });
    src.on('error', (e) => {
      if (!options.quiet) console.warn(`[read] ${file}: ${e.message}`);
      if (options.skipBad) return resolve();
      return reject(e);
    });

    let stream = src;
    // auto-detect gzip by magic, not just extension
    if (file.endsWith('.gz') || looksGzip(file)) {
      const gunzip = zlib.createGunzip();
      gunzip.on('error', (e) => {
        if (!options.quiet) console.warn(`[gunzip] ${file}: ${e.message}`);
        if (options.skipBad) { try { src.destroy(); } catch {} return resolve(); }
        return reject(e);
      });
      stream = src.pipe(gunzip);
    }

    stream.pipe(parser);
  });
}

// ---------- External sort utilities ----------
async function* readLines(filePath) {
  const rl = readline.createInterface({ input: fs.createReadStream(filePath, { encoding: 'utf8' }), crlfDelay: Infinity });
  for await (const line of rl) yield line;
}

async function sortChannelFileToChunks(filePath) {
  const chunks = [];
  let buf = [];
  for await (const line of readLines(filePath)) {
    if (!line) continue;
    try { buf.push(JSON.parse(line)); } catch { continue; }
    if (buf.length >= CHUNK_LINES) {
      buf.sort((a, b) => String(a.start).localeCompare(String(b.start)));
      const chunkPath = `${filePath}.${chunks.length}.chunk`;
      const ws = fs.createWriteStream(chunkPath, { encoding: 'utf8' });
      for (const it of buf) ws.write(JSON.stringify(it) + '\n');
      await new Promise((res) => ws.end(res));
      chunks.push(chunkPath);
      buf = [];
    }
  }
  if (buf.length) {
    buf.sort((a, b) => String(a.start).localeCompare(String(b.start)));
    const chunkPath = `${filePath}.${chunks.length}.chunk`;
    const ws = fs.createWriteStream(chunkPath, { encoding: 'utf8' });
    for (const it of buf) ws.write(JSON.stringify(it) + '\n');
    await new Promise((res) => ws.end(res));
    chunks.push(chunkPath);
  }
  return chunks;
}

async function kWayMergeChunks(chunks, output, dedupeMode = 'first', onWrite = () => {}) {
  // open readers for each chunk
  const readers = await Promise.all(
    chunks.map(async (p) => ({ p, iter: readLines(p)[Symbol.asyncIterator](), current: null }))
  );

  async function load(i) {
    const r = readers[i];
    const n = await r.iter.next();
    if (n.done) { r.current = null; return; }
    try { r.current = JSON.parse(n.value); } catch { r.current = null; await load(i); }
  }

  for (let i = 0; i < readers.length; i++) await load(i);

  // dedupe buffer: items are sorted by start so duplicates are adjacent
  let pending = null; // { start, xml }
  const flushPending = async () => {
    if (pending) { output.write(pending.xml); onWrite(); pending = null; }
  };

  while (true) {
    let minIdx = -1; let minVal = null;
    for (let i = 0; i < readers.length; i++) {
      const cur = readers[i].current; if (!cur) continue;
      if (!minVal || String(cur.start).localeCompare(String(minVal.start)) < 0) { minVal = cur; minIdx = i; }
    }
    if (minIdx === -1) break; // finished

    const curItem = readers[minIdx].current;
    if (!pending) {
      pending = curItem;
    } else if (String(curItem.start) === String(pending.start)) {
      if (dedupeMode.toLowerCase() === 'last') pending = curItem; // replace
      // 'first' keeps existing
    } else {
      await flushPending();
      pending = curItem;
    }

    await load(minIdx);
  }

  await flushPending();

  // cleanup
  for (const r of readers) { try { fs.unlinkSync(r.p); } catch {} }
}

// ---------- Main ----------
(async () => {
  try {
    // Build input list
    let inputFiles = options.input || [];
    if (options.folder) {
      const dirFiles = fs.readdirSync(options.folder)
        .filter((f) => f.endsWith('.xml') || f.endsWith('.xml.gz'))
        .map((f) => path.join(options.folder, f));
      inputFiles = inputFiles.concat(dirFiles);
    }
    if (inputFiles.length === 0) throw new Error('No input files specified');

    // Progress for parsing — start immediately and grow total as we stat files lazily
    let totalSize = 1; // avoid div/0
    const progressBar = options.quiet
      ? null
      : new cliProgress.SingleBar(
          { format: 'Parsing [{bar}] {percentage}% | {mb} MB read | {value} programmes | ETA: {eta}s', clearOnComplete: true },
          cliProgress.Presets.shades_classic
        );
    if (!options.quiet) console.log('Starting merge…');
    if (progressBar) progressBar.start(totalSize, 0);

    for (const file of inputFiles) {
      try { const size = fs.statSync(file).size; if (progressBar) { totalSize += size; progressBar.setTotal(totalSize); } } catch (_) {}
      if (!options.quiet) console.log(`Parsing: ${file}`);
      await streamFile(file, progressBar);
    }

    // Close per-channel writers
    for (const { ws } of channelProgFiles.values()) { await new Promise((res) => ws.end(res)); }

    if (progressBar) progressBar.stop();

    // Prepare output (auto gzip by extension or --gzip)
    const gzipOut = options.gzip || options.output.endsWith('.gz');
    const rawOut = fs.createWriteStream(options.output);
    const output = gzipOut ? zlib.createGzip() : rawOut;
    if (gzipOut) output.pipe(rawOut);

    // Header
    output.write('<?xml version="1.0" encoding="UTF-8"?>\n');
    if (options.doctype) output.write('<!DOCTYPE tv SYSTEM "xmltv.dtd">\n');
    output.write('<tv>\n');

    // Channels first (sorted by id for stability)
    const channelIds = Array.from(channelXml.keys()).sort((a, b) => String(a).localeCompare(String(b)));
    if (!options.quiet) console.log(`Writing ${channelIds.length} channel(s)...`);

    let channelsBar = null;
    if (!options.quiet) {
      channelsBar = new cliProgress.SingleBar({ format: 'Writing channels [{bar}] {percentage}% | {value}/{total}' }, cliProgress.Presets.shades_classic);
      channelsBar.start(channelIds.length, 0);
    }
    for (const id of channelIds) {
      output.write(channelXml.get(id));
      if (channelsBar) channelsBar.increment();
    }
    if (channelsBar) channelsBar.stop();

    // Programmes per channel, sorted by start with external sort + dedupe
    if (!options.quiet) console.log(`Writing ${totalProgrammes} programme(s) sorted by channel/start...`);
    const dedupeMode = (options.dedupe || 'first').toLowerCase();

    let progsBar = null;
    let writtenProgrammes = 0;
    if (!options.quiet) {
      progsBar = new cliProgress.SingleBar({ format: 'Writing programmes [{bar}] {percentage}% | {value}/{total}' }, cliProgress.Presets.shades_classic);
      progsBar.start(Math.max(1, totalProgrammes), 0);
    }
    const onWrite = () => { writtenProgrammes++; if (progsBar) progsBar.update(writtenProgrammes); };

    for (const id of channelIds) {
      const meta = channelProgFiles.get(id);
      if (!meta) continue; // channel without programmes
      const filePath = meta.path;
      const chunks = await sortChannelFileToChunks(filePath);
      await kWayMergeChunks(chunks, output, dedupeMode, onWrite);
      try { fs.unlinkSync(filePath); } catch {}
    }
    if (progsBar) progsBar.stop();

    // Footer
    output.write('</tv>\n');
    await new Promise((res) => output.end(res));

    if (!options.quiet) {
      console.log(`Finished. Output: ${options.output}`);
      console.log(`Channels: ${channelIds.length} | Programmes (pre-dedupe): ${totalProgrammes}`);
    }
  } catch (err) {
    // Cleanup temps
    for (const { path: p } of channelProgFiles.values()) { try { fs.unlinkSync(p); } catch {} }
    console.error('Error:', err);
    process.exit(1);
  }
})();
