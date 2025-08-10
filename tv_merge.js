const fs = require('fs');
const os = require('os');
const path = require('path');
const { createReadStream } = require('fs');
const sax = require('sax');
const { Command } = require('commander');
const cliProgress = require('cli-progress');
const zlib = require('zlib');
const readline = require('readline');

// ------------------------------------------------------------
// tv_merge.js — channels-first; programmes sorted by channel+start (with dedupe)
//  - Supports .xml and .xml.gz input
//  - Auto .gz output if --gzip or output ends with .gz
//  - Streaming capture with self-closing empty tags
//  - Per-channel JSONL buffering + external sort (chunked) to keep memory low
//  - Dedupe by (channel,start) during k-way merge: --dedupe first|last
// ------------------------------------------------------------

const program = new Command();
program
  .option('-i, --input <file...>', 'Input XMLTV file(s), space-separated or use --folder')
  .option('-f, --folder <dir>', 'Directory to merge all .xml/.xml.gz files from')
  .requiredOption('-o, --output <file>', 'Output XMLTV file (.xml or .xml.gz)')
  .option('-t, --doctype', 'Add DOCTYPE to output file')
  .option('--gzip', 'Force gzip output regardless of extension')
  .option('--dedupe <mode>', 'Deduplicate programmes per channel by start: first|last', 'first')
  .option('-q, --quiet', 'Suppress console output')
  .parse(process.argv);

const options = program.opts();

// --- State ---
const channelXml = new Map();            // channelId -> raw <channel> xml
const channelProgFiles = new Map();      // channelId -> { path, ws }
let totalProgrammes = 0;                 // before dedupe (for info)
let totalChannels = 0;

// --- Helpers ---
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

function san(id) {
  return String(id ?? '').replace(/[^A-Za-z0-9_.-]/g, '_');
}

function getProgWriter(channelId) {
  if (!channelProgFiles.has(channelId)) {
    const tmpPath = path.join(os.tmpdir(), `tvmerge-${process.pid}-${Date.now()}-${san(channelId)}.jsonl`);
    const ws = fs.createWriteStream(tmpPath, { encoding: 'utf8' });
    channelProgFiles.set(channelId, { path: tmpPath, ws });
  }
  return channelProgFiles.get(channelId).ws;
}

// --- Core: stream a file and capture raw xml for channel/programme with proper nesting
// Uses a small stack so empty elements are emitted as self-closing <tag ... />
function streamFile(file, progressBar) {
  return new Promise((resolve, reject) => {
    const parser = sax.createStream(true, { trim: true });

    let capType = null;            // 'channel' | 'programme' | null
    let rootOpen = '';
    let pieces = [];               // array of strings (parts of XML)
    let stack = [];                // [{ name, idx, hasContent }]
    let currentChannelId = '';
    let currentProgChannel = '';
    let currentProgStart = '';

    function openTag(name, attrs) {
      const attrStr = Object.entries(attrs || {})
        .map(([k, v]) => ` ${k}="${escapeXml(String(v))}"`)
        .join('');
      return `<${name}${attrStr}>`;
    }

    function pushOpen(name, attrs) {
      // newline + indentation based on current depth inside the captured root
      // Root starts with two spaces ("  "), first child uses 4 spaces; deeper adds 2 spaces per level
      const indent = '\n' + '    ' + '  '.repeat(stack.length);
      const s = indent + openTag(name, attrs);
      pieces.push(s);
      stack.push({ name, idx: pieces.length - 1, hasContent: false });
    }

    function pushClose(name) {
      pieces.push(`</${name}>`);
    }

    function markParentHasContent() {
      if (stack.length > 1) stack[stack.length - 2].hasContent = true;
    }

    parser.on('opentag', (node) => {
      if (capType === null && node.name === 'channel') {
        capType = 'channel';
        currentChannelId = node.attributes.id || '';
        rootOpen = `  ${openTag('channel', { id: currentChannelId })}`;
        pieces = [];
        stack = [];
        return;
      }
      if (capType === null && node.name === 'programme') {
        capType = 'programme';
        currentProgChannel = node.attributes.channel || '';
        currentProgStart = node.attributes.start || '';
        rootOpen = `  ${openTag('programme', {
          start: currentProgStart,
          stop: node.attributes.stop || '',
          channel: currentProgChannel,
        })}`;
        pieces = [];
        stack = [];
        return;
      }
      if (capType) {
        // opening a child under channel/programme
        pushOpen(node.name, node.attributes);
        markParentHasContent();
      }
    });

    parser.on('text', (text) => {
      if (!capType) return;
      if (text && text.length) {
        // any text counts as content for the current element
        if (stack.length) stack[stack.length - 1].hasContent = true;
        pieces.push(escapeXml(text));
      }
    });

    parser.on('closetag', (name) => {
      if (!capType) return;

      if (stack.length) {
        const el = stack.pop();
        if (!el.hasContent) {
          // convert the opening tag at pieces[el.idx] into a self-closing one
          pieces[el.idx] = pieces[el.idx].replace(/>$/, ' />');
        } else {
          pushClose(el.name);
        }
        return; // still inside root capture
      }

      // closing the root captured element
      if (capType === 'channel') {
        const xml = `${rootOpen}${pieces.join('')}\n  </channel>\n`;
        if (!channelXml.has(currentChannelId)) {
          channelXml.set(currentChannelId, xml);
          totalChannels++;
        }
      } else if (capType === 'programme') {
        const xml = `${rootOpen}${pieces.join('')}\n  </programme>\n`;
        const ws = getProgWriter(currentProgChannel);
        ws.write(JSON.stringify({ start: currentProgStart, xml }) + '\n');
        totalProgrammes++;
        if (progressBar) progressBar.increment();
      }

      // reset
      capType = null;
      rootOpen = '';
      pieces = [];
      stack = [];
      currentChannelId = '';
      currentProgChannel = '';
      currentProgStart = '';
    });

    parser.on('end', resolve);
    parser.on('error', reject);

    const rs = file.endsWith('.gz')
      ? createReadStream(file).pipe(zlib.createGunzip())
      : createReadStream(file);

    rs.pipe(parser);
  });
}

// --- External sort per channel (chunked) and write to output ---
const CHUNK_LINES = 50000; // adjust if needed

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

async function kWayMergeChunks(chunks, output, dedupeMode = 'first') {
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
    if (pending) {
      output.write(pending.xml);
      pending = null;
    }
  };

  while (true) {
    let minIdx = -1;
    let minVal = null;
    for (let i = 0; i < readers.length; i++) {
      const cur = readers[i].current;
      if (!cur) continue;
      if (!minVal || String(cur.start).localeCompare(String(minVal.start)) < 0) {
        minVal = cur; minIdx = i;
      }
    }
    if (minIdx === -1) break; // finished

    const curItem = readers[minIdx].current;
    if (!pending) {
      pending = curItem;
    } else if (String(curItem.start) === String(pending.start)) {
      // same start; decide which to keep
      if (dedupeMode.toLowerCase() === 'last') pending = curItem; // replace
      // 'first' keeps the existing pending
    } else {
      await flushPending();
      pending = curItem;
    }

    await load(minIdx);
  }

  await flushPending();

  // cleanup
  for (const r of readers) try { fs.unlinkSync(r.p); } catch {}
}

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

    // Progress for parsing
    const totalSize = inputFiles.map((f) => fs.statSync(f).size).reduce((a, b) => a + b, 0);
    const progressBar = options.quiet
      ? null
      : new cliProgress.SingleBar(
          {
            format: 'Parsing [{bar}] {percentage}% | {value} programmes | ETA: {eta}s',
            clearOnComplete: true,
          },
          cliProgress.Presets.shades_classic
        );
    if (progressBar) progressBar.start(Math.ceil(totalSize / 250), 0);

    // Parse all inputs — channels -> channelXml, programmes -> per-channel JSONL
    for (const file of inputFiles) {
      await streamFile(file, progressBar);
    }

    // Close per-channel writers
    for (const { ws } of channelProgFiles.values()) {
      await new Promise((res) => ws.end(res));
    }

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
    for (const id of channelIds) output.write(channelXml.get(id));

    // Programmes per channel, sorted by start with external sort + dedupe
    if (!options.quiet) console.log(`Writing ${totalProgrammes} programme(s) sorted by channel/start...`);
    const dedupeMode = (options.dedupe || 'first').toLowerCase();

    for (const id of channelIds) {
      const meta = channelProgFiles.get(id);
      if (!meta) continue; // channel without programmes
      const filePath = meta.path;
      const chunks = await sortChannelFileToChunks(filePath);
      await kWayMergeChunks(chunks, output, dedupeMode);
      try { fs.unlinkSync(filePath); } catch {}
    }

    // Footer
    output.write('</tv>\n');
    await new Promise((res) => output.end(res));

    if (!options.quiet) {
      console.log(`Finished. Output: ${options.output}`);
      console.log(`Channels: ${channelIds.length} | Programmes (pre-dedupe): ${totalProgrammes}`);
    }
  } catch (err) {
    // Cleanup temps
    for (const { path: p } of channelProgFiles.values()) {
      try { fs.unlinkSync(p); } catch {}
    }
    console.error('Error:', err);
    process.exit(1);
  }
})();
