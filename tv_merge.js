// tv_merge.js — merge XMLTV feeds
// Features:
//  - --swapname: swap first/second <display-name>
//  - --chno[=suffix]: id = DisplayName1(DisplayName2)[_<lcn>] + optional .suffix (built after swap)
//  - Collision: append _<lcn> before suffix, only when --chno is active
//  - Preserve <lcn> contents (never write into it)
//  - Programmes carry remapped channel ids; one writer per channel to avoid EMFILE
//  - --sortname: sort channels by (post-swap) first display-name
//  - --dedupe first|last (default first)

'use strict';

const fs = require('fs');
const os = require('os');
const path = require('path');
const zlib = require('zlib');
const { program } = require('commander');
const sax = require('sax');
const cliProgress = require('cli-progress');

// ---------- CLI ----------
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
  .option('--sortname', 'Sort channels alphabetically by display-name (call sign/name)')
  .option('--swapname', 'Swap the first and second <display-name> elements in each channel')
  .option('--chno [suffix]', 'Set <channel id> to DisplayName1(DisplayName2)[_<lcn>], removing spaces inside names; if suffix is provided, append .suffix')
  .option('-q, --quiet', 'Suppress console output')
  .parse(process.argv);

const options = program.opts();
const useSortName = Boolean(options.sortname) || (/^true$/i.test(process.env.SORTNAME || ''));
const useSwapName = Boolean(options.swapname);
const useChno = typeof options.chno !== 'undefined';
const chnoSuffix = options.chno === true ? '' : (options.chno || '');

// ---------- Temp base ----------
const TMP_BASE = (() => {
  const cand = options.tmpDir || process.env.TV_MERGE_TMP || process.env.TEMP || os.tmpdir();
  try { if (!fs.existsSync(cand)) fs.mkdirSync(cand, { recursive: true }); } catch (_) {}
  return cand;
})();

// ---------- State ----------
const channelXml = new Map();       // channelId -> raw <channel> xml
const channelSortName = new Map();  // channelId -> preferred display-name for sorting
const channelProgFiles = new Map(); // channelId -> { path, ws } (ONE writer per channel)
const channelIdRemap = new Map();   // oldId -> newId
let totalProgrammes = 0;            // pre-dedupe info
let totalChannels = 0;
let bytesReadTotal = 0;             // for progress

// ---------- Helpers ----------
function escapeXml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}
function localName(qn) {
  const i = qn.indexOf(':');
  return i === -1 ? qn : qn.slice(i + 1);
}
function openTag(name, attrs) {
  const attrStr = Object.entries(attrs || {})
    .filter(([, v]) => v !== undefined && v !== null && v !== '')
    .map(([k, v]) => ` ${k}="${escapeXml(String(v))}"`)
    .join('');
  return `<${name}${attrStr}>`;
}
function pushOpen(stack, pieces, name, attrs) {
  const idx = pieces.length;
  pieces.push(`  ${openTag(name, attrs)}`);
  stack.push({ name, idx, hasContent: false });
}
function pushClose(pieces, name) { pieces.push(`</${name}>`); }
function markParentHasContent(stack) { if (stack.length) stack[stack.length - 1].hasContent = true; }
function san(s) { return String(s).replace(/[^\w.\-]+/g, '_'); }
function looksGzip(file) {
  try {
    const fd = fs.openSync(file, 'r');
    const buf = Buffer.alloc(2);
    const r = fs.readSync(fd, buf, 0, 2, 0);
    fs.closeSync(fd);
    return r === 2 && buf[0] === 0x1f && buf[1] === 0x8b;
  } catch { return false; }
}

function mapChannelId(id) {
  let cur = id;
  let n = 0;
  while (channelIdRemap.has(cur) && n < 1000) { cur = channelIdRemap.get(cur); n++; }
  return cur;
}

// ONE write stream per (mapped) channel ID
function getProgWriter(channelId) {
  const cid = mapChannelId(channelId);
  if (!channelProgFiles.has(cid)) {
    const tmpPath = path.join(TMP_BASE, `tvmerge-${process.pid}-${Date.now()}-${san(cid)}.jsonl`);
    const ws = fs.createWriteStream(tmpPath, { encoding: 'utf8' });
    channelProgFiles.set(cid, { path: tmpPath, ws });
  }
  return channelProgFiles.get(cid).ws;
}

async function flushProgWriters() {
  const closers = [];
  for (const meta of channelProgFiles.values()) {
    if (meta.ws) {
      closers.push(new Promise((res) => meta.ws.end(res)));
      meta.ws = null;
    }
  }
  await Promise.all(closers);
}

async function *readLines(filePath) {
  const rs = fs.createReadStream(filePath, { encoding: 'utf8' });
  let buf = '';
  for await (const chunk of rs) {
    buf += chunk;
    let idx;
    while ((idx = buf.indexOf('\n')) >= 0) {
      const line = buf.slice(0, idx);
      buf = buf.slice(idx + 1);
      yield line;
    }
  }
  if (buf.length) yield buf;
}

async function sortChannelFileToChunks(filePath, chunkSize = 200000) {
  const rows = [];
  for await (const line of readLines(filePath)) {
    try {
      const o = JSON.parse(line);
      if (o && o.start) rows.push(o);
    } catch {}
  }
  rows.sort((a, b) => String(a.start).localeCompare(String(b.start), undefined, { numeric: true }));

  const chunks = [];
  let start = 0;
  while (start < rows.length) {
    const slice = rows.slice(start, Math.min(rows.length, start + chunkSize));
    const chunkPath = path.join(TMP_BASE, `sort-${process.pid}-${Date.now()}-${Math.random().toString(16).slice(2)}.jsonl`);
    const ws = fs.createWriteStream(chunkPath, { encoding: 'utf8' });
    for (const r of slice) ws.write(JSON.stringify(r) + '\n');
    await new Promise((res) => ws.end(res));
    chunks.push(chunkPath);
    start += chunkSize;
  }
  return chunks;
}

async function kWayMergeChunks(chunks, output, dedupeMode = 'first', onWrite = () => {}) {
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

  let lastKey = null;
  let lastItem = null;

  while (true) {
    let minIdx = -1;
    let minVal = null;
    for (let i = 0; i < readers.length; i++) {
      const cur = readers[i].current;
      if (!cur) continue;
      if (!minVal || String(cur.start) < String(minVal.start)) { minVal = cur; minIdx = i; }
    }
    if (minIdx === -1) break;

    const item = readers[minIdx].current;
    const key = `${item.start}::${item.channel}`;

    if (lastKey === null) {
      lastKey = key; lastItem = item;
    } else if (key === lastKey) {
      if (dedupeMode === 'last') lastItem = item;
    } else {
      output.write(lastItem.xml);
      onWrite();
      lastKey = key; lastItem = item;
    }

    await load(minIdx);
  }

  if (lastItem) { output.write(lastItem.xml); onWrite(); }

  for (const r of readers) {
    if (r && r.p) { try { fs.unlinkSync(r.p); } catch {} }
  }
}

// ---------- Collect inputs ----------
function gatherInputFiles() {
  const files = new Set();
  if (options.input && options.input.length) for (const f of options.input) files.add(f);
  if (options.folder) {
    const list = fs.readdirSync(options.folder, { withFileTypes: true });
    for (const ent of list) {
      if (ent.isFile()) {
        const nm = ent.name.toLowerCase();
        if (nm.endsWith('.xml') || nm.endsWith('.xml.gz') || nm.endsWith('.gz')) {
          files.add(path.join(options.folder, ent.name));
        }
      }
    }
  }
  if (!files.size) {
    console.error('No input files. Use -i or --folder.');
    process.exit(1);
  }
  return Array.from(files);
}

// ---------- Main ----------
(async () => {
  try {
    if (!options.quiet) {
      console.log('Starting merge…');
      console.log('Options:', {
        sortname: !!options.sortname,
        swapname: !!options.swapname,
        chno: typeof options.chno !== 'undefined' ? (options.chno === true ? '' : options.chno) : undefined,
        tmpDir: options.tmpDir,
        inputs: options.input,
        output: options.output
      });
    }

    const inputs = gatherInputFiles();

    const rawOut = fs.createWriteStream(options.output);
    const gzipOut = options.gzip || String(options.output).toLowerCase().endsWith('.gz');
    const output = gzipOut ? zlib.createGzip() : rawOut;
    if (gzipOut) output.pipe(rawOut);

    output.write('<?xml version="1.0" encoding="UTF-8"?>\n');
    if (options.doctype) output.write('<!DOCTYPE tv SYSTEM "xmltv.dtd">\n');
    output.write('<tv>\n');

    let progressBar = null;
    if (!options.quiet) {
      progressBar = new cliProgress.SingleBar({ format: 'Parsing [{bar}] {percentage}% | {mb} MB read | {value} programmes | ETA: {eta}s' }, cliProgress.Presets.shades_classic);
      progressBar.start(0, 0, { mb: 0 });
    }

    for (const file of inputs) {
      if (!options.quiet) console.log(`Parsing: ${file}`);
      const gz = looksGzip(file);
      const rs = fs.createReadStream(file);
      const src = gz ? rs.pipe(zlib.createGunzip()) : rs;

      const parser = sax.createStream(true, { xmlns: false, trim: false, normalize: false });
      let capType = null; // 'channel' | 'programme' | null
      let rootOpen = '';
      let pieces = [];
      let stack = [];
      let currentChannelId = '';
      let displayNameBuffer = '';
      let currentDisplayNames = [];
      let currentLcn = '';
      let currentProgChannel = '';
      let currentProgStart = '';
      let currentProgStop = '';

      let seenAny = false;
      let debugLeft = options.debugScan ? 20 : 0;

      function resetCapture() {
        capType = null; rootOpen = ''; pieces = []; stack = [];
        currentChannelId = '';
        currentProgChannel = ''; currentProgStart = ''; currentProgStop = '';
        displayNameBuffer = ''; currentDisplayNames = []; currentLcn = '';
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
          currentProgStop  = node.attributes.stop  || '';
          rootOpen = `  ${openTag('programme', { start: currentProgStart, stop: currentProgStop, channel: currentProgChannel })}`;
          pieces = []; stack = []; return;
        }
        if (capType) {
          const lnameCur = localName(node.name);
          if (capType === 'channel' && lnameCur === 'display-name') displayNameBuffer = '';
          if (capType === 'channel' && lnameCur === 'lcn') currentLcn = '';
          const idx = pieces.length;
          pieces.push(`  ${openTag(node.name, node.attributes)}`);
          stack.push({ name: node.name, idx, hasContent: false });
          markParentHasContent(stack);
        }
      });

      parser.on('text', (text) => {
        if (!capType) return;
        if (text && text.length) {
          if (stack.length) stack[stack.length - 1].hasContent = true;
          const top = stack.length ? stack[stack.length - 1] : null;
          if (capType === 'channel' && top && localName(top.name) === 'display-name') displayNameBuffer += text;
          if (capType === 'channel' && top && localName(top.name) === 'lcn') currentLcn += text;
          pieces.push(escapeXml(text));
        }
      });

      parser.on('closetag', () => {
        if (!capType) return;
        if (stack.length) {
          const el = stack.pop();
          if (capType === 'channel' && localName(el.name) === 'display-name') {
            const cand = (displayNameBuffer || '').trim();
            if (cand) {
              currentDisplayNames.push(cand);
              const prev = channelSortName.get(currentChannelId);
              const candHasLetters = /[A-Za-z]/.test(cand);
              const candStartsDigit = /^\d/.test(cand);
              if (!prev) channelSortName.set(currentChannelId, cand);
              else {
                const prevHasLetters = /[A-Za-z]/.test(prev);
                const prevStartsDigit = /^\d/.test(prev);
                if ((!prevHasLetters && candHasLetters) || (prevStartsDigit && !candStartsDigit)) {
                  channelSortName.set(currentChannelId, cand);
                }
              }
            }
          }
          if (!el.hasContent) pieces[el.idx] = pieces[el.idx].replace(/>$/, ' />'); else pushClose(pieces, el.name);
          if (capType === 'channel' && localName(el.name) === 'lcn') currentLcn = (currentLcn || '').trim();
          return;
        }

        // -------- channel close: swap (optional), compute id, store xml --------
        if (capType === 'channel') {
          let body = pieces.join('');

          // Optional swap of the first two <display-name> blocks
          if (useSwapName && currentDisplayNames.length >= 2) {
            const dnRe = /(<display-name(?:\s[^>]*)?>[\s\S]*?<\/display-name>)/g;
            const matches = Array.from(body.matchAll(dnRe));
            if (matches.length >= 2) {
              const firstBlock = matches[0][1];
              const secondBlock = matches[1][1];
              let idxSwap = 0;
              body = body.replace(dnRe, (m) => {
                if (idxSwap === 0) { idxSwap++; return secondBlock; }
                if (idxSwap === 1) { idxSwap++; return firstBlock; }
                idxSwap++; return m;
              });
            }
          }

          // Names AFTER swap (as captured text)
          const name1 = (useSwapName && currentDisplayNames.length >= 2)
            ? currentDisplayNames[1]
            : currentDisplayNames[0];
          const name2 = (useSwapName && currentDisplayNames.length >= 2)
            ? currentDisplayNames[0]
            : currentDisplayNames[1];

          // Build combined base DisplayName1(DisplayName2), removing spaces inside names
          const pack = (s) => (s ? String(s).replace(/\s+/g, '') : '');
          const combinedBase = name1
            ? (pack(name1) + (name2 ? `(${pack(name2)})` : ''))
            : '';

          // Default: keep original id unless --chno is active
          let newChannelId = currentChannelId;

          if (useChno && combinedBase) {
            const suffixPart = chnoSuffix ? `.${chnoSuffix}` : '';
            let tentative = `${combinedBase}${suffixPart}`;

            // Collision handling ONLY when renaming is active
            if (channelXml.has(tentative)) {
              const safeLcn = (currentLcn || '').trim().replace(/\s+/g, '');
              if (safeLcn) {
                tentative = `${combinedBase}_${safeLcn}${suffixPart}`;
              } else {
                let i = 2;
                while (channelXml.has(`${combinedBase}_${i}${suffixPart}`)) i++;
                tentative = `${combinedBase}_${i}${suffixPart}`;
              }
            }
            newChannelId = tentative;
          }

          // Programmes follow the new id if changed
          if (newChannelId !== currentChannelId) channelIdRemap.set(currentChannelId, newChannelId);

          // Avoid a blank line before </channel>
          body = body.replace(/\s+$/, '');

          const newRootOpen = `  <channel id="${escapeXml(newChannelId)}">`;
          const xml = `${newRootOpen}${body}\n  </channel>\n`;

          if (!channelXml.has(newChannelId)) { channelXml.set(newChannelId, xml); totalChannels++; }
          else { channelXml.set(newChannelId, xml); }

          if (name1) channelSortName.set(newChannelId, name1);
          else channelSortName.set(newChannelId, channelSortName.get(currentChannelId) || newChannelId);

          if (newChannelId !== currentChannelId) {
            channelXml.delete(currentChannelId);
            channelSortName.delete(currentChannelId);
          }
        } else if (capType === 'programme') {
          const remappedChannel = mapChannelId(currentProgChannel);

          // Rebuild <programme ...> open tag so the channel attr is remapped
          const open = `  ${openTag('programme', {
            start:  currentProgStart,
            stop:   currentProgStop,
            channel: remappedChannel
          })}`;

          const xml = `${open}${pieces.join('')}\n  </programme>\n`;

          // write under the remapped channel id (correct grouping)
          const ws = getProgWriter(remappedChannel);
          ws.write(JSON.stringify({ start: currentProgStart, channel: remappedChannel, xml }) + '\n');
          totalProgrammes++;
          if (progressBar) progressBar.update(bytesReadTotal, { value: totalProgrammes, mb: (bytesReadTotal/1048576).toFixed(1) });
        }
        resetCapture();
      });

      parser.on('error', (err) => {
        const msg = `[sax] ${file}: ${err && err.message ? err.message : String(err)}`;
        if (!options.quiet) console.warn(msg);
        if (!(options.lenient || options.skipBad)) throw err;
      });

      src.on('data', (chunk) => {
        bytesReadTotal += chunk.length;
        if (progressBar) progressBar.update(totalProgrammes, { mb: (bytesReadTotal/1048576).toFixed(1) });
      });

      src.pipe(parser);

      await new Promise((resolve, reject) => {
        parser.on('end', resolve);
        parser.on('error', reject);
        src.on('error', reject);
      });

      if (!seenAny && !options.quiet) console.warn(`[warn] No <channel> or <programme> elements found in ${file}`);
    }

    // CLOSE all programme writers so files are flushed
    await flushProgWriters();

    // Channels first (sorted)
    let channelIds = Array.from(channelXml.keys());
    if (useSortName) {
      channelIds.sort((a, b) =>
        String(channelSortName.get(a) || a).localeCompare(String(channelSortName.get(b) || b), undefined, { sensitivity: 'base', numeric: true })
      );
    } else {
      channelIds.sort((a, b) => String(a).localeCompare(String(b), undefined, { numeric: true }));
    }
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

    // Build "final ID -> [paths]" map; old IDs that remap to the same new ID are merged
    const merged = new Map(); // newId -> string[]
    for (const [oldId, meta] of channelProgFiles.entries()) {
      const newId = mapChannelId(oldId);
      const arr = merged.get(newId) || [];
      merged.set(newId, arr);
      if (meta.path) arr.push(meta.path);
    }

    let progsBar = null;
    let writtenProgrammes = 0;
    if (!options.quiet) {
      progsBar = new cliProgress.SingleBar({ format: 'Writing programmes [{bar}] {percentage}% | {value}/{total}' }, cliProgress.Presets.shades_classic);
      progsBar.start(Math.max(1, totalProgrammes), 0);
    }
    const onWrite = () => { writtenProgrammes++; if (progsBar) progsBar.update(writtenProgrammes); };

    for (const id of channelIds) {
      const paths = merged.get(id);
      if (!paths || !paths.length) continue;
      let allChunks = [];
      for (const pth of paths) {
        const chunks = await sortChannelFileToChunks(pth);
        allChunks.push(...chunks);
      }
      await kWayMergeChunks(allChunks, output, dedupeMode, onWrite);
      for (const pth of paths) { try { fs.unlinkSync(pth); } catch {} }
    }
    if (progsBar) progsBar.stop();

    // Footer
    output.write('</tv>\n');

    await new Promise((res, rej) => {
      output.end((err) => (err ? rej(err) : res()));
    });
    // If gzipping, also wait for the destination file stream to finish flushing
    if (gzipOut) {
      await new Promise((res, rej) => {
        try { rawOut.once('finish', res).once('error', rej); } catch { res(); }
      });
      try { rawOut.close(); } catch {}
    }

    if (!options.quiet) {
      console.log(`Finished. Output: ${options.output}`);
      console.log(`Channels: ${channelIds.length} | Programmes (pre-dedupe): ${totalProgrammes}`);
    }

    // Force exit to avoid any lingering handles in some environments
    process.exit(0);
  } catch (err) {
    // Cleanup temps
    for (const meta of channelProgFiles.values()) {
      if (meta.ws) { try { meta.ws.destroy(); } catch {} }
      if (meta.path) { try { fs.unlinkSync(meta.path); } catch {} }
    }
    console.error('Error:', err);
    process.exit(1);
  }
})();
