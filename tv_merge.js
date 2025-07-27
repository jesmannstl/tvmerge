const fs = require('fs');
const os = require('os');
const path = require('path');
const { createReadStream, createWriteStream } = require('fs');
const sax = require('sax');
const { Command } = require('commander');
const { DateTime } = require('luxon');
const cliProgress = require('cli-progress');
const zlib = require('zlib');

const program = new Command();
program
  .option('-i, --input <file...>', 'Input XMLTV file(s), space-separated or use --folder')
  .option('-f, --folder <dir>', 'Directory to merge all .xml files from')
  .requiredOption('-o, --output <file>', 'Output XMLTV file')
  .option('-t, --doctype', 'Add DOCTYPE to output file')
  .option('--fast', 'Enable fast mode (no global sorting/deduplication)')
  .option('--gzip', 'Compress output as .gz')
  .option('--preserve-order', 'Preserve original input file order')
  .option('-q, --quiet', 'Suppress output messages')
  .parse(process.argv);

const options = program.opts();
const tempFiles = new Map();
const channelMap = new Map();
let programmeCount = 0;

function toTimestamp(str) {
  return DateTime.fromFormat(str, 'yyyyLLddHHmmss ZZZ', { zone: 'utc' }).toSeconds();
}

function escapeXml(str) {
  return str?.replace(/[<>&'"\u0000-\u001F]/g, c => ({
    '<': '&lt;', '>': '&gt;', '&': '&amp;', '"': '&quot;', "'": '&apos;'
  })[c]) || '';
}

function generateProgrammeKey(p) {
  const parts = [`${p.channel}`, `${p.start}`, `${p.stop}`];
  for (const tag of ['title', 'sub-title', 'desc', 'category', 'episode-num']) {
    const values = p._children.filter(e => e.tag === tag).map(e => (e.text || '').trim());
    if (values.length) parts.push(...values);
  }
  return parts.join('|');
}

function isDuplicate(p, writtenKeys) {
  const key = generateProgrammeKey(p);
  if (writtenKeys.has(key)) return true;
  writtenKeys.add(key);
  return false;
}

function renderXml(el, indent = '    ') {
  const attrs = Object.entries(el.attrs || {}).map(([k, v]) => ` ${k}="${escapeXml(v)}"`).join('');
  const children = el.children || [];
  const text = (el.text || '').trim();
  const hasContent = text.length > 0 || children.length > 0;
  if (!hasContent) return `${indent}<${el.tag}${attrs} />\n`;
  const childrenXml = children.map(child => renderXml(child, indent + '  ')).join('');
  const content = escapeXml(el.text || '') + childrenXml;
  return `${indent}<${el.tag}${attrs}>${content}</${el.tag}>\n`;
}

function getOutputStream(filePath, gzip) {
  const raw = fs.createWriteStream(filePath);
  return gzip ? zlib.createGzip().pipe(raw) : raw;
}

function handleChannel(c, output) {
  if (!channelMap.has(c.id)) {
    channelMap.set(c.id, c);
    output.write(`  <channel id="${c.id}">\n`);
    for (const el of c._children || []) output.write(renderXml(el));
    output.write('  </channel>\n');
  }
}

function streamFile(file, output, progressBar, fastMode) {
  return new Promise((resolve, reject) => {
    const parser = sax.createStream(true, { trim: true });
    let stack = [];
    const writtenKeys = new Set();

    parser.on('opentag', node => {
      const obj = { tag: node.name, attrs: node.attributes, text: '', children: [] };
      if (stack.length > 0) stack[stack.length - 1].children.push(obj);
      stack.push(obj);
    });

    parser.on('text', text => {
      if (stack.length > 0) stack[stack.length - 1].text += text;
    });

    parser.on('closetag', tagName => {
      const obj = stack.pop();
      if (tagName === 'channel') {
        const channel = { id: obj.attrs.id, _children: obj.children };
        handleChannel(channel, output);
      } else if (tagName === 'programme') {
        const programme = {
          channel: obj.attrs.channel,
          start: obj.attrs.start,
          stop: obj.attrs.stop,
          _children: obj.children
        };
        programmeCount++;
        if (progressBar) progressBar.increment();

        if (fastMode) {
          output.write(`  <programme start="${programme.start}" stop="${programme.stop}" channel="${programme.channel}">\n`);
          for (const el of programme._children || []) output.write(renderXml(el));
          output.write('  </programme>\n');
        } else {
          const tmp = path.join(os.tmpdir(), `tvmerge-${programme.channel}.jsonl`);
          fs.appendFileSync(tmp, JSON.stringify(programme) + '\n');
          tempFiles.set(programme.channel, { path: tmp });
        }
      }
    });

    parser.on('end', resolve);
    parser.on('error', reject);
    createReadStream(file).pipe(parser);
  });
}

(async () => {
  try {
    let inputFiles = options.input || [];
    if (options.folder) {
      const dirFiles = fs.readdirSync(options.folder)
        .filter(f => f.endsWith('.xml'))
        .map(f => path.join(options.folder, f));
      inputFiles = inputFiles.concat(dirFiles);
    }
    if (inputFiles.length === 0) throw new Error('No input files specified');

    const totalSize = inputFiles.map(file => fs.statSync(file).size).reduce((a, b) => a + b, 0);
    const progressBar = options.quiet ? null : new cliProgress.SingleBar({
      format: 'Buffering [{bar}] {percentage}% | {value} items | ETA: {eta}s',
      clearOnComplete: true
    }, cliProgress.Presets.shades_classic);
    if (progressBar) progressBar.start(Math.ceil(totalSize / 250), 0);

    const output = getOutputStream(options.output, options.gzip);
    output.write('<?xml version="1.0" encoding="UTF-8"?>\n');
    if (options.doctype) output.write('<!DOCTYPE tv SYSTEM "xmltv.dtd">\n');
    output.write('<tv>\n');

    for (const file of inputFiles) await streamFile(file, output, progressBar, options.fast);
    if (progressBar) progressBar.stop();

    if (!options.fast) {
      const channelIds = Array.from(tempFiles.keys());
      const totalChannels = channelIds.length;
      let processedChannels = 0;
      const startTime = Date.now();

      for (const channelId of channelIds) {
        const { path: tempPath } = tempFiles.get(channelId);
        const writtenKeys = new Set();
        const lines = fs.readFileSync(tempPath, 'utf-8').split('\n').filter(Boolean);
        const programmes = lines.map(line => JSON.parse(line));
        programmes.sort((a, b) => toTimestamp(a.start) - toTimestamp(b.start));

        if (!options.quiet) {
          const elapsed = (Date.now() - startTime) / 1000;
          const avg = elapsed / Math.max(processedChannels, 1);
          const eta = Math.round((totalChannels - processedChannels) * avg);
          console.log(`  Writing ${programmes.length} for '${channelId}'... ETA: ${eta}s`);
        }

        for (const p of programmes) {
          if (isDuplicate(p, writtenKeys)) continue;
          output.write(`  <programme start="${p.start}" stop="${p.stop}" channel="${p.channel}">\n`);
          for (const el of p._children || []) output.write(renderXml(el));
          output.write('  </programme>\n');
        }

        processedChannels++;
        fs.unlinkSync(tempPath);
      }
    }

    output.write('</tv>\n');
    output.end();
    if (!options.quiet) console.log(`Writing: ${channelMap.size} channel(s), ${programmeCount} programme(s)`);
  } catch (err) {
    console.error('Error:', err);
    process.exit(1);
  }
})();
