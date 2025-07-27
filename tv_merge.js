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
  .option('--fast', 'Enable fast mode (streaming output only)')
  .option('--gzip', 'Compress output as .gz')
  .option('--preserve-order', 'Preserve original input file order')
  .option('-q, --quiet', 'Suppress output messages')
  .parse(process.argv);

const options = program.opts();
const channelMap = new Map();
let totalProgrammes = 0;
let totalChannels = 0;

function escapeXml(str) {
  return str?.replace(/[<>&'"\u0000-\u001F]/g, c => ({
    '<': '&lt;', '>': '&gt;', '&': '&amp;', '"': '&quot;', "'": '&apos;'
  })[c]) || '';
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

function streamFile(file, output, progressBar, fastMode) {
  return new Promise((resolve, reject) => {
    const parser = sax.createStream(true, { trim: true });
    let stack = [];
    let raw = '';
    let currentChannelId = '';
    let insideProgramme = false;
    let insideChannel = false;

    parser.on('opentag', node => {
      if (fastMode) {
        if (node.name === 'programme') {
          insideProgramme = true;
          raw = `<programme start=\"${node.attributes.start}\" stop=\"${node.attributes.stop}\" channel=\"${node.attributes.channel}\">`;
        } else if (node.name === 'channel') {
          insideChannel = true;
          currentChannelId = node.attributes.id;
          raw = `  <channel id=\"${currentChannelId}\">`;
        } else if (insideProgramme || insideChannel) {
          const attrs = Object.entries(node.attributes || {}).map(([k, v]) => ` ${k}=\"${escapeXml(v)}\"`).join('');
          raw += `\n  <${node.name}${attrs}>`;
        }
      } else {
        const obj = { tag: node.name, attrs: node.attributes, text: '', children: [] };
        if (stack.length > 0) stack[stack.length - 1].children.push(obj);
        stack.push(obj);
      }
    });

    parser.on('text', text => {
      if (fastMode && (insideProgramme || insideChannel)) {
        raw += escapeXml(text);
      } else if (!fastMode && stack.length > 0) {
        stack[stack.length - 1].text += text;
      }
    });

    parser.on('closetag', tagName => {
      if (fastMode) {
        if (tagName === 'programme') {
          raw += `\n</programme>\n`;
          output.write(raw);
          insideProgramme = false;
          totalProgrammes++;
          if (progressBar) progressBar.increment();
        } else if (tagName === 'channel') {
          raw += `\n  </channel>\n`;
          if (!channelMap.has(currentChannelId)) {
            channelMap.set(currentChannelId, true);
            output.write(raw);
            totalChannels++;
          }
          insideChannel = false;
        } else if (insideProgramme || insideChannel) {
          raw += `</${tagName}>`;
        }
      } else {
        const obj = stack.pop();
        if (tagName === 'channel') {
          const channel = { id: obj.attrs.id, _children: obj.children };
          if (!channelMap.has(channel.id)) {
            channelMap.set(channel.id, channel);
            output.write(`  <channel id=\"${channel.id}\">\n`);
            for (const el of channel._children || []) output.write(renderXml(el));
            output.write('  </channel>\n');
            totalChannels++;
          }
        } else if (tagName === 'programme') {
          output.write(`  <programme start=\"${obj.attrs.start}\" stop=\"${obj.attrs.stop}\" channel=\"${obj.attrs.channel}\">\n`);
          for (const el of obj.children || []) output.write(renderXml(el));
          output.write('  </programme>\n');
          totalProgrammes++;
          if (progressBar) progressBar.increment();
        }
      }
    });

    parser.on('end', resolve);
    parser.on('error', reject);

    const fileStream = file.endsWith('.gz')
      ? createReadStream(file).pipe(zlib.createGunzip())
      : createReadStream(file);

    fileStream.pipe(parser);
  });
}

(async () => {
  try {
    let inputFiles = options.input || [];
    if (options.folder) {
      const dirFiles = fs.readdirSync(options.folder)
        .filter(f => f.endsWith('.xml') || f.endsWith('.xml.gz'))
        .map(f => path.join(options.folder, f));
      inputFiles = inputFiles.concat(dirFiles);
    }
    if (inputFiles.length === 0) throw new Error('No input files specified');

    const totalSize = inputFiles.map(file => fs.statSync(file).size).reduce((a, b) => a + b, 0);
    const progressBar = options.quiet ? null : new cliProgress.SingleBar({
      format: 'Writing [{bar}] {percentage}% | {value} items | ETA: {eta}s',
      clearOnComplete: true
    }, cliProgress.Presets.shades_classic);
    if (progressBar) progressBar.start(Math.ceil(totalSize / 250), 0);

    const isGzipped = options.gzip || options.output.endsWith('.gz');
    const outputStream = isGzipped
      ? zlib.createGzip().pipe(fs.createWriteStream(options.output))
      : fs.createWriteStream(options.output);

    outputStream.write('<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n');
    if (options.doctype) outputStream.write('<!DOCTYPE tv SYSTEM \"xmltv.dtd\">\n');
    outputStream.write('<tv>\n');

    for (const file of inputFiles) {
      await streamFile(file, outputStream, progressBar, options.fast);
    }

    outputStream.write('</tv>\n');
    outputStream.end();
    if (progressBar) progressBar.stop();

    if (!options.quiet) {
      console.log(`Finished merging ${inputFiles.length} file(s). Output saved to ${options.output}`);
      console.log(`Channels: ${totalChannels} | Programmes: ${totalProgrammes}`);
    }
  } catch (err) {
    console.error('Error:', err);
    process.exit(1);
  }
})();
