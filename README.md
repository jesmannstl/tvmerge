# tvmerge

An updated tvmerge program written in node.js to merge two or more xmltv files to a new file

## How to use

### Node.js

```bash
npm i && node tv_merge.js
```

See [Command line arguments](#command-line-arguments) for configuration options.

## Configuration

### Command line arguments

| Argument                | Description                                                            |
| ----------------------- | ---------------------------------------------------------------------- |
| `-i, --input <file...>` | Input XMLTV file(s), space-separated or use `--folder` .xml or .xml.gz |
| `-f, --folder <dir>`    | Directory to merge all .xml files from                                 |
| `-o, --output <file>`   | Output XMLTV file (Required)                                           |
| `-t, --doctype`         | Add DOCTYPE to output file                                             |
| `--fast`                | Enable fast mode (no global sorting/deduplication)                     |
| `--gzip`                | Compress output as .gz (automatic if you specify .gz output name)      |
| `--preserve-order`      | Preserve original input file order                                     |
| `-q, --quiet`           | Suppress output messages                                               |

## Setup and running in intervals

### Running natively

You can run tvmerge natively on your system. 


`node tv_merge.js [your_options_here]`