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
| ----------------------- | ------------------------------------------------------------------------ |
| `-i, --input <file...>` | Input XMLTV file(s), space-separated or use `--folder` .xml or .xml.gz   |
| `-f, --folder <dir>`    | Directory to merge all .xml files from                                   |
| `-o, --output <file>`   | Output XMLTV file (Required)                                             |
| `-t, --doctype`         | Add DOCTYPE to output file                                               |
| `--dedupe <mode>`       | Removes duplicates on same channel/start `first` (default), `last` entry |
| `--gzip`                | Compress output as .gz (automatic if you specify .gz output name)        |
| `-q, --quiet`           | Suppress output messages                                                 |
| `--tmp-dir <dir>`       | Set temporary directory to process files                                 |
| `--sortname`            | Sort output by `<display name>` instead of channel id                    |

## Setup and running in intervals

### Running natively

You can run tvmerge natively on your system. 


`node tv_merge.js [your_options_here]`

### Examples

`node tv_merge.js -i file1.xml file2.xml -o merge.xml`

`node tv_merge.js -i file1.xml file2.xml file3.xml -o merge.xml`

`node tv_merge.js -f folder -o merge.xml`

`node tv_merge.js --tmp-dir F:\tvmerge_temp -i file1.xml file2.xml -o merge.xml.gz`




