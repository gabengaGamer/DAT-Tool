## Entropy-DAT-Tool

DAT Tool is a command-line interface (CLI) utility for working with [Tribes: Aerial Assault](https://en.wikipedia.org/wiki/Tribes:_Aerial_Assault) DAT archive format. The DAT format is used to store game assets.

This Python implementation is based on the original C++ implementation by Inevitable Entertainment Inc.

## Features

- List the contents of a DAT archive
- Extract files from a DAT archive
- Create a new DAT archive from a directory of files
- Verify the integrity of a DAT archive

## Join our Discord

[![Join our Discord](https://github.com/gabengaGamer/area51-pc/assets/54669564/bac6c8a8-2d95-4513-8943-c5c26bd09173)](https://discord.gg/7gGhFSjxsq)

## Usage
```bash
Usage: CDFSManager <command> [options]

Commands:
  pack <input_dir|file_list.txt> <output_file> [--sector-size SIZE] [--cache-size SIZE] [--debug]   Creates a CDFS archive from the specified directory.
  unpack <input_file> <output_dir> [--debug]                                                        Unpacks files from the specified CDFS archive to the given directory.
  list <input_file> [--write-list list.txt]                                                         Lists the contents of the specified CDFS archive.
  verify <input_file>                                                                               Verifies the integrity of the specified CDFS archive.
  help                                                                                              Displays this help text.

Examples:
  CDFSManager pack my_folder output.dat
  CDFSManager pack file_list.txt output.dat
  CDFSManager unpack archive.dat my_folder
  CDFSManager list archive.dat
  CDFSManager list archive.dat --write-list list.txt
  CDFSManager verify archive.dat
```
