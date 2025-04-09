## Entropy-DAT-Tool

Made specifically for [Tribes: Aerial Assault](https://en.wikipedia.org/wiki/Tribes:_Aerial_Assault)

## Join our Discord

[![Join our Discord](https://github.com/gabengaGamer/area51-pc/assets/54669564/bac6c8a8-2d95-4513-8943-c5c26bd09173)](https://discord.gg/7gGhFSjxsq)

## Usage
```bash
Usage: CDFSManager <command> [options]

Commands:
  pack <input_dir> <output_file> [--sector-size SIZE] [--cache-size SIZE] [--debug]   Creates a CDFS archive from the specified directory.
  unpack <input_file> <output_dir> [--debug]                                          Unpacks files from the specified CDFS archive to the given directory.
  list <input_file>                                                                   Lists the contents of the specified CDFS archive.
  verify <input_file>                                                                 Verifies the integrity of the specified CDFS archive.
  help                                                                                Displays this help text.

Examples:
  CDFSManager pack my_folder output.dat
  CDFSManager unpack archive.dat my_folder
  CDFSManager list archive.dat
  CDFSManager verify archive.dat
```
