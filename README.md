# gzstd - Seekable Zstandard Compression

A command-line utility for creating and decompressing seekable Zstandard archives, compatible with the seekable format used by the official zstd implementation.

## Features

- **Seekable Compression**: Creates zstd archives that support random access
- **Frame-based Structure**: Divides data into frames for efficient seeking
- **Compatible Format**: Uses the same seekable format as the official zstd seekable implementation
- **Flexible Options**: Supports various compression levels and frame sizes
- **Directory Support**: Can recursively compress files in directories
- **Standard Interface**: Command-line interface similar to gzip/zstd

## Installation

```bash
go get github.com/epsniff/gozeekstd
cd $GOPATH/src/github.com/epsniff/gozeekstd
go build -o gzstd main.go
```

## Basic Usage

```bash
# Compress a file (keeps original by default)
gzstd file.txt                  # Creates file.txt.zst

# Compress and remove original
gzstd -nk file.txt              # Creates file.txt.zst, removes file.txt

# Decompress a file
gzstd -d file.txt.zst           # Creates file.txt

# Compress to stdout
gzstd -c file.txt > output.zst

# Decompress from stdin
cat file.txt.zst | gzstd -dc > file.txt
```

## Command Line Options

### Compression Options
- `-c, --compression=1-9` - Set compression level (1=fastest, 9=best, 6=default)
- `-nk, --no-keep` - Don't keep original files after compression

### Output Control
- `-c, --stdout` - Write to standard output, keep original files
- `-n, --no-name` - Don't save/restore original filename and timestamp
- `-N, --name` - Save/restore original filename and timestamp (default)

### Information and Testing
- `-l, --list` - List compressed file contents
- `-t, --test` - Test compressed file integrity
- `-v, --verbose` - Display compression ratio and other info
- `-q, --quiet` - Suppress warnings

### Other Options
- `-r, --recursive` - Recursively compress files in directories
- `-S, --suffix=SUF` - Use suffix SUF instead of .zst
- `-f, --force` - Force overwrite of output files
- `-h, --help` - Display help message
- `--version` - Show version information

### Extended Options
- `--frame-size=SIZE` - Set seekable frame size (default: 512K)
- `--start-frame=N` - Start decompression at frame N
- `--end-frame=N` - End decompression at frame N

## Examples

### Basic Compression/Decompression
```bash
# Compress a file
gzstd document.txt              # Creates document.txt.zst

# Decompress a file
gzstd -d document.txt.zst       # Creates document.txt

# Compress with maximum compression
gzstd -9 largefile.dat

# Compress and remove original
gzstd -nk temporary.log
```

### Working with Directories
```bash
# Recursively compress all files in a directory
gzstd -r /path/to/directory

# Recursively decompress all .zst files
gzstd -dr /path/to/directory
```

### Advanced Usage
```bash
# List contents of compressed file
gzstd -l archive.zst

# Test integrity of compressed file
gzstd -t archive.zst

# Use custom frame size for better seeking granularity
gzstd --frame-size=64K largefile.dat

# Decompress only frames 10-20
gzstd -d --start-frame=10 --end-frame=20 archive.zst
```

### Piping and Streaming
```bash
# Compress from pipe
tar cf - directory/ | gzstd -c > directory.tar.zst

# Decompress to pipe
gzstd -dc directory.tar.zst | tar xf -

# Compress multiple files into one archive
cat file1 file2 file3 | gzstd -c > combined.zst
```

## Frame Size Considerations

The frame size affects the granularity of seeking:
- **Smaller frames** (e.g., 64K): Better seeking precision, slightly larger file size
- **Larger frames** (e.g., 4M): Better compression ratio, coarser seeking

Default frame size is 512K, which provides a good balance.

## Compatibility

The seekable format is compatible with the official zstd seekable format. Archives created with gzstd can be decompressed with other tools that support the zstd seekable format.

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
