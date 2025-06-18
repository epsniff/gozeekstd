package main

import (
	"bytes"
	"flag"
	"fmt"
	"path/filepath"

	"io"
	"os"
	"strings"

	"github.com/epsniff/gozeekstd/src/gzstd"
	"github.com/klauspost/compress/zstd"
)

const (
	defaultCompressionLevel = 6
	defaultFrameSize        = "512K"
	programName             = "gzstd"
	fileExtension           = ".zst"
	version                 = "1.0.0"
)

// Options holds command-line options
type Options struct {
	Decompress   bool
	DecompressTo string // Output filename for decompression
	List         bool
	Stdout       bool
	Force        bool
	Keep         bool
	NoKeep       bool
	Quiet        bool
	Verbose      bool
	Test         bool
	Level        int
	FrameSize    string
	StartFrame   uint32
	EndFrame     uint32
	Recursive    bool
	Suffix       string
	NoName       bool
	Name         bool
	Help         bool
	Version      bool
}

func main() {
	opts := parseOptions()

	// Handle help and version
	if opts.Help {
		showHelp()
		os.Exit(0)
	}
	if opts.Version {
		fmt.Printf("%s version %s\n", programName, version)
		os.Exit(0)
	}

	files := flagSet.Args()
	if len(files) == 0 {
		files = []string{"-"} // Default to stdin
	}

	// Process files
	var exitCode int
	for _, file := range files {
		if err := processFile(file, opts); err != nil {
			if !opts.Quiet {
				fmt.Fprintf(os.Stderr, "%s: %s: %v\n", programName, file, err)
			}
			exitCode = 1
		}
	}

	os.Exit(exitCode)
}

func processFile(file string, opts *Options) error {
	// Handle recursive directory processing
	if opts.Recursive && file != "-" {
		info, err := os.Stat(file)
		if err != nil {
			return err
		}
		if info.IsDir() {
			return processDirectory(file, opts)
		}
	}

	// Process single file
	switch {
	case opts.List:
		return listFile(file, opts)
	case opts.Test:
		return testFile(file, opts)
	case opts.Decompress:
		return decompressFile(file, opts)
	default:
		return compressFile(file, opts)
	}
}

func processDirectory(dir string, opts *Options) error {
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		
		// Skip directories
		if info.IsDir() {
			return nil
		}
		
		// Process based on operation
		if opts.Decompress {
			// Only process files with compression suffix
			if strings.HasSuffix(path, opts.Suffix) {
				return processFile(path, opts)
			}
		} else {
			// Skip already compressed files
			if !strings.HasSuffix(path, opts.Suffix) {
				return processFile(path, opts)
			}
		}
		
		return nil
	})
}

func parseOptions() *Options {
	opts := &Options{
		Suffix: fileExtension,
	}

	// Custom flag set to control parsing
	flagSet := flag.NewFlagSet(programName, flag.ContinueOnError)
	flagSet.SetOutput(io.Discard) // Suppress default error output

	// Decompress flags
	flagSet.BoolVar(&opts.Decompress, "d", false, "decompress")
	flagSet.BoolVar(&opts.Decompress, "decompress", false, "decompress")
	flagSet.StringVar(&opts.DecompressTo, "do", "", "decompress to specified output file")

	// Compression level (removed -c short flag to avoid conflict)
	flagSet.IntVar(&opts.Level, "compression", defaultCompressionLevel, "compression level (1-9)")
	
	// Keep/no-keep flags
	flagSet.BoolVar(&opts.NoKeep, "nk", false, "don't keep original files")
	flagSet.BoolVar(&opts.NoKeep, "no-keep", false, "don't keep original files")

	// Output control
	flagSet.BoolVar(&opts.Stdout, "c", false, "write to stdout")
	flagSet.BoolVar(&opts.Stdout, "stdout", false, "write to stdout")
	
	// Name flags
	flagSet.BoolVar(&opts.NoName, "n", false, "don't save/restore original filename and timestamp")
	flagSet.BoolVar(&opts.NoName, "no-name", false, "don't save/restore original filename and timestamp")
	flagSet.BoolVar(&opts.Name, "N", true, "save/restore original filename and timestamp")
	flagSet.BoolVar(&opts.Name, "name", true, "save/restore original filename and timestamp")

	// Information and testing
	flagSet.BoolVar(&opts.List, "l", false, "list compressed file contents")
	flagSet.BoolVar(&opts.List, "list", false, "list compressed file contents")
	flagSet.BoolVar(&opts.Test, "t", false, "test compressed file integrity")
	flagSet.BoolVar(&opts.Test, "test", false, "test compressed file integrity")
	flagSet.BoolVar(&opts.Verbose, "v", false, "verbose mode")
	flagSet.BoolVar(&opts.Verbose, "verbose", false, "verbose mode")
	flagSet.BoolVar(&opts.Quiet, "q", false, "suppress warnings")
	flagSet.BoolVar(&opts.Quiet, "quiet", false, "suppress warnings")

	// Other options
	flagSet.BoolVar(&opts.Recursive, "r", false, "recursively compress files in directories")
	flagSet.BoolVar(&opts.Recursive, "recursive", false, "recursively compress files in directories")
	flagSet.StringVar(&opts.Suffix, "S", fileExtension, "use suffix instead of .zst")
	flagSet.StringVar(&opts.Suffix, "suffix", fileExtension, "use suffix instead of .zst")
	
	// Help and version
	flagSet.BoolVar(&opts.Help, "h", false, "display help message")
	flagSet.BoolVar(&opts.Help, "help", false, "display help message")
	flagSet.BoolVar(&opts.Version, "version", false, "show version information")

	// Force overwrite
	flagSet.BoolVar(&opts.Force, "f", false, "force overwrite")
	flagSet.BoolVar(&opts.Force, "force", false, "force overwrite")

	// Extended options
	flagSet.StringVar(&opts.FrameSize, "frame-size", defaultFrameSize, "seekable frame size")
	var startFrame, endFrame uint
	flagSet.UintVar(&startFrame, "start-frame", 0, "start decompression at frame")
	flagSet.UintVar(&endFrame, "end-frame", 0, "end decompression at frame")

	// Add compression level shortcuts (1-9) before parsing
	for i := 1; i <= 9; i++ {
		flagSet.Bool(fmt.Sprintf("%d", i), false, fmt.Sprintf("compression level %d", i))
	}

	// Parse flags
	if err := flagSet.Parse(os.Args[1:]); err != nil {
		if err == flag.ErrHelp {
			opts.Help = true
		} else {
			fmt.Fprintf(os.Stderr, "%s: %v\n", programName, err)
			fmt.Fprintf(os.Stderr, "Try '%s --help' for more information.\n", programName)
			os.Exit(1)
		}
	}

	// Handle -d=filename syntax
	for _, arg := range os.Args[1:] {
		if strings.HasPrefix(arg, "-d=") || strings.HasPrefix(arg, "--decompress=") {
			parts := strings.SplitN(arg, "=", 2)
			if len(parts) == 2 && parts[1] != "" {
				opts.Decompress = true
				opts.DecompressTo = parts[1]
			}
		}
	}

	// Handle compression level shortcuts
	for i := 1; i <= 9; i++ {
		if flagSet.Lookup(fmt.Sprintf("%d", i)).Value.String() == "true" {
			opts.Level = i
			break
		}
	}

	// Convert uint to uint32
	opts.StartFrame = uint32(startFrame)
	opts.EndFrame = uint32(endFrame)

	// Set keep behavior
	opts.Keep = !opts.NoKeep

	// Handle -c flag with optional argument
	// If -c is followed by a number 1-9, it's compression level, otherwise stdout
	rawArgs := os.Args[1:]
	for i, arg := range rawArgs {
		if arg == "-c" && i+1 < len(rawArgs) {
			// Check if next arg is a number 1-9
			nextArg := rawArgs[i+1]
			if len(nextArg) == 1 && nextArg[0] >= '1' && nextArg[0] <= '9' {
				// It's a compression level
				level, _ := fmt.Sscanf(nextArg, "%d", &opts.Level)
				opts.Stdout = false
				_ = level
			}
		} else if strings.HasPrefix(arg, "-c") && len(arg) == 3 && arg[2] >= '1' && arg[2] <= '9' {
			// Handle -c1 through -c9 syntax
			opts.Level = int(arg[2] - '0')
			opts.Stdout = false
		}
	}

	// If name flags weren't explicitly set, default to true
	if !opts.NoName {
		opts.Name = true
	}

	return opts
}

func showHelp() {
	fmt.Printf(`%s - Seekable zstd compression utility

Basic Usage:
  %s -nk file.txt      Compress file.txt (creates file.txt%s and removes original)
  %s file.txt          Compress file.txt (creates file.txt%s and keeps the original)
  %s -d file.txt.zst   Decompress file
  %s -d file.txt.zst -do output.txt   Decompress to specific file

Compression Options:
  -1 to -9                 Compression level (1=fastest, 9=best compression, 6=default)
  --compression=LEVEL      Set compression level (1-9)
  -nk, --no-keep           Don't keep the original files (The default is to keep files)

Output Control:
  -c, --stdout             Write to standard output, keep original files
  -n, --no-name            Don't save/restore original filename and timestamp
  -N, --name               Save/restore original filename and timestamp (default)

Information and Testing:
  -l, --list               List compressed file contents
  -t, --test               Test compressed file integrity
  -v, --verbose            Display compression ratio and other info
  -q, --quiet              Suppress warnings

Other Options:
  -r, --recursive          Recursively compress files in directories
  -S, --suffix=SUF         Use suffix SUF instead of %s
  -h, --help               Display help message
  --version                Show version information
  -f, --force              Force overwrite of output files

Extended Options:
  --frame-size=SIZE        Set seekable frame size (default: %s)
  --start-frame=N          Start decompression at frame N
  --end-frame=N            End decompression at frame N

Examples:
  %s file.txt              # Compress file.txt to file.txt%s
  %s -d file.txt%s         # Decompress to file.txt
  %s -c file.txt > out%s   # Compress to stdout
  %s -l file.txt%s         # List archive contents
  %s -r directory          # Recursively compress files in directory

`, programName, programName, fileExtension, programName, fileExtension, programName,
		fileExtension, defaultFrameSize,
		programName, fileExtension,
		programName, fileExtension,
		programName, fileExtension,
		programName, fileExtension,
		programName)
}

func compressFile(inputFile string, opts *Options) error {
	// Parse frame size
	frameSize, err := parseByteSize(opts.FrameSize)
	if err != nil {
		return fmt.Errorf("invalid frame size: %v", err)
	}

	// Open input
	input, inputInfo, err := openInput(inputFile)
	if err != nil {
		return err
	}
	defer input.Close()

	// Determine output
	outputFile := getOutputFileName(inputFile, opts.Suffix, opts.Stdout)

	// Open output
	output, err := openOutput(outputFile, opts.Force)
	if err != nil {
		return err
	}

	// Setup cleanup
	var outputClosed bool
	defer func() {
		if !outputClosed {
			output.Close()
			// Remove partial output on error
			if outputFile != "-" && err != nil {
				os.Remove(outputFile)
			}
		}
	}()

	// Create encoder
	encoderOpts := gzstd.DefaultEncoderOptions()
	encoderOpts.Level = getZstdLevel(opts.Level)
	encoderOpts.FramePolicy = gzstd.CompressedFrameSize{Size: uint32(frameSize)}

	encoder, err := gzstd.NewEncoder(output, encoderOpts)
	if err != nil {
		return err
	}

	// Compress data
	written, err := io.Copy(encoder, input)
	if err != nil {
		return err
	}

	// Finish compression
	if err := encoder.Finish(); err != nil {
		return err
	}

	// Close output
	output.Close()
	outputClosed = true

	// Print statistics
	if opts.Verbose && outputFile != "-" {
		compressedSize := encoder.WrittenCompressed()
		ratio := float64(written) / float64(compressedSize) * 100
		if !opts.Keep {
			fmt.Printf("%s:\t%.1f%% -- replaced with %s\n", inputFile, ratio, outputFile)
		} else {
			fmt.Printf("%s:\t%.1f%% -- compressed to %s\n", inputFile, ratio, outputFile)
		}
	}

	// Remove original file if no-keep is set
	if !opts.Keep && inputFile != "-" && outputFile != "-" {
		if err := os.Remove(inputFile); err != nil {
			return err
		}
	}

	// Preserve file times if name preservation is enabled
	if opts.Name && inputInfo != nil && outputFile != "-" {
		os.Chtimes(outputFile, inputInfo.ModTime(), inputInfo.ModTime())
	}

	return nil
}

func decompressFile(inputFile string, opts *Options) error {
	// Open input
	input, inputInfo, err := openInput(inputFile)
	if err != nil {
		return err
	}
	defer input.Close()

	// Check if file has correct extension
	if inputFile != "-" && !strings.HasSuffix(inputFile, opts.Suffix) {
		return fmt.Errorf("unknown suffix -- ignored")
	}

	// Determine output
	var outputFile string
	if opts.DecompressTo != "" {
		outputFile = opts.DecompressTo
	} else {
		outputFile = getOutputFileName(inputFile, "", opts.Stdout)
	}
	
	// Check if we would overwrite the input file
	if outputFile == inputFile && inputFile != "-" {
		return fmt.Errorf("would overwrite input file")
	}

	// Open output
	output, err := openOutput(outputFile, opts.Force)
	if err != nil {
		return err
	}

	// Setup cleanup
	var outputClosed bool
	defer func() {
		if !outputClosed {
			output.Close()
			// Remove partial output on error
			if outputFile != "-" && err != nil {
				os.Remove(outputFile)
			}
		}
	}()

	// Create decoder
	decoderOpts := gzstd.DefaultDecoderOptions()
	decoderOpts.LowerFrame = opts.StartFrame
	decoderOpts.UpperFrame = opts.EndFrame

	// Create seekable reader if needed
	var seekableInput gzstd.Seekable
	if inputFile == "-" {
		// For stdin, we need to buffer the entire input
		data, err := io.ReadAll(input)
		if err != nil {
			return err
		}
		seekableInput = bytes.NewReader(data)
	} else {
		seekableInput = input.(*os.File)
	}

	decoder, err := gzstd.NewDecoder(seekableInput, decoderOpts)
	if err != nil {
		return err
	}

	// Decompress data
	_, err = io.Copy(output, decoder)
	if err != nil {
		return err
	}

	// Close output
	output.Close()
	outputClosed = true

	// Print statistics
	if opts.Verbose && outputFile != "-" {
		fmt.Printf("%s:\t%s\n", inputFile, outputFile)
	}

	// Remove original file if no-keep is set
	if !opts.Keep && inputFile != "-" && outputFile != "-" {
		if err := os.Remove(inputFile); err != nil {
			return err
		}
	}

	// Preserve file times if name preservation is enabled
	if opts.Name && inputInfo != nil && outputFile != "-" {
		os.Chtimes(outputFile, inputInfo.ModTime(), inputInfo.ModTime())
	}

	return nil
}

func listFile(inputFile string, opts *Options) error {
	if inputFile == "-" {
		return fmt.Errorf("cannot list from stdin")
	}

	f, err := os.Open(inputFile)
	if err != nil {
		return err
	}
	defer f.Close()

	// Get file info
	info, err := f.Stat()
	if err != nil {
		return err
	}

	// Read seek table
	seekTable, err := readSeekTable(f)
	if err != nil {
		return err
	}

	// Calculate totals
	totalCompressed := uint64(0)
	totalDecompressed := uint64(0)
	if seekTable.NumFrames() > 0 {
		totalCompressed, _ = seekTable.FrameEndComp(seekTable.NumFrames() - 1)
		totalDecompressed, _ = seekTable.FrameEndDecomp(seekTable.NumFrames() - 1)
	}

	// Add seek table overhead to compressed size
	totalCompressed = uint64(info.Size())

	// Print in gzip-like format
	ratio := 0.0
	if totalDecompressed > 0 {
		ratio = float64(totalCompressed) / float64(totalDecompressed) * 100
	}

	if opts.Verbose {
		// Verbose format with frame details
		fmt.Printf("method  crc     date  time  compressed uncompressed  ratio uncompressed_name\n")
		fmt.Printf("defla 00000000 %s %12d %12d %5.1f%% %s\n",
			info.ModTime().Format("Jan _2 15:04"),
			totalCompressed,
			totalDecompressed,
			ratio,
			strings.TrimSuffix(inputFile, opts.Suffix))

		// Frame details
		fmt.Printf("\nFrames: %d\n", seekTable.NumFrames())
		for i := uint32(0); i < seekTable.NumFrames() && i < 10; i++ {
			cSize, _ := seekTable.FrameSizeComp(i)
			dSize, _ := seekTable.FrameSizeDecomp(i)
			fmt.Printf("  Frame %d: %d -> %d bytes\n", i, cSize, dSize)
		}
		if seekTable.NumFrames() > 10 {
			fmt.Printf("  ... and %d more frames\n", seekTable.NumFrames()-10)
		}
	} else {
		// Standard format
		uncompressedName := strings.TrimSuffix(inputFile, opts.Suffix)
		fmt.Printf("%12d %12d %5.1f%% %s\n",
			totalCompressed,
			totalDecompressed,
			ratio,
			uncompressedName)
	}

	return nil
}

func testFile(inputFile string, opts *Options) error {
	// Open input
	input, _, err := openInput(inputFile)
	if err != nil {
		return err
	}
	defer input.Close()

	// Create seekable reader
	var seekableInput gzstd.Seekable
	if inputFile == "-" {
		data, err := io.ReadAll(input)
		if err != nil {
			return err
		}
		seekableInput = bytes.NewReader(data)
	} else {
		seekableInput = input.(*os.File)
	}

	// Create decoder
	decoder, err := gzstd.NewDecoder(seekableInput, nil)
	if err != nil {
		return err
	}

	// Test by reading all data
	_, err = io.Copy(io.Discard, decoder)
	if err != nil {
		return err
	}

	if opts.Verbose {
		fmt.Printf("%s:\tOK\n", inputFile)
	}

	return nil
}

// Helper functions

func openInput(filename string) (io.ReadCloser, os.FileInfo, error) {
	if filename == "-" {
		return os.Stdin, nil, nil
	}

	f, err := os.Open(filename)
	if err != nil {
		return nil, nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}

	return f, info, nil
}

func openOutput(filename string, force bool) (io.WriteCloser, error) {
	if filename == "-" {
		return os.Stdout, nil
	}

	// Check if file exists
	if !force {
		if _, err := os.Stat(filename); err == nil {
			return nil, fmt.Errorf("file exists")
		}
	}

	return os.Create(filename)
}

func getOutputFileName(inputFile, extension string, toStdout bool) string {
	if toStdout || inputFile == "-" {
		return "-"
	}

	if extension != "" {
		// Compressing: add extension
		return inputFile + extension
	}

	// Decompressing: remove extension
	for _, suffix := range []string{".zst", ".gz", ".Z"} {
		if strings.HasSuffix(inputFile, suffix) {
			return strings.TrimSuffix(inputFile, suffix)
		}
	}

	return inputFile + ".out"
}

func getZstdLevel(level int) zstd.EncoderLevel {
	// Map 1-9 to zstd levels
	switch level {
	case 1:
		return zstd.SpeedFastest
	case 2:
		return zstd.SpeedDefault
	case 3:
		return zstd.SpeedDefault
	case 4:
		return zstd.SpeedBetterCompression
	case 5:
		return zstd.SpeedBetterCompression
	case 6:
		return zstd.SpeedBetterCompression
	case 7:
		return zstd.SpeedBestCompression
	case 8:
		return zstd.SpeedBestCompression
	case 9:
		return zstd.SpeedBestCompression
	default:
		return zstd.SpeedDefault
	}
}

func readSeekTable(f *os.File) (*gzstd.SeekTable, error) {
	footer, err := gzstd.ReadSeekTableFooter(f)
	if err != nil {
		return nil, err
	}

	seekTableSize, err := gzstd.ParseSeekTableSize(footer)
	if err != nil {
		return nil, err
	}

	if _, err := f.Seek(-int64(seekTableSize), io.SeekEnd); err != nil {
		return nil, err
	}

	seekTableData := make([]byte, seekTableSize)
	if _, err := io.ReadFull(f, seekTableData); err != nil {
		return nil, err
	}

	return gzstd.ParseSeekTable(seekTableData)
}

func parseByteSize(s string) (int64, error) {
	s = strings.ToUpper(strings.TrimSpace(s))

	// Extract numeric part
	numStr := ""
	unit := ""
	for i, r := range s {
		if r >= '0' && r <= '9' || r == '.' {
			numStr += string(r)
		} else {
			unit = s[i:]
			break
		}
	}

	if numStr == "" {
		return 0, fmt.Errorf("no numeric value found")
	}

	var num float64
	if _, err := fmt.Sscanf(numStr, "%f", &num); err != nil {
		return 0, err
	}

	multiplier := int64(1)
	switch strings.TrimSpace(unit) {
	case "", "B":
		multiplier = 1
	case "K", "KB", "KIB":
		multiplier = 1024
	case "M", "MB", "MIB":
		multiplier = 1024 * 1024
	case "G", "GB", "GIB":
		multiplier = 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("unknown unit: %s", unit)
	}

	return int64(num * float64(multiplier)), nil
}
