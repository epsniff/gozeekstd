package main

import (
	"bytes"
	"flag"
	"fmt"

	"io"
	"os"
	"strings"

	"github.com/epsniff/gozeekstd/src/gzstd"
	"github.com/klauspost/compress/zstd"
)

const (
	defaultCompressionLevel = 3
	defaultFrameSize        = "512K"
	programName             = "gzstd"
	fileExtension           = ".zst"
)

// Options holds command-line options
type Options struct {
	Decompress bool
	List       bool
	Stdout     bool
	Force      bool
	Keep       bool
	Remove     bool // New flag to explicitly remove original files
	Quiet      bool
	Verbose    bool
	Test       bool
	Level      int
	FrameSize  string
	StartFrame uint32
	EndFrame   uint32
}

func main() {
	opts := parseOptions()

	files := flag.Args()
	if len(files) == 0 {
		files = []string{"-"} // Default to stdin
	}

	// Execute the appropriate operation
	var exitCode int
	for _, file := range files {
		var err error
		switch {
		case opts.List:
			err = listFile(file, opts)
		case opts.Test:
			err = testFile(file, opts)
		case opts.Decompress:
			err = decompressFile(file, opts)
		default:
			err = compressFile(file, opts)
		}

		if err != nil {
			if !opts.Quiet {
				fmt.Fprintf(os.Stderr, "%s: %s: %v\n", programName, file, err)
			}
			exitCode = 1
		}
	}

	os.Exit(exitCode)
}

func parseOptions() *Options {
	opts := &Options{}

	// Decompress flags (multiple aliases like gzip)
	flag.BoolVar(&opts.Decompress, "d", false, "decompress")
	flag.BoolVar(&opts.Decompress, "decompress", false, "decompress")
	flag.BoolVar(&opts.Decompress, "uncompress", false, "decompress")

	// List/test flags
	flag.BoolVar(&opts.List, "l", false, "list compressed file contents")
	flag.BoolVar(&opts.List, "list", false, "list compressed file contents")
	flag.BoolVar(&opts.Test, "t", false, "test compressed file integrity")
	flag.BoolVar(&opts.Test, "test", false, "test compressed file integrity")

	// Output flags
	flag.BoolVar(&opts.Stdout, "c", false, "write to stdout")
	flag.BoolVar(&opts.Stdout, "stdout", false, "write to stdout")
	flag.BoolVar(&opts.Keep, "k", false, "keep original files (deprecated, now default)")
	flag.BoolVar(&opts.Keep, "keep", false, "keep original files (deprecated, now default)")
	flag.BoolVar(&opts.Remove, "rm", false, "remove original files after successful compression")
	flag.BoolVar(&opts.Remove, "remove", false, "remove original files after successful compression")

	// Behavior flags
	flag.BoolVar(&opts.Force, "f", false, "force overwrite")
	flag.BoolVar(&opts.Force, "force", false, "force overwrite")
	flag.BoolVar(&opts.Quiet, "q", false, "suppress all warnings")
	flag.BoolVar(&opts.Quiet, "quiet", false, "suppress all warnings")
	flag.BoolVar(&opts.Verbose, "v", false, "verbose mode")
	flag.BoolVar(&opts.Verbose, "verbose", false, "verbose mode")

	// Compression options
	flag.IntVar(&opts.Level, "1", 1, "fastest compression")
	flag.IntVar(&opts.Level, "2", 2, "")
	flag.IntVar(&opts.Level, "3", 3, "")
	flag.IntVar(&opts.Level, "4", 4, "")
	flag.IntVar(&opts.Level, "5", 5, "")
	flag.IntVar(&opts.Level, "6", 6, "")
	flag.IntVar(&opts.Level, "7", 7, "")
	flag.IntVar(&opts.Level, "8", 8, "")
	flag.IntVar(&opts.Level, "9", 9, "best compression")
	flag.IntVar(&opts.Level, "best", 9, "best compression")
	flag.IntVar(&opts.Level, "fast", 1, "fastest compression")

	// Extended options
	flag.StringVar(&opts.FrameSize, "frame-size", defaultFrameSize, "seekable frame size")
	var startFrame, endFrame uint
	flag.UintVar(&startFrame, "start-frame", 0, "start decompression at frame")
	flag.UintVar(&endFrame, "end-frame", 0, "end decompression at frame")

	flag.Parse()

	// Set default compression level if not explicitly set
	if opts.Level == 0 {
		opts.Level = defaultCompressionLevel
	}

	// Convert uint to uint32
	opts.StartFrame = uint32(startFrame)
	opts.EndFrame = uint32(endFrame)

	// Default is to keep files (unless -rm is specified)
	// The -k flag is now deprecated but still works
	if !opts.Remove {
		opts.Keep = true
	}

	return opts
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
	outputFile := getOutputFileName(inputFile, fileExtension, opts.Stdout)

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
		if opts.Remove {
			fmt.Printf("%s:\t%.1f%% -- replaced with %s\n", inputFile, ratio, outputFile)
		} else {
			fmt.Printf("%s:\t%.1f%% -- compressed to %s\n", inputFile, ratio, outputFile)
		}
	}

	// Remove original file only if explicitly requested
	if opts.Remove && inputFile != "-" && outputFile != "-" {
		if err := os.Remove(inputFile); err != nil {
			return err
		}
	}

	// Preserve file times
	if inputInfo != nil && outputFile != "-" {
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
	if inputFile != "-" && !strings.HasSuffix(inputFile, fileExtension) {
		return fmt.Errorf("unknown suffix -- ignored")
	}

	// Determine output
	outputFile := getOutputFileName(inputFile, "", opts.Stdout)
	if outputFile == inputFile {
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

	// Remove original file only if explicitly requested
	if opts.Remove && inputFile != "-" && outputFile != "-" {
		if err := os.Remove(inputFile); err != nil {
			return err
		}
	}

	// Preserve file times
	if inputInfo != nil && outputFile != "-" {
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
			strings.TrimSuffix(inputFile, fileExtension))

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
		fmt.Printf("%12d %12d %5.1f%% %s\n",
			totalCompressed,
			totalDecompressed,
			ratio,
			inputFile)
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
	if strings.HasSuffix(inputFile, fileExtension) {
		return strings.TrimSuffix(inputFile, fileExtension)
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
