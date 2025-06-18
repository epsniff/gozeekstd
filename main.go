package main

import (
	"flag"
	"fmt"
	"gozeekstd/src/gzstd"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/zstd"
)

func main() {
	compress := flag.Bool("c", false, "Compress mode")
	decompress := flag.Bool("d", false, "Decompress mode")
	list := flag.Bool("l", false, "List frames in seekable archive")
	level := flag.Int("level", 3, "Compression level (1-19)")
	frameSize := flag.String("frame-size", "512K", "Frame size (e.g., 512K, 1M, 4M)")
	output := flag.String("o", "", "Output file (default: stdout or input.zst)")
	force := flag.Bool("f", false, "Force overwrite existing files")
	startFrame := flag.Uint("start-frame", 0, "Start decompression at frame")
	endFrame := flag.Uint("end-frame", 0, "End decompression at frame (0 = end)")

	flag.Parse()

	// Default to compression if no mode specified
	if !*compress && !*decompress && !*list {
		*compress = true
	}

	// Validate modes
	modeCount := 0
	if *compress {
		modeCount++
	}
	if *decompress {
		modeCount++
	}
	if *list {
		modeCount++
	}
	if modeCount > 1 {
		fmt.Fprintf(os.Stderr, "Error: Only one mode (-c, -d, -l) can be specified\n")
		os.Exit(1)
	}

	// Get input file
	inputFile := "-"
	if flag.NArg() > 0 {
		inputFile = flag.Arg(0)
	}

	// Execute the appropriate mode
	var err error
	switch {
	case *list:
		err = listFrames(inputFile)
	case *decompress:
		err = decompressFile(inputFile, *output, *force, uint32(*startFrame), uint32(*endFrame))
	default:
		err = compressFile(inputFile, *output, *force, *level, *frameSize)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func compressFile(inputFile, outputFile string, force bool, level int, frameSizeStr string) error {
	// Parse frame size
	frameSize, err := parseByteSize(frameSizeStr)
	if err != nil {
		return fmt.Errorf("invalid frame size: %v", err)
	}

	// Open input
	var input io.ReadCloser
	if inputFile == "-" {
		input = os.Stdin
	} else {
		f, err := os.Open(inputFile)
		if err != nil {
			return err
		}
		defer f.Close()
		input = f
	}

	// Determine output file
	if outputFile == "" {
		if inputFile == "-" {
			outputFile = "-"
		} else {
			outputFile = inputFile + ".zst"
		}
	}

	// Open output
	var output io.WriteCloser
	if outputFile == "-" {
		output = os.Stdout
	} else {
		// Check if file exists
		if !force {
			if _, err := os.Stat(outputFile); err == nil {
				return fmt.Errorf("output file exists: %s (use -f to overwrite)", outputFile)
			}
		}
		f, err := os.Create(outputFile)
		if err != nil {
			return err
		}
		defer f.Close()
		output = f
	}

	// Create encoder
	opts := gzstd.DefaultEncoderOptions()
	opts.Level = zstd.SpeedDefault
	opts.FramePolicy = gzstd.CompressedFrameSize{Size: uint32(frameSize)}

	encoder, err := gzstd.NewEncoder(output, opts)
	if err != nil {
		return err
	}

	// Compress data
	buf := make([]byte, 32*1024)
	for {
		n, err := input.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}

		if _, err := encoder.Write(buf[:n]); err != nil {
			return err
		}
	}

	// Finish compression
	if err := encoder.Finish(); err != nil {
		return err
	}

	// Print statistics if not stdout
	if outputFile != "-" {
		fmt.Printf("Compressed %s -> %s\n", inputFile, outputFile)
		fmt.Printf("Frames: %d, Compressed size: %d bytes\n",
			encoder.SeekTable().NumFrames(), encoder.WrittenCompressed())
	}

	return nil
}

func decompressFile(inputFile, outputFile string, force bool, startFrame, endFrame uint32) error {
	// Open input
	var input *os.File
	if inputFile == "-" {
		input = os.Stdin
	} else {
		f, err := os.Open(inputFile)
		if err != nil {
			return err
		}
		defer f.Close()
		input = f
	}

	// Determine output file
	if outputFile == "" {
		if inputFile == "-" {
			outputFile = "-"
		} else {
			outputFile = strings.TrimSuffix(inputFile, ".zst")
			if outputFile == inputFile {
				outputFile = inputFile + ".out"
			}
		}
	}

	// Open output
	var output io.WriteCloser
	if outputFile == "-" {
		output = os.Stdout
	} else {
		// Check if file exists
		if !force {
			if _, err := os.Stat(outputFile); err == nil {
				return fmt.Errorf("output file exists: %s (use -f to overwrite)", outputFile)
			}
		}
		f, err := os.Create(outputFile)
		if err != nil {
			return err
		}
		defer f.Close()
		output = f
	}

	// Create decoder
	opts := gzstd.DefaultDecoderOptions()
	opts.LowerFrame = startFrame
	opts.UpperFrame = endFrame

	decoder, err := gzstd.NewDecoder(input, opts)
	if err != nil {
		return err
	}

	// Decompress data
	buf := make([]byte, 32*1024)
	totalBytes := int64(0)
	for {
		n, err := decoder.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}

		if _, err := output.Write(buf[:n]); err != nil {
			return err
		}
		totalBytes += int64(n)
	}

	// Print statistics if not stdout
	if outputFile != "-" {
		fmt.Printf("Decompressed %s -> %s\n", inputFile, outputFile)
		fmt.Printf("Decompressed size: %d bytes\n", totalBytes)
	}

	return nil
}

func listFrames(inputFile string) error {
	// Open input
	// var input *os.File
	if inputFile == "-" {
		return fmt.Errorf("cannot list frames from stdin")
	}

	f, err := os.Open(inputFile)
	if err != nil {
		return err
	}
	defer f.Close()

	// Read seek table
	footer, err := gzstd.ReadSeekTableFooter(f)
	if err != nil {
		return fmt.Errorf("failed to read seek table: %v", err)
	}

	seekTableSize, err := gzstd.ParseSeekTableSize(footer)
	if err != nil {
		return fmt.Errorf("failed to parse seek table size: %v", err)
	}

	// Read full seek table
	if _, err := f.Seek(-int64(seekTableSize), io.SeekEnd); err != nil {
		return err
	}

	seekTableData := make([]byte, seekTableSize)
	if _, err := io.ReadFull(f, seekTableData); err != nil {
		return err
	}

	seekTable, err := gzstd.ParseSeekTable(seekTableData)
	if err != nil {
		return fmt.Errorf("failed to parse seek table: %v", err)
	}

	// Print summary
	fmt.Printf("File: %s\n", filepath.Base(inputFile))
	fmt.Printf("Frames: %d\n", seekTable.NumFrames())

	totalCompressed := uint64(0)
	totalDecompressed := uint64(0)
	if seekTable.NumFrames() > 0 {
		totalCompressed, _ = seekTable.FrameEndComp(seekTable.NumFrames() - 1)
		totalDecompressed, _ = seekTable.FrameEndDecomp(seekTable.NumFrames() - 1)
	}

	fmt.Printf("Compressed size: %s (%d bytes)\n", formatBytes(totalCompressed), totalCompressed)
	fmt.Printf("Decompressed size: %s (%d bytes)\n", formatBytes(totalDecompressed), totalDecompressed)
	if totalCompressed > 0 {
		ratio := float64(totalDecompressed) / float64(totalCompressed)
		fmt.Printf("Compression ratio: %.2f:1\n", ratio)
	}

	// Print frame details
	fmt.Printf("\nFrame details:\n")
	fmt.Printf("%-10s %-15s %-15s %-10s\n", "Frame", "Compressed", "Decompressed", "Ratio")
	fmt.Printf("%-10s %-15s %-15s %-10s\n", "-----", "----------", "------------", "-----")

	for i := uint32(0); i < seekTable.NumFrames(); i++ {
		cSize, _ := seekTable.FrameSizeComp(i)
		dSize, _ := seekTable.FrameSizeDecomp(i)
		ratio := float64(dSize) / float64(cSize)

		fmt.Printf("%-10d %-15s %-15s %-10.2f\n",
			i, formatBytes(cSize), formatBytes(dSize), ratio)
	}

	return nil
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

func formatBytes(bytes uint64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}
