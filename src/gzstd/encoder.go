package gzstd

import (
	"bytes"
	"io"

	"github.com/klauspost/compress/zstd"
)

const (
	MAX_FRAME_SIZE     = 1 << 32    // 4GB max frame size
	DEFAULT_FRAME_SIZE = 512 * 1024 // 512KB default
)

// FrameSizePolicy defines how frames are sized
type FrameSizePolicy interface {
	isFrameSizePolicy()
	MaxSize() uint32
}

// CompressedFrameSize limits frame size by compressed bytes
type CompressedFrameSize struct {
	Size uint32
}

func (c CompressedFrameSize) isFrameSizePolicy() {}
func (c CompressedFrameSize) MaxSize() uint32    { return c.Size }

// UncompressedFrameSize limits frame size by uncompressed bytes
type UncompressedFrameSize struct {
	Size uint32
}

func (u UncompressedFrameSize) isFrameSizePolicy() {}
func (u UncompressedFrameSize) MaxSize() uint32    { return u.Size }

// EncoderOptions configures the encoder
type EncoderOptions struct {
	Level           zstd.EncoderLevel
	FramePolicy     FrameSizePolicy
	ChecksumFlag    bool
	CompressionDict []byte
}

// DefaultEncoderOptions returns default encoder options
func DefaultEncoderOptions() *EncoderOptions {
	return &EncoderOptions{
		Level:        zstd.SpeedDefault,
		FramePolicy:  CompressedFrameSize{Size: DEFAULT_FRAME_SIZE},
		ChecksumFlag: true,
	}
}

// Encoder handles seekable compression
type Encoder struct {
	writer          io.Writer
	encoder         *zstd.Encoder
	options         *EncoderOptions
	seekTable       *SeekTable
	frameBuffer     bytes.Buffer
	frameCSize      uint64
	frameDSize      uint64
	writtenTotal    uint64
	currentFrameNum uint32
}

// NewEncoder creates a new seekable encoder
func NewEncoder(w io.Writer, opts *EncoderOptions) (*Encoder, error) {
	if opts == nil {
		opts = DefaultEncoderOptions()
	}

	encoderOpts := []zstd.EOption{
		zstd.WithEncoderLevel(opts.Level),
	}

	if opts.ChecksumFlag {
		encoderOpts = append(encoderOpts, zstd.WithEncoderCRC(true))
	}

	if len(opts.CompressionDict) > 0 {
		encoderOpts = append(encoderOpts, zstd.WithEncoderDict(opts.CompressionDict))
	}

	encoder, err := zstd.NewWriter(nil, encoderOpts...)
	if err != nil {
		return nil, err
	}

	return &Encoder{
		writer:    w,
		encoder:   encoder,
		options:   opts,
		seekTable: NewSeekTable(),
	}, nil
}

// Write implements io.Writer
func (e *Encoder) Write(p []byte) (int, error) {
	return e.WriteWithPrefix(p, nil)
}

// WriteWithPrefix writes data with an optional prefix
func (e *Encoder) WriteWithPrefix(p []byte, prefix []byte) (int, error) {
	totalWritten := 0

	for len(p) > 0 {
		remaining := e.remainingFrameSize()
		if remaining == 0 {
			if err := e.EndFrame(); err != nil {
				return totalWritten, err
			}
			remaining = e.remainingFrameSize()
		}

		toWrite := len(p)
		if toWrite > remaining {
			toWrite = remaining
		}

		// For the first write of a frame with prefix
		if e.frameDSize == 0 && prefix != nil {
			// Create a combined input
			combined := append(prefix, p[:toWrite]...)
			compressed := e.encoder.EncodeAll(combined, nil)

			e.frameBuffer.Write(compressed)
			e.frameCSize += uint64(len(compressed))
			e.frameDSize += uint64(toWrite) // Don't count prefix in decompressed size
		} else {
			// Normal compression
			compressed := e.encoder.EncodeAll(p[:toWrite], nil)
			e.frameBuffer.Write(compressed)
			e.frameCSize += uint64(len(compressed))
			e.frameDSize += uint64(toWrite)
		}

		totalWritten += toWrite
		p = p[toWrite:]

		if e.isFrameComplete() {
			if err := e.EndFrame(); err != nil {
				return totalWritten, err
			}
		}
	}

	return totalWritten, nil
}

// EndFrame finishes the current frame
func (e *Encoder) EndFrame() error {
	if e.frameDSize == 0 {
		return nil // No data in frame
	}

	// Write frame to output
	frameData := e.frameBuffer.Bytes()
	if _, err := e.writer.Write(frameData); err != nil {
		return err
	}

	// Log frame in seek table
	if err := e.seekTable.LogFrame(uint32(e.frameCSize), uint32(e.frameDSize)); err != nil {
		return err
	}

	e.writtenTotal += e.frameCSize
	e.currentFrameNum++

	// Reset for next frame
	e.frameBuffer.Reset()
	e.frameCSize = 0
	e.frameDSize = 0

	return nil
}

// Finish finalizes compression and writes the seek table
func (e *Encoder) Finish() error {
	return e.FinishWithFormat(FormatFoot)
}

// FinishWithFormat finalizes compression with specified seek table format
func (e *Encoder) FinishWithFormat(format Format) error {
	// End any remaining frame
	if err := e.EndFrame(); err != nil {
		return err
	}

	// Serialize and write seek table
	serializer := e.seekTable.NewSerializer(format)
	buf := make([]byte, 4096)

	for {
		n := serializer.WriteTo(buf)
		if n == 0 {
			break
		}
		if _, err := e.writer.Write(buf[:n]); err != nil {
			return err
		}
	}

	// Close the encoder
	e.encoder.Close()

	return nil
}

// SeekTable returns the current seek table
func (e *Encoder) SeekTable() *SeekTable {
	return e.seekTable
}

// WrittenCompressed returns total compressed bytes written
func (e *Encoder) WrittenCompressed() uint64 {
	return e.writtenTotal
}

func (e *Encoder) remainingFrameSize() int {
	switch policy := e.options.FramePolicy.(type) {
	case CompressedFrameSize:
		remaining := int64(policy.Size) - int64(e.frameCSize)
		if remaining < 0 {
			return 0
		}
		maxRemaining := int64(MAX_FRAME_SIZE) - int64(e.frameDSize)
		if remaining > maxRemaining {
			return int(maxRemaining)
		}
		return int(remaining)
	case UncompressedFrameSize:
		remaining := int64(policy.Size) - int64(e.frameDSize)
		if remaining < 0 {
			return 0
		}
		if remaining > MAX_FRAME_SIZE {
			return MAX_FRAME_SIZE
		}
		return int(remaining)
	default:
		return 0
	}
}

func (e *Encoder) isFrameComplete() bool {
	switch policy := e.options.FramePolicy.(type) {
	case CompressedFrameSize:
		return e.frameCSize >= uint64(policy.Size) || e.frameDSize >= MAX_FRAME_SIZE
	case UncompressedFrameSize:
		maxSize := uint64(policy.Size)
		if maxSize > MAX_FRAME_SIZE {
			maxSize = MAX_FRAME_SIZE
		}
		return e.frameDSize >= maxSize
	default:
		return true
	}
}
