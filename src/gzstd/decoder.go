package gzstd

import (
	"bytes"
	"errors"
	"io"

	"github.com/klauspost/compress/zstd"
)

// Seekable represents a seekable source
type Seekable interface {
	io.Reader
	io.Seeker
}

// DecoderOptions configures the decoder
type DecoderOptions struct {
	SeekTable    *SeekTable
	LowerFrame   uint32
	UpperFrame   uint32
	Dict         []byte
	MaxWindowLog int
}

// DefaultDecoderOptions returns default decoder options
func DefaultDecoderOptions() *DecoderOptions {
	return &DecoderOptions{
		MaxWindowLog: 27, // 128MB max window
		LowerFrame:   0,
		UpperFrame:   0,
	}
}

// Decoder handles seekable decompression
type Decoder struct {
	source       Seekable
	decoder      *zstd.Decoder
	options      *DecoderOptions
	seekTable    *SeekTable
	currentFrame uint32
	frameData    []byte
	framePos     int
	decompressed bytes.Buffer
	lowerFrame   uint32
	upperFrame   uint32
	totalRead    uint64
	eofReached   bool
}

// NewDecoder creates a new seekable decoder
func NewDecoder(source Seekable, opts *DecoderOptions) (*Decoder, error) {
	if opts == nil {
		opts = DefaultDecoderOptions()
	}

	// Try to read seek table from source
	var seekTable *SeekTable
	if opts.SeekTable != nil {
		seekTable = opts.SeekTable
	} else {
		// Try to read seek table from the end of file
		footer, err := ReadSeekTableFooter(source)
		if err == nil {
			seekTableSize, err := ParseSeekTableSize(footer)
			if err == nil {
				// Seek to start of seek table
				currentPos, _ := source.Seek(0, io.SeekCurrent)
				if _, err := source.Seek(-int64(seekTableSize), io.SeekEnd); err == nil {
					seekTableData := make([]byte, seekTableSize)
					if _, err := io.ReadFull(source, seekTableData); err == nil {
						seekTable, _ = ParseSeekTable(seekTableData)
					}
				}
				// Restore position
				source.Seek(currentPos, io.SeekStart)
			}
		}
	}

	if seekTable == nil {
		return nil, errors.New("no seek table found")
	}

	decoderOpts := []zstd.DOption{
		zstd.WithDecoderConcurrency(1),
	}
	
	// Only set max window if it's large enough
	if opts.MaxWindowLog >= 10 { // 2^10 = 1024 bytes minimum
		decoderOpts = append(decoderOpts, zstd.WithDecoderMaxWindow(1 << uint(opts.MaxWindowLog)))
	}

	if len(opts.Dict) > 0 {
		decoderOpts = append(decoderOpts, zstd.WithDecoderDicts(opts.Dict))
	}

	decoder, err := zstd.NewReader(nil, decoderOpts...)
	if err != nil {
		return nil, err
	}

	d := &Decoder{
		source:       source,
		decoder:      decoder,
		options:      opts,
		seekTable:    seekTable,
		currentFrame: opts.LowerFrame,
		lowerFrame:   opts.LowerFrame,
		upperFrame:   opts.UpperFrame,
	}

	if d.upperFrame == 0 || d.upperFrame >= seekTable.NumFrames() {
		d.upperFrame = seekTable.NumFrames() - 1
	}

	// Seek to start of first frame
	if d.currentFrame > 0 {
		startOffset, err := seekTable.FrameStartComp(d.currentFrame)
		if err != nil {
			return nil, err
		}
		if _, err := source.Seek(int64(startOffset), io.SeekStart); err != nil {
			return nil, err
		}
	} else {
		// Ensure we're at the start
		if _, err := source.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
	}

	return d, nil
}

// Read implements io.Reader
func (d *Decoder) Read(p []byte) (int, error) {
	return d.ReadWithPrefix(p, nil)
}

// ReadWithPrefix reads decompressed data with optional prefix
func (d *Decoder) ReadWithPrefix(p []byte, prefix []byte) (int, error) {
	if d.eofReached {
		return 0, io.EOF
	}

	totalRead := 0

	for totalRead < len(p) && !d.eofReached {
		// If we have decompressed data, return it
		if d.decompressed.Len() > 0 {
			n, _ := d.decompressed.Read(p[totalRead:])
			totalRead += n
			d.totalRead += uint64(n)
			continue
		}

		// Need to decompress more data
		if err := d.decompressNextFrame(prefix); err != nil {
			if err == io.EOF {
				d.eofReached = true
				if totalRead > 0 {
					return totalRead, nil
				}
			}
			return totalRead, err
		}
	}

	return totalRead, nil
}

// Seek implements io.Seeker
func (d *Decoder) Seek(offset int64, whence int) (int64, error) {
	var targetOffset uint64

	switch whence {
	case io.SeekStart:
		targetOffset = uint64(offset)
	case io.SeekCurrent:
		targetOffset = d.totalRead + uint64(offset)
	case io.SeekEnd:
		totalSize, err := d.seekTable.FrameEndDecomp(d.seekTable.NumFrames() - 1)
		if err != nil {
			return 0, err
		}
		targetOffset = totalSize + uint64(offset)
	default:
		return 0, errors.New("invalid whence")
	}

	// Find the frame containing the target offset
	targetFrame := d.findFrameAtOffset(targetOffset)
	if targetFrame < d.lowerFrame {
		targetFrame = d.lowerFrame
	}
	if targetFrame > d.upperFrame {
		targetFrame = d.upperFrame
	}

	// Seek to the frame start
	frameStartDecomp, err := d.seekTable.FrameStartDecomp(targetFrame)
	if err != nil {
		return 0, err
	}

	frameStartComp, err := d.seekTable.FrameStartComp(targetFrame)
	if err != nil {
		return 0, err
	}

	if _, err := d.source.Seek(int64(frameStartComp), io.SeekStart); err != nil {
		return 0, err
	}

	// Reset decoder state
	d.currentFrame = targetFrame
	d.decompressed.Reset()
	d.totalRead = frameStartDecomp
	d.eofReached = false

	// If target is within the frame, decompress and skip to target
	if targetOffset > frameStartDecomp {
		skipBytes := targetOffset - frameStartDecomp
		skipBuf := make([]byte, 4096)
		for skipBytes > 0 {
			toSkip := skipBytes
			if toSkip > uint64(len(skipBuf)) {
				toSkip = uint64(len(skipBuf))
			}
			n, err := d.Read(skipBuf[:toSkip])
			if err != nil {
				return 0, err
			}
			skipBytes -= uint64(n)
		}
	}

	return int64(d.totalRead), nil
}

// SeekTable returns the decoder's seek table
func (d *Decoder) SeekTable() *SeekTable {
	return d.seekTable
}

// SetLowerFrame sets the lower frame boundary
func (d *Decoder) SetLowerFrame(frame uint32) {
	d.lowerFrame = frame
	if d.currentFrame < frame {
		d.currentFrame = frame
	}
}

// SetUpperFrame sets the upper frame boundary
func (d *Decoder) SetUpperFrame(frame uint32) {
	d.upperFrame = frame
	if d.upperFrame >= d.seekTable.NumFrames() {
		d.upperFrame = d.seekTable.NumFrames() - 1
	}
}

func (d *Decoder) decompressNextFrame(prefix []byte) error {
	if d.currentFrame > d.upperFrame {
		return io.EOF
	}

	// Get frame size
	frameSize, err := d.seekTable.FrameSizeComp(d.currentFrame)
	if err != nil {
		return err
	}

	// Read compressed frame
	compressedData := make([]byte, frameSize)
	if _, err := io.ReadFull(d.source, compressedData); err != nil {
		return err
	}

	// Decompress frame
	var decompressed []byte
	if prefix != nil && d.currentFrame == d.lowerFrame {
		// For first frame, prepend prefix before decompression
		combined := append(prefix, compressedData...)
		decompressed, err = d.decoder.DecodeAll(combined, nil)
		if err != nil {
			// Try without prefix
			decompressed, err = d.decoder.DecodeAll(compressedData, nil)
		}
	} else {
		decompressed, err = d.decoder.DecodeAll(compressedData, nil)
	}

	if err != nil {
		return err
	}

	d.decompressed.Write(decompressed)
	d.currentFrame++

	return nil
}

func (d *Decoder) findFrameAtOffset(offset uint64) uint32 {
	if offset == 0 {
		return 0
	}

	numFrames := d.seekTable.NumFrames()
	if offset >= d.mustFrameEndDecomp(numFrames-1) {
		return numFrames - 1
	}

	low := uint32(0)
	high := numFrames

	for low+1 < high {
		mid := (low + high) / 2
		midOffset := d.mustFrameEndDecomp(mid)
		if offset < midOffset {
			high = mid
		} else {
			low = mid
		}
	}

	if offset < d.mustFrameEndDecomp(low) {
		return low
	}
	return high
}

func (d *Decoder) mustFrameEndDecomp(frame uint32) uint64 {
	offset, _ := d.seekTable.FrameEndDecomp(frame)
	return offset
}
