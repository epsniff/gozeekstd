package gzstd

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	// Magic numbers and constants
	SKIPPABLE_MAGIC_NUMBER = 0x184D2A5F
	SEEKABLE_MAGIC_NUMBER  = 0x8F92EAB1
	SKIPPABLE_HEADER_SIZE  = 8
	SEEK_TABLE_FOOTER_SIZE = 9
	SIZE_PER_FRAME         = 17
	SEEKABLE_MAX_FRAMES    = 0x8000000 // 134217728

	// Error messages
	ErrFrameIndexTooLarge = "frame index too large"
	ErrCorrupted          = "corrupted seek table"
	ErrInvalidMagic       = "invalid magic number"
)

// Format represents the seek table format
type Format int

const (
	FormatHead Format = iota
	FormatFoot
)

// Entry represents a seek table entry
type Entry struct {
	CompressedOffset   uint64
	DecompressedOffset uint64
}

// Frame represents a frame in the seek table
type Frame struct {
	CompressedSize   uint32
	DecompressedSize uint32
}

// SeekTable manages frame offsets for seekable archives
type SeekTable struct {
	entries []Entry
}

// NewSeekTable creates a new empty seek table
func NewSeekTable() *SeekTable {
	return &SeekTable{
		entries: []Entry{{CompressedOffset: 0, DecompressedOffset: 0}},
	}
}

// LogFrame adds a new frame to the seek table
func (st *SeekTable) LogFrame(compressedSize, decompressedSize uint32) error {
	if st.NumFrames() >= SEEKABLE_MAX_FRAMES {
		return errors.New(ErrFrameIndexTooLarge)
	}

	last := st.entries[len(st.entries)-1]
	st.entries = append(st.entries, Entry{
		CompressedOffset:   last.CompressedOffset + uint64(compressedSize),
		DecompressedOffset: last.DecompressedOffset + uint64(decompressedSize),
	})

	return nil
}

// NumFrames returns the number of frames in the seek table
func (st *SeekTable) NumFrames() uint32 {
	return uint32(len(st.entries) - 1)
}

// FrameStartComp returns the compressed offset of the frame start
func (st *SeekTable) FrameStartComp(index uint32) (uint64, error) {
	if index >= st.NumFrames() {
		return 0, errors.New(ErrFrameIndexTooLarge)
	}
	return st.entries[index].CompressedOffset, nil
}

// FrameStartDecomp returns the decompressed offset of the frame start
func (st *SeekTable) FrameStartDecomp(index uint32) (uint64, error) {
	if index >= st.NumFrames() {
		return 0, errors.New(ErrFrameIndexTooLarge)
	}
	return st.entries[index].DecompressedOffset, nil
}

// FrameEndComp returns the compressed offset of the frame end
func (st *SeekTable) FrameEndComp(index uint32) (uint64, error) {
	if index >= st.NumFrames() {
		return 0, errors.New(ErrFrameIndexTooLarge)
	}
	return st.entries[index+1].CompressedOffset, nil
}

// FrameEndDecomp returns the decompressed offset of the frame end
func (st *SeekTable) FrameEndDecomp(index uint32) (uint64, error) {
	if index >= st.NumFrames() {
		return 0, errors.New(ErrFrameIndexTooLarge)
	}
	return st.entries[index+1].DecompressedOffset, nil
}

// FrameSizeComp returns the compressed size of a frame
func (st *SeekTable) FrameSizeComp(index uint32) (uint64, error) {
	if index >= st.NumFrames() {
		return 0, errors.New(ErrFrameIndexTooLarge)
	}
	return st.entries[index+1].CompressedOffset - st.entries[index].CompressedOffset, nil
}

// FrameSizeDecomp returns the decompressed size of a frame
func (st *SeekTable) FrameSizeDecomp(index uint32) (uint64, error) {
	if index >= st.NumFrames() {
		return 0, errors.New(ErrFrameIndexTooLarge)
	}
	return st.entries[index+1].DecompressedOffset - st.entries[index].DecompressedOffset, nil
}

// MaxFrameSizeDecomp returns the maximum decompressed frame size
func (st *SeekTable) MaxFrameSizeDecomp() uint64 {
	var maxSize uint64
	for i := uint32(0); i < st.NumFrames(); i++ {
		size, _ := st.FrameSizeDecomp(i)
		if size > maxSize {
			maxSize = size
		}
	}
	return maxSize
}

// Serializer handles seek table serialization
type Serializer struct {
	frames     []Frame
	frameIndex int
	writePos   int
	format     Format
}

// NewSerializer creates a serializer from a seek table
func (st *SeekTable) NewSerializer(format Format) *Serializer {
	frames := make([]Frame, 0, len(st.entries)-1)
	for i := 0; i < len(st.entries)-1; i++ {
		frames = append(frames, Frame{
			CompressedSize:   uint32(st.entries[i+1].CompressedOffset - st.entries[i].CompressedOffset),
			DecompressedSize: uint32(st.entries[i+1].DecompressedOffset - st.entries[i].DecompressedOffset),
		})
	}

	return &Serializer{
		frames:     frames,
		frameIndex: 0,
		writePos:   0,
		format:     format,
	}
}

// EncodedLen returns the total encoded length
func (s *Serializer) EncodedLen() int {
	return SKIPPABLE_HEADER_SIZE + SEEK_TABLE_FOOTER_SIZE + len(s.frames)*SIZE_PER_FRAME
}

// WriteTo writes the serialized seek table
func (s *Serializer) WriteTo(buf []byte) int {
	bufPos := 0
	remaining := len(buf)

	// Write skippable header
	if s.writePos < SKIPPABLE_HEADER_SIZE {
		needed := SKIPPABLE_HEADER_SIZE - s.writePos
		if needed > remaining {
			needed = remaining
		}

		header := make([]byte, 8)
		binary.LittleEndian.PutUint32(header[0:4], SKIPPABLE_MAGIC_NUMBER)
		binary.LittleEndian.PutUint32(header[4:8], uint32(s.frameSize()))

		copy(buf[bufPos:], header[s.writePos:s.writePos+needed])
		bufPos += needed
		s.writePos += needed
		remaining -= needed
	}

	// Write integrity field for Head format
	if s.format == FormatHead && s.writePos >= SKIPPABLE_HEADER_SIZE && s.writePos < SKIPPABLE_HEADER_SIZE+SEEK_TABLE_FOOTER_SIZE {
		integrityPos := s.writePos - SKIPPABLE_HEADER_SIZE
		needed := SEEK_TABLE_FOOTER_SIZE - integrityPos
		if needed > remaining {
			needed = remaining
		}

		integrity := s.makeIntegrity()
		copy(buf[bufPos:], integrity[integrityPos:integrityPos+needed])
		bufPos += needed
		s.writePos += needed
		remaining -= needed
	}

	// Write frames
	startPos := SKIPPABLE_HEADER_SIZE
	if s.format == FormatHead {
		startPos += SEEK_TABLE_FOOTER_SIZE
	}

	for s.frameIndex < len(s.frames) && remaining > 0 {
		frameOffset := s.writePos - startPos
		framePos := frameOffset % SIZE_PER_FRAME
		frameIdx := frameOffset / SIZE_PER_FRAME

		if frameIdx >= len(s.frames) {
			break
		}

		frame := s.frames[frameIdx]
		frameData := make([]byte, SIZE_PER_FRAME)

		// Pack frame data
		binary.LittleEndian.PutUint32(frameData[0:4], frame.CompressedSize)
		binary.LittleEndian.PutUint32(frameData[4:8], frame.DecompressedSize)
		// Reserved byte at position 8 is already 0

		needed := SIZE_PER_FRAME - framePos
		if needed > remaining {
			needed = remaining
		}

		copy(buf[bufPos:], frameData[framePos:framePos+needed])
		bufPos += needed
		s.writePos += needed
		remaining -= needed

		if framePos+needed == SIZE_PER_FRAME {
			s.frameIndex++
		}
	}

	// Write integrity field for Foot format
	if s.format == FormatFoot {
		integrityStart := startPos + len(s.frames)*SIZE_PER_FRAME
		if s.writePos >= integrityStart && remaining > 0 {
			integrityPos := s.writePos - integrityStart
			needed := SEEK_TABLE_FOOTER_SIZE - integrityPos
			if needed > remaining {
				needed = remaining
			}

			integrity := s.makeIntegrity()
			copy(buf[bufPos:], integrity[integrityPos:integrityPos+needed])
			bufPos += needed
			s.writePos += needed
		}
	}

	return bufPos
}

func (s *Serializer) frameSize() int {
	return SEEK_TABLE_FOOTER_SIZE + len(s.frames)*SIZE_PER_FRAME
}

func (s *Serializer) makeIntegrity() []byte {
	integrity := make([]byte, SEEK_TABLE_FOOTER_SIZE)
	binary.LittleEndian.PutUint32(integrity[0:4], uint32(len(s.frames)))
	integrity[4] = 0 // descriptor byte
	binary.LittleEndian.PutUint32(integrity[5:9], SEEKABLE_MAGIC_NUMBER)
	return integrity
}

// ParseSeekTable parses a seek table from bytes
func ParseSeekTable(data []byte) (*SeekTable, error) {
	if len(data) < SEEK_TABLE_FOOTER_SIZE {
		return nil, errors.New(ErrCorrupted)
	}

	// Parse integrity footer
	footerStart := len(data) - SEEK_TABLE_FOOTER_SIZE
	footer := data[footerStart:]

	if binary.LittleEndian.Uint32(footer[5:9]) != SEEKABLE_MAGIC_NUMBER {
		return nil, errors.New(ErrInvalidMagic)
	}

	numFrames := binary.LittleEndian.Uint32(footer[0:4])
	if numFrames > SEEKABLE_MAX_FRAMES {
		return nil, errors.New(ErrFrameIndexTooLarge)
	}

	expectedSize := SKIPPABLE_HEADER_SIZE + SEEK_TABLE_FOOTER_SIZE + int(numFrames)*SIZE_PER_FRAME
	if len(data) != expectedSize {
		return nil, errors.New(ErrCorrupted)
	}

	// Verify skippable header
	if binary.LittleEndian.Uint32(data[0:4]) != SKIPPABLE_MAGIC_NUMBER {
		return nil, errors.New(ErrInvalidMagic)
	}

	// Parse entries
	st := NewSeekTable()
	dataStart := SKIPPABLE_HEADER_SIZE

	// Check if integrity is at the beginning (Head format)
	if len(data) > SKIPPABLE_HEADER_SIZE+SEEK_TABLE_FOOTER_SIZE {
		possibleIntegrity := data[SKIPPABLE_HEADER_SIZE : SKIPPABLE_HEADER_SIZE+SEEK_TABLE_FOOTER_SIZE]
		if binary.LittleEndian.Uint32(possibleIntegrity[5:9]) == SEEKABLE_MAGIC_NUMBER {
			dataStart += SEEK_TABLE_FOOTER_SIZE
		}
	}

	for i := 0; i < int(numFrames); i++ {
		offset := dataStart + i*SIZE_PER_FRAME
		compSize := binary.LittleEndian.Uint32(data[offset : offset+4])
		decompSize := binary.LittleEndian.Uint32(data[offset+4 : offset+8])

		if err := st.LogFrame(compSize, decompSize); err != nil {
			return nil, err
		}
	}

	return st, nil
}

// ReadSeekTableFooter reads the seek table footer from a reader
func ReadSeekTableFooter(r io.ReadSeeker) ([]byte, error) {
	footer := make([]byte, SEEK_TABLE_FOOTER_SIZE)
	if _, err := r.Seek(-SEEK_TABLE_FOOTER_SIZE, io.SeekEnd); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(r, footer); err != nil {
		return nil, err
	}
	return footer, nil
}

// ParseSeekTableSize parses the seek table size from integrity bytes
func ParseSeekTableSize(integrity []byte) (int, error) {
	if len(integrity) != SEEK_TABLE_FOOTER_SIZE {
		return 0, errors.New("invalid integrity size")
	}

	if binary.LittleEndian.Uint32(integrity[5:9]) != SEEKABLE_MAGIC_NUMBER {
		return 0, errors.New(ErrInvalidMagic)
	}

	numFrames := binary.LittleEndian.Uint32(integrity[0:4])
	if numFrames > SEEKABLE_MAX_FRAMES {
		return 0, errors.New(ErrFrameIndexTooLarge)
	}

	return SKIPPABLE_HEADER_SIZE + SEEK_TABLE_FOOTER_SIZE + int(numFrames)*SIZE_PER_FRAME, nil
}
