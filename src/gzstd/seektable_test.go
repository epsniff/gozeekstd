package gzstd

import (
	"encoding/binary"
	"testing"
)

func TestNewSeekTable(t *testing.T) {
	st := NewSeekTable()
	if st == nil {
		t.Fatal("NewSeekTable returned nil")
	}
	if len(st.entries) != 1 {
		t.Errorf("Expected 1 entry, got %d", len(st.entries))
	}
	if st.entries[0].CompressedOffset != 0 || st.entries[0].DecompressedOffset != 0 {
		t.Error("Initial entry should have zero offsets")
	}
}

func TestSeekTable_LogFrame(t *testing.T) {
	st := NewSeekTable()
	
	// Log first frame
	err := st.LogFrame(1000, 2000)
	if err != nil {
		t.Fatalf("LogFrame failed: %v", err)
	}
	
	if st.NumFrames() != 1 {
		t.Errorf("Expected 1 frame, got %d", st.NumFrames())
	}
	
	// Log second frame
	err = st.LogFrame(1500, 3000)
	if err != nil {
		t.Fatalf("LogFrame failed: %v", err)
	}
	
	if st.NumFrames() != 2 {
		t.Errorf("Expected 2 frames, got %d", st.NumFrames())
	}
	
	// Verify offsets
	if st.entries[1].CompressedOffset != 1000 {
		t.Errorf("Expected compressed offset 1000, got %d", st.entries[1].CompressedOffset)
	}
	if st.entries[2].CompressedOffset != 2500 {
		t.Errorf("Expected compressed offset 2500, got %d", st.entries[2].CompressedOffset)
	}
}

func TestSeekTable_FrameQueries(t *testing.T) {
	st := NewSeekTable()
	st.LogFrame(1000, 2000)
	st.LogFrame(1500, 3000)
	st.LogFrame(2000, 4000)
	
	tests := []struct {
		name     string
		fn       func(uint32) (uint64, error)
		frame    uint32
		expected uint64
		wantErr  bool
	}{
		{"FrameStartComp(0)", st.FrameStartComp, 0, 0, false},
		{"FrameStartComp(1)", st.FrameStartComp, 1, 1000, false},
		{"FrameStartComp(2)", st.FrameStartComp, 2, 2500, false},
		{"FrameStartComp(10)", st.FrameStartComp, 10, 0, true},
		
		{"FrameStartDecomp(0)", st.FrameStartDecomp, 0, 0, false},
		{"FrameStartDecomp(1)", st.FrameStartDecomp, 1, 2000, false},
		{"FrameStartDecomp(2)", st.FrameStartDecomp, 2, 5000, false},
		
		{"FrameEndComp(0)", st.FrameEndComp, 0, 1000, false},
		{"FrameEndComp(1)", st.FrameEndComp, 1, 2500, false},
		{"FrameEndComp(2)", st.FrameEndComp, 2, 4500, false},
		
		{"FrameEndDecomp(0)", st.FrameEndDecomp, 0, 2000, false},
		{"FrameEndDecomp(1)", st.FrameEndDecomp, 1, 5000, false},
		{"FrameEndDecomp(2)", st.FrameEndDecomp, 2, 9000, false},
		
		{"FrameSizeComp(0)", st.FrameSizeComp, 0, 1000, false},
		{"FrameSizeComp(1)", st.FrameSizeComp, 1, 1500, false},
		{"FrameSizeComp(2)", st.FrameSizeComp, 2, 2000, false},
		
		{"FrameSizeDecomp(0)", st.FrameSizeDecomp, 0, 2000, false},
		{"FrameSizeDecomp(1)", st.FrameSizeDecomp, 1, 3000, false},
		{"FrameSizeDecomp(2)", st.FrameSizeDecomp, 2, 4000, false},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.fn(tt.frame)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.expected {
				t.Errorf("got %d, want %d", got, tt.expected)
			}
		})
	}
}

func TestSeekTable_MaxFrameSizeDecomp(t *testing.T) {
	st := NewSeekTable()
	st.LogFrame(1000, 2000)
	st.LogFrame(1500, 5000) // Largest
	st.LogFrame(2000, 3000)
	
	maxSize := st.MaxFrameSizeDecomp()
	if maxSize != 5000 {
		t.Errorf("Expected max size 5000, got %d", maxSize)
	}
}

func TestSeekTable_Serialization(t *testing.T) {
	st := NewSeekTable()
	st.LogFrame(1000, 2000)
	st.LogFrame(1500, 3000)
	
	// Test serialization with Foot format
	serializer := st.NewSerializer(FormatFoot)
	
	expectedLen := SKIPPABLE_HEADER_SIZE + SEEK_TABLE_FOOTER_SIZE + 2*SIZE_PER_FRAME
	if serializer.EncodedLen() != expectedLen {
		t.Errorf("Expected encoded length %d, got %d", expectedLen, serializer.EncodedLen())
	}
	
	// Serialize
	buf := make([]byte, serializer.EncodedLen())
	totalWritten := 0
	for {
		n := serializer.WriteTo(buf[totalWritten:])
		if n == 0 {
			break
		}
		totalWritten += n
	}
	
	if totalWritten != expectedLen {
		t.Errorf("Expected to write %d bytes, wrote %d", expectedLen, totalWritten)
	}
	
	// Verify magic numbers
	if binary.LittleEndian.Uint32(buf[0:4]) != SKIPPABLE_MAGIC_NUMBER {
		t.Error("Invalid skippable magic number")
	}
	
	footerStart := len(buf) - SEEK_TABLE_FOOTER_SIZE
	if binary.LittleEndian.Uint32(buf[footerStart+5:footerStart+9]) != SEEKABLE_MAGIC_NUMBER {
		t.Error("Invalid seekable magic number")
	}
}

func TestParseSeekTable(t *testing.T) {
	// Create a valid seek table
	st := NewSeekTable()
	st.LogFrame(1000, 2000)
	st.LogFrame(1500, 3000)
	
	// Serialize it
	serializer := st.NewSerializer(FormatFoot)
	buf := make([]byte, serializer.EncodedLen())
	totalWritten := 0
	for {
		n := serializer.WriteTo(buf[totalWritten:])
		if n == 0 {
			break
		}
		totalWritten += n
	}
	
	// Parse it back
	parsed, err := ParseSeekTable(buf)
	if err != nil {
		t.Fatalf("ParseSeekTable failed: %v", err)
	}
	
	if parsed.NumFrames() != st.NumFrames() {
		t.Errorf("Expected %d frames, got %d", st.NumFrames(), parsed.NumFrames())
	}
	
	// Verify frame data
	for i := uint32(0); i < st.NumFrames(); i++ {
		origComp, _ := st.FrameSizeComp(i)
		parsedComp, _ := parsed.FrameSizeComp(i)
		if origComp != parsedComp {
			t.Errorf("Frame %d: compressed size mismatch %d vs %d", i, origComp, parsedComp)
		}
		
		origDecomp, _ := st.FrameSizeDecomp(i)
		parsedDecomp, _ := parsed.FrameSizeDecomp(i)
		if origDecomp != parsedDecomp {
			t.Errorf("Frame %d: decompressed size mismatch %d vs %d", i, origDecomp, parsedDecomp)
		}
	}
}

func TestParseSeekTable_Errors(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr string
	}{
		{
			name:    "too small",
			data:    make([]byte, 5),
			wantErr: ErrCorrupted,
		},
		{
			name:    "invalid magic",
			data:    make([]byte, SEEK_TABLE_FOOTER_SIZE),
			wantErr: ErrInvalidMagic,
		},
		{
			name: "frame count too large",
			data: func() []byte {
				data := make([]byte, SEEK_TABLE_FOOTER_SIZE)
				binary.LittleEndian.PutUint32(data[0:4], SEEKABLE_MAX_FRAMES+1)
				binary.LittleEndian.PutUint32(data[5:9], SEEKABLE_MAGIC_NUMBER)
				return data
			}(),
			wantErr: ErrFrameIndexTooLarge,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseSeekTable(tt.data)
			if err == nil {
				t.Fatal("Expected error, got nil")
			}
			if err.Error() != tt.wantErr {
				t.Errorf("Expected error %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

func TestParseSeekTableSize(t *testing.T) {
	integrity := make([]byte, SEEK_TABLE_FOOTER_SIZE)
	binary.LittleEndian.PutUint32(integrity[0:4], 10) // 10 frames
	binary.LittleEndian.PutUint32(integrity[5:9], SEEKABLE_MAGIC_NUMBER)
	
	size, err := ParseSeekTableSize(integrity)
	if err != nil {
		t.Fatalf("ParseSeekTableSize failed: %v", err)
	}
	
	expectedSize := SKIPPABLE_HEADER_SIZE + SEEK_TABLE_FOOTER_SIZE + 10*SIZE_PER_FRAME
	if size != expectedSize {
		t.Errorf("Expected size %d, got %d", expectedSize, size)
	}
}
