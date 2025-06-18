package gzstd

import (
	"bytes"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestNewEncoder(t *testing.T) {
	var buf bytes.Buffer
	encoder, err := NewEncoder(&buf, nil)
	if err != nil {
		t.Fatalf("NewEncoder failed: %v", err)
	}
	if encoder == nil {
		t.Fatal("NewEncoder returned nil")
	}
}

func TestEncoder_Write(t *testing.T) {
	var buf bytes.Buffer
	encoder, err := NewEncoder(&buf, nil)
	if err != nil {
		t.Fatalf("NewEncoder failed: %v", err)
	}
	
	// Write some data
	data := []byte("Hello, World!")
	n, err := encoder.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("Expected to write %d bytes, wrote %d", len(data), n)
	}
	
	// Finish encoding
	if err := encoder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}
	
	// Verify we got some output
	if buf.Len() == 0 {
		t.Error("No data written to buffer")
	}
	
	// Verify seek table has frames
	if encoder.SeekTable().NumFrames() == 0 {
		t.Error("No frames in seek table")
	}
}

func TestEncoder_MultipleFrames(t *testing.T) {
	var buf bytes.Buffer
	opts := &EncoderOptions{
		Level:        zstd.SpeedDefault,
		FramePolicy:  UncompressedFrameSize{Size: 100}, // Small frames
		ChecksumFlag: true,
	}
	
	encoder, err := NewEncoder(&buf, opts)
	if err != nil {
		t.Fatalf("NewEncoder failed: %v", err)
	}
	
	// Write data that will span multiple frames
	data := make([]byte, 300)
	for i := range data {
		data[i] = byte(i % 256)
	}
	
	n, err := encoder.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("Expected to write %d bytes, wrote %d", len(data), n)
	}
	
	// Finish encoding
	if err := encoder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}
	
	// Should have 3 frames (300 bytes / 100 bytes per frame)
	if encoder.SeekTable().NumFrames() != 3 {
		t.Errorf("Expected 3 frames, got %d", encoder.SeekTable().NumFrames())
	}
}

func TestEncoder_CompressedFrameSize(t *testing.T) {
	var buf bytes.Buffer
	opts := &EncoderOptions{
		Level:        zstd.SpeedDefault,
		FramePolicy:  CompressedFrameSize{Size: 1000},
		ChecksumFlag: true,
	}
	
	encoder, err := NewEncoder(&buf, opts)
	if err != nil {
		t.Fatalf("NewEncoder failed: %v", err)
	}
	
	// Write compressible data
	data := make([]byte, 10000)
	// Fill with repetitive data that compresses well
	for i := range data {
		data[i] = byte(i % 10)
	}
	
	n, err := encoder.Write(data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("Expected to write %d bytes, wrote %d", len(data), n)
	}
	
	if err := encoder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}
	
	// Should have multiple frames based on compressed size
	if encoder.SeekTable().NumFrames() == 0 {
		t.Error("No frames created")
	}
}

func TestEncoder_WriteWithPrefix(t *testing.T) {
	var buf bytes.Buffer
	encoder, err := NewEncoder(&buf, nil)
	if err != nil {
		t.Fatalf("NewEncoder failed: %v", err)
	}
	
	prefix := []byte("PREFIX")
	data := []byte("Hello, World!")
	
	n, err := encoder.WriteWithPrefix(data, prefix)
	if err != nil {
		t.Fatalf("WriteWithPrefix failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("Expected to write %d bytes, wrote %d", len(data), n)
	}
	
	if err := encoder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}
	
	if buf.Len() == 0 {
		t.Error("No data written to buffer")
	}
}

func TestEncoder_EndFrame(t *testing.T) {
	var buf bytes.Buffer
	encoder, err := NewEncoder(&buf, nil)
	if err != nil {
		t.Fatalf("NewEncoder failed: %v", err)
	}
	
	// Write some data
	encoder.Write([]byte("Frame 1"))
	
	// Manually end frame
	if err := encoder.EndFrame(); err != nil {
		t.Fatalf("EndFrame failed: %v", err)
	}
	
	// Write more data
	encoder.Write([]byte("Frame 2"))
	
	if err := encoder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}
	
	// Should have 2 frames
	if encoder.SeekTable().NumFrames() != 2 {
		t.Errorf("Expected 2 frames, got %d", encoder.SeekTable().NumFrames())
	}
}

func TestEncoder_FinishWithFormat(t *testing.T) {
	tests := []struct {
		name   string
		format Format
	}{
		{"Foot format", FormatFoot},
		{"Head format", FormatHead},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			encoder, err := NewEncoder(&buf, nil)
			if err != nil {
				t.Fatalf("NewEncoder failed: %v", err)
			}
			
			encoder.Write([]byte("Test data"))
			
			if err := encoder.FinishWithFormat(tt.format); err != nil {
				t.Fatalf("FinishWithFormat failed: %v", err)
			}
			
			if buf.Len() == 0 {
				t.Error("No data written")
			}
		})
	}
}

func TestFrameSizePolicy(t *testing.T) {
	// Test CompressedFrameSize
	cfs := CompressedFrameSize{Size: 1024}
	if cfs.MaxSize() != 1024 {
		t.Errorf("Expected max size 1024, got %d", cfs.MaxSize())
	}
	
	// Test UncompressedFrameSize
	ufs := UncompressedFrameSize{Size: 2048}
	if ufs.MaxSize() != 2048 {
		t.Errorf("Expected max size 2048, got %d", ufs.MaxSize())
	}
}
