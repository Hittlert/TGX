package meta

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	MetaMagic        uint32 = 0x53424D31 // "SBM1" in ASCII BigEndian
	MetaVersion      uint32 = 1
	MetaHeaderSize   int64  = 128
	SlotHeaderSize   int64  = 32
	StandardBlockSize uint32 = 2 * 1024 * 1024 // 2 MiB
)

// Slot Flags
const (
	SlotFlagInvalid  uint32 = 0
	SlotFlagValid    uint32 = 1
	SlotFlagComplete uint32 = 2
)

var (
	ErrInvalidMagic      = errors.New("invalid meta header magic number")
	ErrUnsupportedVersion = errors.New("unsupported meta header version")
	ErrHeaderCRCMismatch = errors.New("meta header CRC32 checksum mismatch")
	ErrSlotCRCMismatch   = errors.New("slot CRC32 checksum mismatch")
	ErrInvalidAttempt    = errors.New("attempt ID mismatch in meta file")
	ErrInvalidFileKey    = errors.New("file key hash mismatch in meta file")
)

// MetaHeader represents the fixed 128-byte header of a .meta sidecar file.
type MetaHeader struct {
	Magic             uint32
	Version           uint32
	FileKeyHash       [32]byte
	SourceFingerprint uint64
	AttemptID         [16]byte
	TotalSize         int64
	BlockSize         uint32
	TotalBlocks       uint32
	HeaderCRC         uint32
	Reserved          [44]byte
}

// SlotHeader represents the fixed 32-byte header of each state slot (Slot A / Slot B).
type SlotHeader struct {
	Generation        uint64
	Flags             uint32
	BitmapLengthBytes uint32
	SlotDataCRC       uint32
	Reserved          [12]byte
}

// SlotSize returns the total byte length of one state slot given total block count.
func SlotSize(totalBlocks uint32) int64 {
	bitmapBytes := int64((totalBlocks + 7) / 8)
	if bitmapBytes == 0 {
		bitmapBytes = 1
	}
	return SlotHeaderSize + bitmapBytes
}

// SlotAOffset returns the physical byte offset of Slot A (always 128).
func SlotAOffset() int64 {
	return MetaHeaderSize
}

// SlotBOffset returns the physical byte offset of Slot B.
func SlotBOffset(totalBlocks uint32) int64 {
	return MetaHeaderSize + SlotSize(totalBlocks)
}

// TotalMetaFileSize returns the total pre-allocated physical file size for a .meta file.
func TotalMetaFileSize(totalBlocks uint32) int64 {
	return MetaHeaderSize + 2*SlotSize(totalBlocks)
}

// ComputeFingerprint computes a normalized 64-bit fingerprint from source metadata.
func ComputeFingerprint(peerID int64, msgID int, mediaHash uint64) uint64 {
	h := sha256.New()
	var buf [24]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(peerID))
	binary.BigEndian.PutUint64(buf[8:16], uint64(msgID))
	binary.BigEndian.PutUint64(buf[16:24], mediaHash)
	h.Write(buf[:])
	sum := h.Sum(nil)
	return binary.BigEndian.Uint64(sum[0:8])
}

// Encode serializes the MetaHeader into a 128-byte buffer with computed CRC32.
func (h *MetaHeader) Encode() []byte {
	buf := make([]byte, MetaHeaderSize)
	binary.BigEndian.PutUint32(buf[0:4], h.Magic)
	binary.BigEndian.PutUint32(buf[4:8], h.Version)
	copy(buf[8:40], h.FileKeyHash[:])
	binary.BigEndian.PutUint64(buf[40:48], h.SourceFingerprint)
	copy(buf[48:64], h.AttemptID[:])
	binary.BigEndian.PutUint64(buf[64:72], uint64(h.TotalSize))
	binary.BigEndian.PutUint32(buf[72:76], h.BlockSize)
	binary.BigEndian.PutUint32(buf[76:80], h.TotalBlocks)

	// Compute CRC of first 80 bytes [0..79]
	crc := crc32.ChecksumIEEE(buf[0:80])
	h.HeaderCRC = crc
	binary.BigEndian.PutUint32(buf[80:84], crc)
	copy(buf[84:128], h.Reserved[:])

	return buf
}

// DecodeMetaHeader deserializes and verifies a 128-byte buffer into a MetaHeader.
func DecodeMetaHeader(data []byte) (*MetaHeader, error) {
	if len(data) < int(MetaHeaderSize) {
		return nil, fmt.Errorf("buffer too short for meta header: %d < %d", len(data), MetaHeaderSize)
	}

	magic := binary.BigEndian.Uint32(data[0:4])
	if magic != MetaMagic {
		return nil, ErrInvalidMagic
	}
	ver := binary.BigEndian.Uint32(data[4:8])
	if ver != MetaVersion {
		return nil, ErrUnsupportedVersion
	}

	expectedCRC := binary.BigEndian.Uint32(data[80:84])
	actualCRC := crc32.ChecksumIEEE(data[0:80])
	if expectedCRC != actualCRC {
		return nil, ErrHeaderCRCMismatch
	}

	h := &MetaHeader{
		Magic:             magic,
		Version:           ver,
		SourceFingerprint: binary.BigEndian.Uint64(data[40:48]),
		TotalSize:         int64(binary.BigEndian.Uint64(data[64:72])),
		BlockSize:         binary.BigEndian.Uint32(data[72:76]),
		TotalBlocks:       binary.BigEndian.Uint32(data[76:80]),
		HeaderCRC:         expectedCRC,
	}
	copy(h.FileKeyHash[:], data[8:40])
	copy(h.AttemptID[:], data[48:64])
	copy(h.Reserved[:], data[84:128])

	return h, nil
}

// EncodeSlot encodes a SlotHeader and its bitmap bytes, calculating the combined CRC32.
func EncodeSlot(gen uint64, flags uint32, bitmapBytes []byte) []byte {
	slotLen := SlotHeaderSize + int64(len(bitmapBytes))
	buf := make([]byte, slotLen)

	binary.BigEndian.PutUint64(buf[0:8], gen)
	binary.BigEndian.PutUint32(buf[8:12], flags)
	binary.BigEndian.PutUint32(buf[12:16], uint32(len(bitmapBytes)))

	// Copy bitmap into payload area
	copy(buf[SlotHeaderSize:], bitmapBytes)

	// CRC includes first 16 bytes of header [0..15] + payload bytes [SlotHeaderSize..]
	h := crc32.NewIEEE()
	h.Write(buf[0:16])
	h.Write(bitmapBytes)
	slotCRC := h.Sum32()

	binary.BigEndian.PutUint32(buf[16:20], slotCRC)
	// Reserved [20..31] remain zero

	return buf
}

// DecodeSlot deserializes and verifies a SlotHeader and bitmap from raw bytes.
func DecodeSlot(data []byte, expectedBitmapBytes uint32) (*SlotHeader, []byte, error) {
	if len(data) < int(SlotHeaderSize)+int(expectedBitmapBytes) {
		return nil, nil, fmt.Errorf("slot data too short: %d < %d", len(data), int(SlotHeaderSize)+int(expectedBitmapBytes))
	}

	gen := binary.BigEndian.Uint64(data[0:8])
	flags := binary.BigEndian.Uint32(data[8:12])
	bLen := binary.BigEndian.Uint32(data[12:16])
	expectedCRC := binary.BigEndian.Uint32(data[16:20])

	if bLen != expectedBitmapBytes {
		return nil, nil, fmt.Errorf("bitmap length mismatch: %d != %d", bLen, expectedBitmapBytes)
	}

	payload := data[SlotHeaderSize : SlotHeaderSize+int64(bLen)]

	// Verify CRC
	h := crc32.NewIEEE()
	h.Write(data[0:16])
	h.Write(payload)
	actualCRC := h.Sum32()

	if expectedCRC != actualCRC {
		return nil, nil, ErrSlotCRCMismatch
	}

	sh := &SlotHeader{
		Generation:        gen,
		Flags:             flags,
		BitmapLengthBytes: bLen,
		SlotDataCRC:       expectedCRC,
	}

	bitmapCopy := make([]byte, bLen)
	copy(bitmapCopy, payload)

	return sh, bitmapCopy, nil
}
