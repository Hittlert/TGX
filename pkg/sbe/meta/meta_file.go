package meta

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/bits-and-blooms/bitset"
)

// MetaFile represents an active sidecar metadata file on disk.
type MetaFile struct {
	path        string
	file        *os.File
	header      *MetaHeader
	totalBlocks uint32
	bitmapBytes uint32

	slotSize    int64
	slotAOffset int64
	slotBOffset int64

	currentGen  uint64
	nextSlotIsB bool

	mu sync.Mutex
}

// MetaRecoverResult represents the loaded state after opening or recovering a .meta file.
type MetaRecoverResult struct {
	Header        *MetaHeader
	LatestGen     uint64
	IsComplete    bool
	DurableBitmap *bitset.BitSet
	ValidSlot     string // "A", "B", or "NONE"
}

// CreateOrOpenMetaFile creates a new preallocated .meta file or recovers an existing one.
func CreateOrOpenMetaFile(dir, fileName string, header *MetaHeader) (*MetaFile, *MetaRecoverResult, error) {
	if header.TotalBlocks == 0 {
		return nil, nil, fmt.Errorf("invalid total blocks: 0")
	}

	attemptHex := fmt.Sprintf("%x", header.AttemptID)
	metaName := fmt.Sprintf("%s.meta.%s", fileName, attemptHex)
	metaPath := filepath.Join(dir, metaName)

	totalSize := TotalMetaFileSize(header.TotalBlocks)
	bitmapBytes := uint32((header.TotalBlocks + 7) / 8)
	if bitmapBytes == 0 {
		bitmapBytes = 1
	}

	f, err := os.OpenFile(metaPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open meta file %s: %w", metaPath, err)
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}

	mf := &MetaFile{
		path:        metaPath,
		file:        f,
		header:      header,
		totalBlocks: header.TotalBlocks,
		bitmapBytes: bitmapBytes,
		slotSize:    SlotSize(header.TotalBlocks),
		slotAOffset: SlotAOffset(),
		slotBOffset: SlotBOffset(header.TotalBlocks),
	}

	// 1. If new or smaller than totalSize, preallocate and initialize
	if fi.Size() < totalSize {
		if err := f.Truncate(totalSize); err != nil {
			f.Close()
			return nil, nil, fmt.Errorf("failed to preallocate meta file: %w", err)
		}

		// Write Header
		headerBytes := header.Encode()
		if _, err := f.WriteAt(headerBytes, 0); err != nil {
			f.Close()
			return nil, nil, fmt.Errorf("failed to write meta header: %w", err)
		}

		if err := f.Sync(); err != nil {
			f.Close()
			return nil, nil, fmt.Errorf("failed to sync meta file: %w", err)
		}

		res := &MetaRecoverResult{
			Header:        header,
			LatestGen:     0,
			IsComplete:    false,
			DurableBitmap: bitset.New(uint(header.TotalBlocks)),
			ValidSlot:     "NONE",
		}
		return mf, res, nil
	}

	// 2. Existing file: Read and verify Header
	hBuf := make([]byte, MetaHeaderSize)
	if _, err := f.ReadAt(hBuf, 0); err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("failed to read existing meta header: %w", err)
	}

	decodedH, err := DecodeMetaHeader(hBuf)
	if err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("corrupted meta header in %s: %w", metaPath, err)
	}

	// Verify AttemptID and FileKeyHash match
	if !bytes.Equal(decodedH.AttemptID[:], header.AttemptID[:]) {
		f.Close()
		return nil, nil, ErrInvalidAttempt
	}
	if !bytes.Equal(decodedH.FileKeyHash[:], header.FileKeyHash[:]) {
		f.Close()
		return nil, nil, ErrInvalidFileKey
	}

	mf.header = decodedH

	// 3. Read Slot A and Slot B
	slotBuf := make([]byte, mf.slotSize)

	var (
		slotAHeader *SlotHeader
		slotABitmap []byte
		slotAErr    error

		slotBHeader *SlotHeader
		slotBBitmap []byte
		slotBErr    error
	)

	if _, err := f.ReadAt(slotBuf, mf.slotAOffset); err == nil {
		slotAHeader, slotABitmap, slotAErr = DecodeSlot(slotBuf, bitmapBytes)
	} else {
		slotAErr = err
	}

	if _, err := f.ReadAt(slotBuf, mf.slotBOffset); err == nil {
		slotBHeader, slotBBitmap, slotBErr = DecodeSlot(slotBuf, bitmapBytes)
	} else {
		slotBErr = err
	}

	res := &MetaRecoverResult{
		Header:        decodedH,
		DurableBitmap: bitset.New(uint(decodedH.TotalBlocks)),
		ValidSlot:     "NONE",
	}

	// Choose latest valid slot
	hasA := (slotAErr == nil && slotAHeader != nil && (slotAHeader.Flags == SlotFlagValid || slotAHeader.Flags == SlotFlagComplete))
	hasB := (slotBErr == nil && slotBHeader != nil && (slotBHeader.Flags == SlotFlagValid || slotBHeader.Flags == SlotFlagComplete))

	if hasA && hasB {
		if slotAHeader.Generation >= slotBHeader.Generation {
			mf.currentGen = slotAHeader.Generation
			mf.nextSlotIsB = true
			res.LatestGen = slotAHeader.Generation
			res.IsComplete = (slotAHeader.Flags == SlotFlagComplete)
			res.DurableBitmap = bytesToBitSet(slotABitmap, decodedH.TotalBlocks)
			res.ValidSlot = "A"
		} else {
			mf.currentGen = slotBHeader.Generation
			mf.nextSlotIsB = false
			res.LatestGen = slotBHeader.Generation
			res.IsComplete = (slotBHeader.Flags == SlotFlagComplete)
			res.DurableBitmap = bytesToBitSet(slotBBitmap, decodedH.TotalBlocks)
			res.ValidSlot = "B"
		}
	} else if hasA {
		mf.currentGen = slotAHeader.Generation
		mf.nextSlotIsB = true
		res.LatestGen = slotAHeader.Generation
		res.IsComplete = (slotAHeader.Flags == SlotFlagComplete)
		res.DurableBitmap = bytesToBitSet(slotABitmap, decodedH.TotalBlocks)
		res.ValidSlot = "A"
	} else if hasB {
		mf.currentGen = slotBHeader.Generation
		mf.nextSlotIsB = false
		res.LatestGen = slotBHeader.Generation
		res.IsComplete = (slotBHeader.Flags == SlotFlagComplete)
		res.DurableBitmap = bytesToBitSet(slotBBitmap, decodedH.TotalBlocks)
		res.ValidSlot = "B"
	} else {
		// Both slots invalid / torn -> fresh start
		mf.currentGen = 0
		mf.nextSlotIsB = false
		res.ValidSlot = "NONE"
	}

	return mf, res, nil
}

// WriteSlot writes a new generation checkpoint to the alternating slot and fsyncs the meta file.
func (m *MetaFile) WriteSlot(bs *bitset.BitSet) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.currentGen++
	rawBytes := bitSetToBytes(bs, m.bitmapBytes)
	encoded := EncodeSlot(m.currentGen, SlotFlagValid, rawBytes)

	offset := m.slotAOffset
	if m.nextSlotIsB {
		offset = m.slotBOffset
	}

	if _, err := m.file.WriteAt(encoded, offset); err != nil {
		return 0, fmt.Errorf("failed to write slot at offset %d: %w", offset, err)
	}

	if err := m.file.Sync(); err != nil {
		return 0, fmt.Errorf("failed to sync meta file: %w", err)
	}

	m.nextSlotIsB = !m.nextSlotIsB
	return m.currentGen, nil
}

// WriteComplete writes a COMPLETE state slot and fsyncs the meta file.
func (m *MetaFile) WriteComplete(bs *bitset.BitSet) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.currentGen++
	rawBytes := bitSetToBytes(bs, m.bitmapBytes)
	encoded := EncodeSlot(m.currentGen, SlotFlagComplete, rawBytes)

	offset := m.slotAOffset
	if m.nextSlotIsB {
		offset = m.slotBOffset
	}

	if _, err := m.file.WriteAt(encoded, offset); err != nil {
		return fmt.Errorf("failed to write complete slot: %w", err)
	}

	if err := m.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync complete meta file: %w", err)
	}

	m.nextSlotIsB = !m.nextSlotIsB
	return nil
}

// Close syncs and closes the meta file descriptor.
func (m *MetaFile) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.file == nil {
		return nil
	}
	_ = m.file.Sync()
	err := m.file.Close()
	m.file = nil
	return err
}

// Path returns the absolute file path of this meta file.
func (m *MetaFile) Path() string {
	return m.path
}

func bitSetToBytes(bs *bitset.BitSet, totalBytes uint32) []byte {
	out := make([]byte, totalBytes)
	if bs == nil {
		return out
	}
	for i := uint(0); i < bs.Len(); i++ {
		if bs.Test(i) {
			byteIdx := i / 8
			bitIdx := i % 8
			if byteIdx < uint(totalBytes) {
				out[byteIdx] |= (1 << bitIdx)
			}
		}
	}
	return out
}

func bytesToBitSet(data []byte, totalBlocks uint32) *bitset.BitSet {
	bs := bitset.New(uint(totalBlocks))
	for i := uint(0); i < uint(totalBlocks); i++ {
		byteIdx := i / 8
		bitIdx := i % 8
		if byteIdx < uint(len(data)) {
			if (data[byteIdx] & (1 << bitIdx)) != 0 {
				bs.Set(i)
			}
		}
	}
	return bs
}
