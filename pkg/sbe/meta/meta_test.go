package meta

import (
	"crypto/sha256"
	"os"
	"testing"

	"github.com/bits-and-blooms/bitset"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sampleHeader(totalBlocks uint32) *MetaHeader {
	h := &MetaHeader{
		Magic:             MetaMagic,
		Version:           MetaVersion,
		FileKeyHash:       sha256.Sum256([]byte("sample_file_key")),
		SourceFingerprint: ComputeFingerprint(100123456, 42, 999888),
		TotalSize:         int64(totalBlocks) * int64(StandardBlockSize),
		BlockSize:         StandardBlockSize,
		TotalBlocks:       totalBlocks,
	}
	copy(h.AttemptID[:], []byte("attempt_uuid_001"))
	return h
}

func TestMetaHeader_EncodeDecode(t *testing.T) {
	h := sampleHeader(100)
	encoded := h.Encode()
	assert.Equal(t, int(MetaHeaderSize), len(encoded))

	decoded, err := DecodeMetaHeader(encoded)
	require.NoError(t, err)
	assert.Equal(t, h.Magic, decoded.Magic)
	assert.Equal(t, h.Version, decoded.Version)
	assert.Equal(t, h.FileKeyHash, decoded.FileKeyHash)
	assert.Equal(t, h.SourceFingerprint, decoded.SourceFingerprint)
	assert.Equal(t, h.AttemptID, decoded.AttemptID)
	assert.Equal(t, h.TotalSize, decoded.TotalSize)
	assert.Equal(t, h.BlockSize, decoded.BlockSize)
	assert.Equal(t, h.TotalBlocks, decoded.TotalBlocks)
	assert.Equal(t, h.HeaderCRC, decoded.HeaderCRC)

	// Tamper CRC
	encoded[80] ^= 0xFF
	_, err = DecodeMetaHeader(encoded)
	assert.Equal(t, ErrHeaderCRCMismatch, err)
}

func TestSlot_EncodeDecode(t *testing.T) {
	bs := []byte{0b10101010, 0b11001100}
	encoded := EncodeSlot(42, SlotFlagValid, bs)
	assert.Equal(t, int(SlotHeaderSize)+len(bs), len(encoded))

	sh, decodedBytes, err := DecodeSlot(encoded, uint32(len(bs)))
	require.NoError(t, err)
	assert.Equal(t, uint64(42), sh.Generation)
	assert.Equal(t, SlotFlagValid, sh.Flags)
	assert.Equal(t, bs, decodedBytes)

	// Tamper payload byte
	encoded[SlotHeaderSize] ^= 0xFF
	_, _, err = DecodeSlot(encoded, uint32(len(bs)))
	assert.Equal(t, ErrSlotCRCMismatch, err)
}

func TestMetaFile_PreallocationAndSlotRotation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sbe_meta_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	header := sampleHeader(16) // 16 blocks = 2 bytes bitmap
	expectedSize := TotalMetaFileSize(16)

	// 1. Create fresh meta file
	mf, rec, err := CreateOrOpenMetaFile(tmpDir, "video.mp4", header)
	require.NoError(t, err)
	require.NotNil(t, mf)
	assert.Equal(t, "NONE", rec.ValidSlot)
	assert.False(t, rec.IsComplete)

	fi, err := os.Stat(mf.Path())
	require.NoError(t, err)
	assert.Equal(t, expectedSize, fi.Size())

	// 2. Write Slot 1 (Slot A)
	bs1 := bitset.New(16)
	bs1.Set(0)
	bs1.Set(3)
	gen1, err := mf.WriteSlot(bs1)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), gen1)

	// 3. Write Slot 2 (Slot B)
	bs2 := bs1.Clone()
	bs2.Set(7)
	gen2, err := mf.WriteSlot(bs2)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), gen2)

	require.NoError(t, mf.Close())

	// 4. Re-open and verify latest state recovered from Slot B
	mf2, rec2, err := CreateOrOpenMetaFile(tmpDir, "video.mp4", header)
	require.NoError(t, err)
	defer mf2.Close()

	assert.Equal(t, "B", rec2.ValidSlot)
	assert.Equal(t, uint64(2), rec2.LatestGen)
	assert.True(t, rec2.DurableBitmap.Test(0))
	assert.True(t, rec2.DurableBitmap.Test(3))
	assert.True(t, rec2.DurableBitmap.Test(7))
	assert.False(t, rec2.DurableBitmap.Test(1))
}

func TestMetaFile_CrashRecoveryAndSlotFailover(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sbe_meta_failover_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	header := sampleHeader(16)
	mf, _, err := CreateOrOpenMetaFile(tmpDir, "video.mp4", header)
	require.NoError(t, err)

	// Write Gen 1 to Slot A
	bsA := bitset.New(16)
	bsA.Set(1)
	_, err = mf.WriteSlot(bsA)
	require.NoError(t, err)

	// Write Gen 2 to Slot B
	bsB := bsA.Clone()
	bsB.Set(2)
	_, err = mf.WriteSlot(bsB)
	require.NoError(t, err)

	require.NoError(t, mf.Close())

	// Corrupt Slot B
	data, err := os.ReadFile(mf.Path())
	require.NoError(t, err)
	slotBOffset := SlotBOffset(16)
	data[slotBOffset+4] ^= 0xFF // Corrupt Slot B header
	err = os.WriteFile(mf.Path(), data, 0644)
	require.NoError(t, err)

	// Reopen -> Should failover to Slot A
	mfRecovered, rec, err := CreateOrOpenMetaFile(tmpDir, "video.mp4", header)
	require.NoError(t, err)
	defer mfRecovered.Close()

	assert.Equal(t, "A", rec.ValidSlot)
	assert.Equal(t, uint64(1), rec.LatestGen)
	assert.True(t, rec.DurableBitmap.Test(1))
	assert.False(t, rec.DurableBitmap.Test(2))
}

func TestMetaFile_WriteComplete(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sbe_meta_complete_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	header := sampleHeader(8)
	mf, _, err := CreateOrOpenMetaFile(tmpDir, "video.mp4", header)
	require.NoError(t, err)

	bs := bitset.New(8)
	for i := uint(0); i < 8; i++ {
		bs.Set(i)
	}

	err = mf.WriteComplete(bs)
	require.NoError(t, err)
	require.NoError(t, mf.Close())

	// Reopen
	_, rec, err := CreateOrOpenMetaFile(tmpDir, "video.mp4", header)
	require.NoError(t, err)
	assert.True(t, rec.IsComplete)
	assert.Equal(t, uint(8), rec.DurableBitmap.Count())
}
