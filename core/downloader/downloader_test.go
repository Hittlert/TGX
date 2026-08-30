package downloader

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Hittlert/TGX/pkg/sbe/gate"
)

type fakeFile struct {
	size int64
	dc   int
}

func (f *fakeFile) Location() tg.InputFileLocationClass { return &tg.InputPeerPhotoFileLocation{} }
func (f *fakeFile) Size() int64                         { return f.size }
func (f *fakeFile) DC() int                             { return f.dc }

type fakeElem struct {
	file     *fakeFile
	buf      io.WriterAt
	take     bool
	cancMu   sync.Mutex
	canceled bool
	ctx      context.Context
	onDone   func(error)
}

func (e *fakeElem) File() File      { return e.file }
func (e *fakeElem) To() io.WriterAt { return e.buf }
func (e *fakeElem) AsTakeout() bool { return e.take }
func (e *fakeElem) IsCanceled() bool {
	e.cancMu.Lock()
	defer e.cancMu.Unlock()
	return e.canceled
}
func (e *fakeElem) Cancel() {
	e.cancMu.Lock()
	defer e.cancMu.Unlock()
	e.canceled = true
}
func (e *fakeElem) Context() context.Context {
	if e.ctx != nil {
		return e.ctx
	}
	return context.Background()
}

type memWriterAt struct {
	mu   sync.Mutex
	data []byte
}

func newMemWriterAt(size int64) *memWriterAt {
	return &memWriterAt{data: make([]byte, size)}
}

func (m *memWriterAt) WriteAt(p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if off+int64(len(p)) > int64(len(m.data)) {
		return 0, errors.New("write out of bounds")
	}
	copy(m.data[off:], p)
	return len(p), nil
}

type fakeIter struct {
	mu    sync.Mutex
	elems []*fakeElem
	curr  int
}

func (i *fakeIter) Next(ctx context.Context) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.curr < len(i.elems) {
		i.curr++
		return true
	}
	return false
}

func (i *fakeIter) Value() Elem {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.curr > 0 && i.curr <= len(i.elems) {
		return i.elems[i.curr-1]
	}
	return nil
}

func (i *fakeIter) Err() error { return nil }

type fakeProgress struct {
	mu         sync.Mutex
	added      []Elem
	downloaded map[Elem]int64
	done       map[Elem]error
}

func newFakeProgress() *fakeProgress {
	return &fakeProgress{
		downloaded: make(map[Elem]int64),
		done:       make(map[Elem]error),
	}
}

func (p *fakeProgress) OnAdd(elem Elem) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.added = append(p.added, elem)
}

func (p *fakeProgress) OnDownload(elem Elem, state ProgressState) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.downloaded[elem] = state.Downloaded
}

func (p *fakeProgress) OnDone(elem Elem, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.done[elem] = err
}

type fakeInvoker struct {
	mu         sync.Mutex
	requests   []int64 // offsets
	failOffset int64
	failCount  int
}

func (f *fakeInvoker) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	req, ok := input.(*tg.UploadGetFileRequest)
	if !ok {
		return errors.New("unexpected input type")
	}

	f.mu.Lock()
	f.requests = append(f.requests, req.Offset)
	fail := f.failOffset > 0 && req.Offset == f.failOffset && f.failCount < 5
	if fail {
		f.failCount++
	}
	f.mu.Unlock()

	if fail {
		return errors.New("simulated chunk network error")
	}

	data := bytes.Repeat([]byte{byte(req.Offset / 512)}, req.Limit)
	res := &tg.UploadFile{
		Type:  &tg.StorageFilePartial{},
		Mtime: int(time.Now().Unix()),
		Bytes: data,
	}

	out, ok := output.(*tg.UploadFileBox)
	if ok {
		out.File = res
	}
	return nil
}

type fakePool struct {
	invoker tg.Invoker
}

func (p *fakePool) Client(ctx context.Context, dc int) *tg.Client {
	return tg.NewClient(p.invoker)
}
func (p *fakePool) Takeout(ctx context.Context, dc int) *tg.Client {
	return tg.NewClient(p.invoker)
}
func (p *fakePool) Default(ctx context.Context) *tg.Client {
	return tg.NewClient(p.invoker)
}
func (p *fakePool) Close() error { return nil }

func TestDownloader_InterleavingAndDecoupledWrite(t *testing.T) {
	invoker := &fakeInvoker{}
	pool := &fakePool{invoker: invoker}

	// Create 5 files with 2MB size each (4 parts each = 20 parts total)
	elems := make([]*fakeElem, 5)
	for i := 0; i < 5; i++ {
		elems[i] = &fakeElem{
			file: &fakeFile{size: 2 * 1024 * 1024, dc: 4},
			buf:  newMemWriterAt(2 * 1024 * 1024),
		}
	}

	iter := &fakeIter{elems: elems}
	progress := newFakeProgress()
	fg := gate.NewFloodGate(1000, 100)

	dl := New(Options{
		Pool:            pool,
		Threads:         8,
		DiskWorkers:     4,
		FileConcurrency: 5,
		Iter:            iter,
		Progress:        progress,
		FloodGate:       fg,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := dl.Download(ctx, 8)
	require.NoError(t, err)

	// Verify all 5 files were completed
	progress.mu.Lock()
	defer progress.mu.Unlock()

	assert.Equal(t, 5, len(progress.added))
	assert.Equal(t, 5, len(progress.done))
	for _, e := range elems {
		assert.NoError(t, progress.done[e])
		assert.Equal(t, int64(2*1024*1024), progress.downloaded[e])
	}
}

type shortReadInvoker struct {
	mu    sync.Mutex
	tries int
}

func (s *shortReadInvoker) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	req, ok := input.(*tg.UploadGetFileRequest)
	if !ok {
		return errors.New("unexpected input type")
	}

	s.mu.Lock()
	s.tries++
	currentTry := s.tries
	s.mu.Unlock()

	var data []byte
	if currentTry == 1 {
		// First try returns truncated data (100 bytes instead of 512KB) -> triggers short read rejection
		data = bytes.Repeat([]byte("X"), 100)
	} else {
		// Second try returns full expected bytes
		data = bytes.Repeat([]byte("Y"), req.Limit)
	}

	res := &tg.UploadFile{
		Type:  &tg.StorageFilePartial{},
		Mtime: int(time.Now().Unix()),
		Bytes: data,
	}

	out, ok := output.(*tg.UploadFileBox)
	if ok {
		out.File = res
	}
	return nil
}

type shortReadPool struct {
	invoker *shortReadInvoker
}

func (p *shortReadPool) Client(ctx context.Context, dc int) *tg.Client {
	return tg.NewClient(p.invoker)
}
func (p *shortReadPool) Takeout(ctx context.Context, dc int) *tg.Client {
	return tg.NewClient(p.invoker)
}
func (p *shortReadPool) Default(ctx context.Context) *tg.Client {
	return tg.NewClient(p.invoker)
}
func (p *shortReadPool) Close() error { return nil }

func TestDownloader_ShortReadValidationAndAutoRetry(t *testing.T) {
	invoker := &shortReadInvoker{}
	pool := &shortReadPool{invoker: invoker}

	// 1 File with 2 parts (1MB)
	elem := &fakeElem{
		file: &fakeFile{size: 1024 * 1024, dc: 4},
		buf:  newMemWriterAt(1024 * 1024),
	}

	iter := &fakeIter{elems: []*fakeElem{elem}}
	progress := newFakeProgress()
	fg := gate.NewFloodGate(1000, 100)

	dl := New(Options{
		Pool:            pool,
		Threads:         2,
		DiskWorkers:     2,
		FileConcurrency: 1,
		Iter:            iter,
		Progress:        progress,
		FloodGate:       fg,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := dl.Download(ctx, 2)
	require.NoError(t, err)

	progress.mu.Lock()
	defer progress.mu.Unlock()

	assert.Equal(t, 1, len(progress.done))
	assert.NoError(t, progress.done[elem])
	// Should have retried part 0 and succeeded on second try
	assert.GreaterOrEqual(t, invoker.tries, 2)
}

func TestDownloader_CancellationZeroPanic(t *testing.T) {
	invoker := &fakeInvoker{}
	pool := &fakePool{invoker: invoker}

	// Create 10 files with 100MB each
	elems := make([]*fakeElem, 10)
	for i := 0; i < 10; i++ {
		elems[i] = &fakeElem{
			file: &fakeFile{size: 100 * 1024 * 1024, dc: 4},
			buf:  newMemWriterAt(100 * 1024 * 1024),
		}
	}

	iter := &fakeIter{elems: elems}
	progress := newFakeProgress()
	fg := gate.NewFloodGate(1000, 100)

	dl := New(Options{
		Pool:            pool,
		Threads:         16,
		DiskWorkers:     4,
		FileConcurrency: 5,
		Iter:            iter,
		Progress:        progress,
		FloodGate:       fg,
	})

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel immediately or quickly
	cancel()

	// Must cleanly return on context cancellation with ZERO panics or send on closed channel
	err := dl.Download(ctx, 16)
	assert.Error(t, err)
}

func TestDownloader_TaskLevelCancellation(t *testing.T) {
	invoker := &fakeInvoker{}
	pool := &fakePool{invoker: invoker}

	canceledElem := &fakeElem{
		file:     &fakeFile{size: 5 * 1024 * 1024, dc: 4},
		buf:      newMemWriterAt(5 * 1024 * 1024),
		canceled: true,
	}

	iter := &fakeIter{elems: []*fakeElem{canceledElem}}
	progress := newFakeProgress()
	fg := gate.NewFloodGate(1000, 100)

	dl := New(Options{
		Pool:            pool,
		Threads:         4,
		DiskWorkers:     2,
		FileConcurrency: 2,
		Iter:            iter,
		Progress:        progress,
		FloodGate:       fg,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := dl.Download(ctx, 4)
	require.NoError(t, err)

	progress.mu.Lock()
	defer progress.mu.Unlock()

	// Canceled task must fail fast with context.Canceled
	assert.Equal(t, 1, len(progress.done))
	assert.Equal(t, context.Canceled, progress.done[canceledElem])
}

func TestDownloader_DualLane_MixedSmallAndLargeFiles(t *testing.T) {
	invoker := &fakeInvoker{}
	pool := &fakePool{invoker: invoker}

	// 8 Large files (2MB each = 4 parts each) -> tests maxActiveLargeFiles=5 queueing!
	// 20 Small files (200KB each = 1 part each)
	elems := make([]*fakeElem, 0, 28)
	for i := 0; i < 8; i++ {
		elems = append(elems, &fakeElem{
			file: &fakeFile{size: 2 * 1024 * 1024, dc: 4},
			buf:  newMemWriterAt(2 * 1024 * 1024),
		})
	}
	for i := 0; i < 20; i++ {
		elems = append(elems, &fakeElem{
			file: &fakeFile{size: 200 * 1024, dc: 4},
			buf:  newMemWriterAt(200 * 1024),
		})
	}

	iter := &fakeIter{elems: elems}
	progress := newFakeProgress()
	fg := gate.NewFloodGate(1000, 100)

	dl := New(Options{
		Pool:            pool,
		Threads:         16,
		DiskWorkers:     6,
		FileConcurrency: 5,
		Iter:            iter,
		Progress:        progress,
		FloodGate:       fg,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := dl.Download(ctx, 16)
	require.NoError(t, err)

	progress.mu.Lock()
	defer progress.mu.Unlock()

	assert.Equal(t, 28, len(progress.added))
	assert.Equal(t, 28, len(progress.done))
	for _, e := range elems {
		assert.NoError(t, progress.done[e])
		assert.Equal(t, e.file.size, progress.downloaded[e])
	}
}

func TestDownloader_DualLane_OnlySmallFiles(t *testing.T) {
	invoker := &fakeInvoker{}
	pool := &fakePool{invoker: invoker}

	// 32 Small files (100KB each)
	elems := make([]*fakeElem, 32)
	for i := 0; i < 32; i++ {
		elems[i] = &fakeElem{
			file: &fakeFile{size: 100 * 1024, dc: 4},
			buf:  newMemWriterAt(100 * 1024),
		}
	}

	iter := &fakeIter{elems: elems}
	progress := newFakeProgress()
	fg := gate.NewFloodGate(1000, 100)

	dl := New(Options{
		Pool:            pool,
		Threads:         32,
		DiskWorkers:     6,
		FileConcurrency: 5,
		Iter:            iter,
		Progress:        progress,
		FloodGate:       fg,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := dl.Download(ctx, 32)
	require.NoError(t, err)

	progress.mu.Lock()
	defer progress.mu.Unlock()

	assert.Equal(t, 32, len(progress.added))
	assert.Equal(t, 32, len(progress.done))
	for _, e := range elems {
		assert.NoError(t, progress.done[e])
		assert.Equal(t, e.file.size, progress.downloaded[e])
	}
}

func TestDownloader_LargeFileChunkFailureConvergesAndReleasesWriter(t *testing.T) {
	invoker := &fakeInvoker{
		failOffset: 1024 * 1024, // Fail the 2nd chunk (1 MiB) of the first file
	}
	pool := &fakePool{invoker: invoker}

	// 2 Large files (1.5 MB = 3 chunks each)
	elems := []*fakeElem{
		{
			file: &fakeFile{size: 1536 * 1024, dc: 4},
			buf:  newMemWriterAt(1536 * 1024),
		},
		{
			file: &fakeFile{size: 1536 * 1024, dc: 4},
			buf:  newMemWriterAt(1536 * 1024),
		},
	}

	iter := &fakeIter{elems: elems}
	progress := newFakeProgress()
	fg := gate.NewFloodGate(1000, 100)

	dl := New(Options{
		Pool:            pool,
		Threads:         16,
		DiskWorkers:     6,
		FileConcurrency: 1, // Only 1 active large file at a time to strictly test slot release upon error!
		Iter:            iter,
		Progress:        progress,
		FloodGate:       fg,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := dl.Download(ctx, 16)
	require.NoError(t, err)

	progress.mu.Lock()
	defer progress.mu.Unlock()

	// File 1 must fail, File 2 must succeed (proving writer slot was released cleanly!)
	assert.Error(t, progress.done[elems[0]])
	assert.NoError(t, progress.done[elems[1]])
}

func TestDownloader_MassiveSmallAndLargeInterleavedNonBlocking(t *testing.T) {
	invoker := &fakeInvoker{}
	pool := &fakePool{invoker: invoker}

	// Interleave 50 small files (100KB) and 10 large files (2MB)
	elems := make([]*fakeElem, 0, 60)
	for i := 0; i < 10; i++ {
		// 1 Large
		elems = append(elems, &fakeElem{
			file: &fakeFile{size: 2 * 1024 * 1024, dc: 4},
			buf:  newMemWriterAt(2 * 1024 * 1024),
		})
		// 5 Small
		for s := 0; s < 5; s++ {
			elems = append(elems, &fakeElem{
				file: &fakeFile{size: 100 * 1024, dc: 4},
				buf:  newMemWriterAt(100 * 1024),
			})
		}
	}

	iter := &fakeIter{elems: elems}
	progress := newFakeProgress()
	fg := gate.NewFloodGate(1000, 100)

	dl := New(Options{
		Pool:            pool,
		Threads:         32,
		DiskWorkers:     6,
		FileConcurrency: 5,
		Iter:            iter,
		Progress:        progress,
		FloodGate:       fg,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := dl.Download(ctx, 32)
	require.NoError(t, err)

	progress.mu.Lock()
	defer progress.mu.Unlock()

	assert.Equal(t, 60, len(progress.added))
	assert.Equal(t, 60, len(progress.done))
	for _, e := range elems {
		assert.NoError(t, progress.done[e])
		assert.Equal(t, e.file.size, progress.downloaded[e])
	}
}

type delayedFakeInvoker struct {
	mu            sync.Mutex
	failOffset    int64
	failCount     int
	delayOffset   int64
	delayDuration time.Duration
}

func (f *delayedFakeInvoker) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	req, ok := input.(*tg.UploadGetFileRequest)
	if !ok {
		return errors.New("unexpected input type")
	}

	f.mu.Lock()
	fail := f.failOffset >= 0 && req.Offset == f.failOffset && f.failCount < 5
	if fail {
		f.failCount++
	}
	delay := f.delayOffset > 0 && req.Offset == f.delayOffset
	f.mu.Unlock()

	if delay {
		time.Sleep(f.delayDuration)
	}

	if fail {
		return errors.New("simulated chunk failure")
	}

	data := bytes.Repeat([]byte{byte(req.Offset / 512)}, req.Limit)
	res := &tg.UploadFile{
		Type:  &tg.StorageFilePartial{},
		Mtime: int(time.Now().Unix()),
		Bytes: data,
	}

	out, ok := output.(*tg.UploadFileBox)
	if ok {
		out.File = res
	}
	return nil
}

type slowMemWriterAt struct {
	mu    sync.Mutex
	buf   []byte
	delay time.Duration
}

func newSlowMemWriterAt(size int, delay time.Duration) *slowMemWriterAt {
	return &slowMemWriterAt{buf: make([]byte, size), delay: delay}
}

func (m *slowMemWriterAt) WriteAt(p []byte, off int64) (int, error) {
	if m.delay > 0 {
		time.Sleep(m.delay)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if int(off)+len(p) > len(m.buf) {
		return 0, io.ErrShortWrite
	}
	copy(m.buf[off:], p)
	return len(p), nil
}

func TestDownloader_LateRPCFromOldLeaseGenerationDoesNotCorruptNewFile(t *testing.T) {
	invoker := &delayedFakeInvoker{
		failOffset:    0, // fail offset 0 of first file
		delayOffset:   512 * 1024,
		delayDuration: 300 * time.Millisecond,
	}
	pool := &fakePool{invoker: invoker}

	// 2 Large files (1.5 MB each)
	elems := []*fakeElem{
		{
			file: &fakeFile{size: 1536 * 1024, dc: 4},
			buf:  newMemWriterAt(1536 * 1024),
		},
		{
			file: &fakeFile{size: 1536 * 1024, dc: 4},
			buf:  newMemWriterAt(1536 * 1024),
		},
	}

	iter := &fakeIter{elems: elems}
	progress := newFakeProgress()
	fg := gate.NewFloodGate(1000, 100)

	dl := New(Options{
		Pool:            pool,
		Threads:         16,
		DiskWorkers:     2, // 1 large disk writer slot (2-1=1) shared across File 1 then File 2!
		FileConcurrency: 1,
		Iter:            iter,
		Progress:        progress,
		FloodGate:       fg,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := dl.Download(ctx, 16)
	require.NoError(t, err)

	progress.mu.Lock()
	defer progress.mu.Unlock()

	// File 0 failed cleanly
	assert.Error(t, progress.done[elems[0]])
	// File 1 succeeded completely without corruption or dropped chunks!
	assert.NoError(t, progress.done[elems[1]])
	assert.Equal(t, int64(1536*1024), progress.downloaded[elems[1]])
}

func TestDownloader_Massive100SmallFilesWithSlowDiskWriter(t *testing.T) {
	invoker := &fakeInvoker{}
	pool := &fakePool{invoker: invoker}

	// 100 small files (50KB each = 5MB total) with 2ms slow disk write delay
	elems := make([]*fakeElem, 0, 100)
	for i := 0; i < 100; i++ {
		elems = append(elems, &fakeElem{
			file: &fakeFile{size: 50 * 1024, dc: 4},
			buf:  newSlowMemWriterAt(50*1024, 2*time.Millisecond),
		})
	}

	iter := &fakeIter{elems: elems}
	progress := newFakeProgress()
	fg := gate.NewFloodGate(1000, 100)

	dl := New(Options{
		Pool:            pool,
		Threads:         32,
		DiskWorkers:     6,
		FileConcurrency: 5,
		SmallMemBudget:  128 * 1024 * 1024,
		Iter:            iter,
		Progress:        progress,
		FloodGate:       fg,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := dl.Download(ctx, 32)
	require.NoError(t, err)

	progress.mu.Lock()
	defer progress.mu.Unlock()

	assert.Equal(t, 100, len(progress.added))
	assert.Equal(t, 100, len(progress.done))
	for _, e := range elems {
		assert.NoError(t, progress.done[e])
		assert.Equal(t, e.file.size, progress.downloaded[e])
	}
}

type trackWriterAt struct {
	mu           sync.Mutex
	buf          []byte
	delay        time.Duration
	activeWrites int32
	maxActive    int32
	finishedAt   time.Time
}

func newTrackWriterAt(size int, delay time.Duration) *trackWriterAt {
	return &trackWriterAt{buf: make([]byte, size), delay: delay}
}

func (w *trackWriterAt) WriteAt(p []byte, off int64) (int, error) {
	atomic.AddInt32(&w.activeWrites, 1)
	defer func() {
		atomic.AddInt32(&w.activeWrites, -1)
		w.mu.Lock()
		w.finishedAt = time.Now()
		w.mu.Unlock()
	}()
	if w.delay > 0 {
		time.Sleep(w.delay)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if int(off)+len(p) > len(w.buf) {
		return 0, io.ErrShortWrite
	}
	copy(w.buf[off:], p)
	return len(p), nil
}

func TestDownloader_SlowWriterDrainedBeforeSlotReused(t *testing.T) {
	invoker := &fakeInvoker{}
	pool := &fakePool{invoker: invoker}

	w1 := newTrackWriterAt(1536*1024, 50*time.Millisecond)
	w2 := newTrackWriterAt(1536*1024, 0)

	elem1 := &fakeElem{
		file: &fakeFile{size: 1536 * 1024, dc: 4},
		buf:  w1,
	}
	elem2 := &fakeElem{
		file: &fakeFile{size: 1536 * 1024, dc: 4},
		buf:  w2,
	}

	iter := &fakeIter{elems: []*fakeElem{elem1, elem2}}
	progress := newFakeProgress()
	fg := gate.NewFloodGate(1000, 100)

	dl := New(Options{
		Pool:            pool,
		Threads:         8,
		DiskWorkers:     2, // strictly 1 large writer slot shared across File 1 then File 2
		FileConcurrency: 1,
		Iter:            iter,
		Progress:        progress,
		FloodGate:       fg,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Cancel elem1 after 30ms
	go func() {
		time.Sleep(30 * time.Millisecond)
		elem1.Cancel()
	}()

	err := dl.Download(ctx, 8)
	require.NoError(t, err)

	progress.mu.Lock()
	defer progress.mu.Unlock()

	// File 1 was canceled
	assert.Error(t, progress.done[elem1])
	// File 2 completed successfully
	assert.NoError(t, progress.done[elem2])

	// Verify that File 1 has zero active writes
	assert.Equal(t, int32(0), atomic.LoadInt32(&w1.activeWrites))
}

func TestDownloader_1MiBPartAlignmentAndTailTruncation(t *testing.T) {
	invoker := &fakeInvoker{}
	pool := &fakePool{invoker: invoker}

	// 2.5 MiB file -> Part 0 (1 MiB), Part 1 (1 MiB), Part 2 (0.5 MiB)
	elem := &fakeElem{
		file: &fakeFile{size: 2621440, dc: 4}, // 2.5 MiB
		buf:  newMemWriterAt(2621440),
	}

	iter := &fakeIter{elems: []*fakeElem{elem}}
	progress := newFakeProgress()
	fg := gate.NewFloodGate(1000, 100)

	dl := New(Options{
		Pool:            pool,
		Threads:         16,
		DiskWorkers:     6,
		FileConcurrency: 5,
		Iter:            iter,
		Progress:        progress,
		FloodGate:       fg,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := dl.Download(ctx, 16)
	require.NoError(t, err)

	progress.mu.Lock()
	defer progress.mu.Unlock()

	assert.NoError(t, progress.done[elem])
	assert.Equal(t, int64(2621440), progress.downloaded[elem])

	invoker.mu.Lock()
	defer invoker.mu.Unlock()

	// Verify all offsets are 1 MiB aligned: 0, 1048576, 2097152
	expectedOffsets := []int64{0, 1048576, 2097152}
	assert.ElementsMatch(t, expectedOffsets, invoker.requests)
}

type trackMaxInFlightInvoker struct {
	mu           sync.Mutex
	activeRPC    int64
	maxActiveRPC int64
	delay        time.Duration
}

func (f *trackMaxInFlightInvoker) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	f.mu.Lock()
	f.activeRPC++
	if f.activeRPC > f.maxActiveRPC {
		f.maxActiveRPC = f.activeRPC
	}
	f.mu.Unlock()

	defer func() {
		f.mu.Lock()
		f.activeRPC--
		f.mu.Unlock()
	}()

	if f.delay > 0 {
		time.Sleep(f.delay)
	}

	req, ok := input.(*tg.UploadGetFileRequest)
	if !ok {
		return errors.New("unexpected input type")
	}

	data := bytes.Repeat([]byte{1}, req.Limit)
	res := &tg.UploadFile{
		Type:  &tg.StorageFilePartial{},
		Mtime: int(time.Now().Unix()),
		Bytes: data,
	}

	out, ok := output.(*tg.UploadFileBox)
	if ok {
		out.File = res
	}
	return nil
}

func TestDownloader_MaxDataInFlightNeverExceeds40(t *testing.T) {
	invoker := &trackMaxInFlightInvoker{delay: 15 * time.Millisecond}
	pool := &fakePool{invoker: invoker}

	// 5 large files (10 MiB each) + 50 small files (100 KiB each)
	elems := make([]*fakeElem, 0, 55)
	for i := 0; i < 5; i++ {
		elems = append(elems, &fakeElem{
			file: &fakeFile{size: 10 * 1024 * 1024, dc: 4},
			buf:  newMemWriterAt(10 * 1024 * 1024),
		})
	}
	for i := 0; i < 50; i++ {
		elems = append(elems, &fakeElem{
			file: &fakeFile{size: 100 * 1024, dc: 4},
			buf:  newMemWriterAt(100 * 1024),
		})
	}

	iter := &fakeIter{elems: elems}
	progress := newFakeProgress()
	fg := gate.NewFloodGate(1000, 100)

	dl := New(Options{
		Pool:            pool,
		Threads:         64, // High worker count
		DiskWorkers:     6,
		FileConcurrency: 5,
		Iter:            iter,
		Progress:        progress,
		FloodGate:       fg,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := dl.Download(ctx, 64)
	require.NoError(t, err)

	invoker.mu.Lock()
	maxActive := invoker.maxActiveRPC
	invoker.mu.Unlock()

	// Invariant 1: Total simultaneous data RPCs in flight NEVER exceeds 40!
	assert.LessOrEqual(t, maxActive, int64(gate.MaxDataInFlight))
}

func TestDownloader_SmallFilesBorrowAll40SlotsWhenNoLargeFiles(t *testing.T) {
	invoker := &trackMaxInFlightInvoker{delay: 20 * time.Millisecond}
	pool := &fakePool{invoker: invoker}

	// 100 small files ONLY (no large files)
	elems := make([]*fakeElem, 0, 100)
	for i := 0; i < 100; i++ {
		elems = append(elems, &fakeElem{
			file: &fakeFile{size: 200 * 1024, dc: 4},
			buf:  newMemWriterAt(200 * 1024),
		})
	}

	iter := &fakeIter{elems: elems}
	progress := newFakeProgress()
	fg := gate.NewFloodGate(1000, 100)

	dl := New(Options{
		Pool:            pool,
		Threads:         64,
		DiskWorkers:     6,
		FileConcurrency: 5,
		Iter:            iter,
		Progress:        progress,
		FloodGate:       fg,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := dl.Download(ctx, 64)
	require.NoError(t, err)

	invoker.mu.Lock()
	maxActive := invoker.maxActiveRPC
	invoker.mu.Unlock()

	// Small files should be able to scale up toward 40
	assert.Greater(t, maxActive, int64(20))
	assert.LessOrEqual(t, maxActive, int64(gate.MaxDataInFlight))
}

func TestDownloader_100SmallFilesBurstLargeThroughputProtected(t *testing.T) {
	invoker := &fakeInvoker{}
	pool := &fakePool{invoker: invoker}

	// 5 Large files (5 MiB each) + 100 Small files (50 KiB each)
	elems := make([]*fakeElem, 0, 105)
	largeElems := make([]*fakeElem, 0, 5)
	for i := 0; i < 5; i++ {
		e := &fakeElem{
			file: &fakeFile{size: 5 * 1024 * 1024, dc: 4},
			buf:  newMemWriterAt(5 * 1024 * 1024),
		}
		elems = append(elems, e)
		largeElems = append(largeElems, e)
	}
	for i := 0; i < 100; i++ {
		elems = append(elems, &fakeElem{
			file: &fakeFile{size: 50 * 1024, dc: 4},
			buf:  newMemWriterAt(50 * 1024),
		})
	}

	iter := &fakeIter{elems: elems}
	progress := newFakeProgress()
	fg := gate.NewFloodGate(1000, 100)

	dl := New(Options{
		Pool:            pool,
		Threads:         64,
		DiskWorkers:     6,
		FileConcurrency: 5,
		Iter:            iter,
		Progress:        progress,
		FloodGate:       fg,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := dl.Download(ctx, 64)
	require.NoError(t, err)

	progress.mu.Lock()
	defer progress.mu.Unlock()

	// All 105 files completed
	assert.Equal(t, 105, len(progress.added))
	assert.Equal(t, 105, len(progress.done))
	for _, e := range largeElems {
		assert.NoError(t, progress.done[e])
		assert.Equal(t, int64(5*1024*1024), progress.downloaded[e])
	}
}

func TestDownloader_CdnRedirectDecryptionAndHashVerification(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef") // 32 bytes
	iv := []byte("1234567890abcdef")                 // 16 bytes
	plainData := bytes.Repeat([]byte("A"), 1024*1024)

	// Encrypt plainData with AES-CTR
	block, err := aes.NewCipher(key)
	require.NoError(t, err)
	encryptedData := make([]byte, len(plainData))
	stream := cipher.NewCTR(block, iv)
	stream.XORKeyStream(encryptedData, plainData)

	chunkHash := sha256.Sum256(encryptedData)

	cdnInvoker := &fakeCDNInvoker{encryptedData: encryptedData}
	masterInvoker := &fakeMasterCDNRedirectInvoker{
		key:       key,
		iv:        iv,
		hashes:    []tg.FileHash{{Offset: 0, Limit: len(plainData), Hash: chunkHash[:]}},
		fileToken: []byte("test_file_token"),
	}

	pool := &fakeDualPool{master: masterInvoker, cdn: cdnInvoker}
	buf := newMemWriterAt(int64(len(plainData)))
	elem := &fakeElem{
		file: &fakeFile{size: int64(len(plainData)), dc: 2},
		buf:  buf,
	}

	iter := &fakeIter{elems: []*fakeElem{elem}}
	progress := newFakeProgress()
	fg := gate.NewFloodGate(1000, 100)

	dl := New(Options{
		Pool:            pool,
		Threads:         4,
		DiskWorkers:     2,
		FileConcurrency: 1,
		Iter:            iter,
		Progress:        progress,
		FloodGate:       fg,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = dl.Download(ctx, 4)
	require.NoError(t, err)

	progress.mu.Lock()
	defer progress.mu.Unlock()

	assert.NoError(t, progress.done[elem])
	assert.Equal(t, int64(len(plainData)), progress.downloaded[elem])

	// Verify decrypted content on disk matches original plainData
	buf.mu.Lock()
	assert.Equal(t, plainData, buf.data)
	buf.mu.Unlock()
}

type fakeMasterCDNRedirectInvoker struct {
	key       []byte
	iv        []byte
	hashes    []tg.FileHash
	fileToken []byte
}

func (f *fakeMasterCDNRedirectInvoker) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	out, ok := output.(*tg.UploadFileBox)
	if ok {
		out.File = &tg.UploadFileCDNRedirect{
			DCID:          5, // CDN DC
			FileToken:     f.fileToken,
			EncryptionKey: f.key,
			EncryptionIv:  f.iv,
			FileHashes:    f.hashes,
		}
	}
	return nil
}

type fakeCDNInvoker struct {
	encryptedData []byte
}

func (f *fakeCDNInvoker) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	out, ok := output.(*tg.UploadCDNFileBox)
	if ok {
		out.CdnFile = &tg.UploadCDNFile{
			Bytes: f.encryptedData,
		}
	}
	return nil
}

type fakeDualPool struct {
	master tg.Invoker
	cdn    tg.Invoker
}

func (p *fakeDualPool) Client(ctx context.Context, dc int) *tg.Client {
	if dc == 5 {
		return tg.NewClient(p.cdn)
	}
	return tg.NewClient(p.master)
}
func (p *fakeDualPool) Takeout(ctx context.Context, dc int) *tg.Client {
	return p.Client(ctx, dc)
}
func (p *fakeDualPool) Default(ctx context.Context) *tg.Client {
	return tg.NewClient(p.master)
}
func (p *fakeDualPool) Close() error { return nil }
