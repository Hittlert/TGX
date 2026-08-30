package downloader

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
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
	file   *fakeFile
	buf    *memWriterAt
	take   bool
	onDone func(error)
}

func (e *fakeElem) File() File          { return e.file }
func (e *fakeElem) To() io.WriterAt     { return e.buf }
func (e *fakeElem) AsTakeout() bool     { return e.take }

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
	mu       sync.Mutex
	requests []int64 // offsets
}

func (f *fakeInvoker) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	req, ok := input.(*tg.UploadGetFileRequest)
	if !ok {
		return errors.New("unexpected input type")
	}

	f.mu.Lock()
	f.requests = append(f.requests, req.Offset)
	f.mu.Unlock()

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
	invoker *fakeInvoker
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

func TestDownloader_ShortReadValidationAndRecovery(t *testing.T) {
	invoker := &shortReadInvoker{}
	clientPool := &shortReadPool{invoker: invoker}

	elem := &fakeElem{
		file: &fakeFile{size: 512 * 1024, dc: 2},
		buf:  newMemWriterAt(512 * 1024),
	}

	iter := &fakeIter{elems: []*fakeElem{elem}}
	progress := newFakeProgress()
	fg := gate.NewFloodGate(1000, 100)

	dl := New(Options{
		Pool:            clientPool,
		Threads:         1,
		DiskWorkers:     1,
		FileConcurrency: 1,
		Iter:            iter,
		Progress:        progress,
		FloodGate:       fg,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := dl.Download(ctx, 1)
	require.NoError(t, err)

	progress.mu.Lock()
	defer progress.mu.Unlock()

	assert.NoError(t, progress.done[elem])
	assert.Equal(t, int64(512*1024), progress.downloaded[elem])
	assert.GreaterOrEqual(t, invoker.tries, 2, "short read must trigger retry")
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

func TestDownloader_CancellationZeroPanic(t *testing.T) {
	invoker := &fakeInvoker{}
	pool := &fakePool{invoker: invoker}

	// Create 10 files with 10MB each
	elems := make([]*fakeElem, 10)
	for i := 0; i < 10; i++ {
		elems[i] = &fakeElem{
			file: &fakeFile{size: 10 * 1024 * 1024, dc: 4},
			buf:  newMemWriterAt(10 * 1024 * 1024),
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

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	// Must cleanly return on context cancellation with ZERO panics or send on closed channel
	err := dl.Download(ctx, 16)
	assert.Error(t, err)
}
