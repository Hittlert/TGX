package daemon

import (
	"context"
	"math"
	"sync"
)

type SlotPoolConfig struct {
	TotalSlots      int
	MaxActiveFiles  int
	SlotUnitMB      int
	MaxSlotsPerFile int
}

func DefaultSlotPoolConfig() SlotPoolConfig {
	return SlotPoolConfig{
		TotalSlots:      64,
		MaxActiveFiles:  8,
		SlotUnitMB:      4,
		MaxSlotsPerFile: 16,
	}
}

type SlotPoolSnapshot struct {
	TotalSlots         int     `json:"total_slots"`
	UsedSlots          int     `json:"used_slots"`
	AvailableSlots     int     `json:"available_slots"`
	MaxActiveFiles     int     `json:"max_active_files"`
	ActiveFilesCount   int     `json:"active_files_count"`
	SlotUnitMB         int     `json:"slot_unit_mb"`
	MaxSlotsPerFile    int     `json:"max_slots_per_file"`
	SlotUtilizationPct float64 `json:"slot_utilization_pct"`
	FileUtilizationPct float64 `json:"file_utilization_pct"`
}

type GlobalSlotPool struct {
	cfg        SlotPoolConfig
	mu         sync.Mutex
	cond       *sync.Cond
	usedSlots  int
	activeRuns map[string]int
}

func NewGlobalSlotPool(cfg SlotPoolConfig) *GlobalSlotPool {
	p := &GlobalSlotPool{
		cfg:        cfg,
		activeRuns: make(map[string]int),
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

func (p *GlobalSlotPool) recalculateUsedSlotsLocked() {
	total := 0
	for _, s := range p.activeRuns {
		total += s
	}
	p.usedSlots = total
}

func (p *GlobalSlotPool) CalculateRequiredSlots(sizeBytes int64) int {
	if sizeBytes <= 0 {
		return 1
	}
	unitBytes := int64(p.cfg.SlotUnitMB) * 1024 * 1024
	slots := int(math.Ceil(float64(sizeBytes) / float64(unitBytes)))
	if slots < 1 {
		slots = 1
	}
	if slots > p.cfg.MaxSlotsPerFile {
		slots = p.cfg.MaxSlotsPerFile
	}
	if slots > p.cfg.TotalSlots {
		slots = p.cfg.TotalSlots
	}
	return slots
}

func (p *GlobalSlotPool) Acquire(ctx context.Context, taskID string, sizeBytes int64) (int, error) {
	requiredSlots := p.CalculateRequiredSlots(sizeBytes)

	// Context watcher to ensure cond.Wait() never hangs on canceled context
	stopWatcher := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			p.mu.Lock()
			p.cond.Broadcast()
			p.mu.Unlock()
		case <-stopWatcher:
		}
	}()
	defer close(stopWatcher)

	p.mu.Lock()
	defer p.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}

		p.recalculateUsedSlotsLocked()

		canRunFile := len(p.activeRuns) < p.cfg.MaxActiveFiles
		canTakeSlots := (p.usedSlots + requiredSlots) <= p.cfg.TotalSlots

		if canRunFile && canTakeSlots {
			p.activeRuns[taskID] = requiredSlots
			p.recalculateUsedSlotsLocked()
			return requiredSlots, nil
		}

		p.cond.Wait()
	}
}

func (p *GlobalSlotPool) Release(taskID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.activeRuns, taskID)
	p.recalculateUsedSlotsLocked()
	p.cond.Broadcast()
}

func (p *GlobalSlotPool) Snapshot() SlotPoolSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.recalculateUsedSlotsLocked()

	activeFiles := len(p.activeRuns)
	availSlots := p.cfg.TotalSlots - p.usedSlots
	if availSlots < 0 {
		availSlots = 0
	}

	slotUtil := 0.0
	if p.cfg.TotalSlots > 0 {
		slotUtil = (float64(p.usedSlots) / float64(p.cfg.TotalSlots)) * 100.0
	}

	fileUtil := 0.0
	if p.cfg.MaxActiveFiles > 0 {
		fileUtil = (float64(activeFiles) / float64(p.cfg.MaxActiveFiles)) * 100.0
	}

	return SlotPoolSnapshot{
		TotalSlots:         p.cfg.TotalSlots,
		UsedSlots:          p.usedSlots,
		AvailableSlots:     availSlots,
		MaxActiveFiles:     p.cfg.MaxActiveFiles,
		ActiveFilesCount:   activeFiles,
		SlotUnitMB:         p.cfg.SlotUnitMB,
		MaxSlotsPerFile:    p.cfg.MaxSlotsPerFile,
		SlotUtilizationPct: math.Round(slotUtil*10) / 10,
		FileUtilizationPct: math.Round(fileUtil*10) / 10,
	}
}
