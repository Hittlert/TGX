package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

type NodeSample struct {
	Timestamp int64   `json:"ts"`
	SpeedBPS  float64 `json:"bps"`
}

type NodeStat struct {
	Samples    []NodeSample `json:"samples"`
	LastPing   *int         `json:"last_ping"`
	LastActive int64        `json:"last_active"`
}

type ProxyMetrics struct {
	PeakSpeedBPS       int64   `json:"peak_speed_bps"`
	PeakSpeedStr       string  `json:"peak_speed_str"`
	AvgSpeedBPS        int64   `json:"avg_speed_bps"`
	AvgSpeedStr        string  `json:"avg_speed_str"`
	TotalBytes24h      float64 `json:"total_bytes_24h"`
	TotalBytesStr      string  `json:"total_bytes_str"`
	ActiveSamplesCount int     `json:"active_samples_count"`
	LastActiveStr      string  `json:"last_active_str"`
	LastPingMS         *int    `json:"last_ping_ms"`
	IsPeakLeader       bool    `json:"is_peak_leader"`
	EffectiveScore     float64 `json:"effective_score"`
}

type ProxyNode struct {
	Tag        string       `json:"tag"`
	Name       string       `json:"name"`
	Type       string       `json:"type"`
	Metrics24h ProxyMetrics `json:"metrics_24h"`
}

type ProxyManager struct {
	apiURL     string
	statsFile  string
	httpClient *http.Client
	mu         sync.RWMutex
	nodeStats  map[string]*NodeStat
	activeTag  string
	lastSave   int64
}

func NewProxyManager(apiURL string, statsFile string) *ProxyManager {
	if apiURL == "" {
		apiURL = "http://127.0.0.1:9090"
	}
	pm := &ProxyManager{
		apiURL:    apiURL,
		statsFile: statsFile,
		httpClient: &http.Client{
			Timeout: 6 * time.Second,
		},
		nodeStats: make(map[string]*NodeStat),
	}
	pm.loadStats()
	return pm
}

func (m *ProxyManager) loadStats() {
	if m.statsFile == "" {
		return
	}
	data, err := os.ReadFile(m.statsFile)
	if err != nil {
		return
	}
	var loaded map[string]*NodeStat
	if err := json.Unmarshal(data, &loaded); err != nil {
		return
	}
	now := time.Now().Unix()
	cutoff := now - 86400

	m.mu.Lock()
	defer m.mu.Unlock()
	for tag, stat := range loaded {
		if stat == nil {
			continue
		}
		valid := make([]NodeSample, 0, len(stat.Samples))
		for _, s := range stat.Samples {
			if s.Timestamp >= cutoff {
				valid = append(valid, s)
			}
		}
		stat.Samples = valid
		m.nodeStats[tag] = stat
	}
}

func (m *ProxyManager) saveStatsLocked() {
	if m.statsFile == "" {
		return
	}
	now := time.Now().Unix()
	if now-m.lastSave < 10 {
		return
	}
	m.lastSave = now

	data, err := json.Marshal(m.nodeStats)
	if err != nil {
		return
	}
	_ = os.WriteFile(m.statsFile, data, 0o644)
}

func (m *ProxyManager) GetActiveProxy() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.activeTag
}

func (m *ProxyManager) SetActiveProxy(tag string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.activeTag = tag
}

func (m *ProxyManager) RecordDownloadSpeed(tag string, speedBPS float64) {
	if tag == "" || speedBPS < 0 {
		return
	}
	now := time.Now().Unix()
	cutoff := now - 86400

	m.mu.Lock()
	defer m.mu.Unlock()

	stat, ok := m.nodeStats[tag]
	if !ok {
		stat = &NodeStat{Samples: make([]NodeSample, 0)}
		m.nodeStats[tag] = stat
	}

	valid := make([]NodeSample, 0, len(stat.Samples)+1)
	for _, s := range stat.Samples {
		if s.Timestamp >= cutoff {
			valid = append(valid, s)
		}
	}
	valid = append(valid, NodeSample{Timestamp: now, SpeedBPS: speedBPS})
	stat.Samples = valid

	if speedBPS > 10240 {
		stat.LastActive = now
	}

	m.saveStatsLocked()
}

type clashProxiesResp struct {
	Proxies map[string]struct {
		Name    string   `json:"name"`
		Type    string   `json:"type"`
		Now     string   `json:"now"`
		All     []string `json:"all"`
		History []struct {
			Delay int `json:"delay"`
		} `json:"history"`
	} `json:"proxies"`
}

func (m *ProxyManager) GetProxyList(ctx context.Context) ([]ProxyNode, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.apiURL+"/proxies", nil)
	if err != nil {
		return nil, "", err
	}

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	var data clashProxiesResp
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, "", err
	}

	currentSelected := ""
	var nodes []ProxyNode

	var selectorAll []string
	for _, p := range data.Proxies {
		if p.Type == "Selector" {
			currentSelected = p.Now
			selectorAll = p.All
			break
		}
	}

	if currentSelected != "" {
		m.SetActiveProxy(currentSelected)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	now := time.Now().Unix()
	cutoff := now - 86400
	var maxPeak int64 = 0

	if len(selectorAll) > 0 {
		for _, name := range selectorAll {
			nodeType := "vless"
			var lastPing *int

			stat := m.nodeStats[name]
			if stat != nil && stat.LastPing != nil {
				lastPing = stat.LastPing
			}

			if nodeData, ok := data.Proxies[name]; ok {
				if nodeData.Type != "" {
					nodeType = nodeData.Type
				}
				if len(nodeData.History) > 0 {
					pVal := nodeData.History[len(nodeData.History)-1].Delay
					if pVal > 0 && lastPing == nil {
						lastPing = &pVal
					}
				}
			}

			var peakBPS float64 = 0
			var activeCount int = 0
			var activeSum float64 = 0
			var totalBytes float64 = 0
			var lastActive int64 = 0

			if stat != nil {
				lastActive = stat.LastActive
				samples := stat.Samples
				for i := 0; i < len(samples); i++ {
					if samples[i].Timestamp < cutoff {
						continue
					}
					s := samples[i].SpeedBPS
					if s > peakBPS {
						peakBPS = s
					}
					if s >= 51200 { // > 50KB/s
						activeCount++
						activeSum += s
					}
					if i > 0 {
						dt := samples[i].Timestamp - samples[i-1].Timestamp
						if dt > 0 && dt <= 10 {
							totalBytes += ((s + samples[i-1].SpeedBPS) / 2.0) * float64(dt)
						}
					}
				}
			}

			var avgBPS float64 = 0
			if activeCount > 0 {
				avgBPS = activeSum / float64(activeCount)
			}

			lastActiveStr := "从未活跃"
			if lastActive > 0 {
				if now-lastActive < 15 && peakBPS > 0 {
					lastActiveStr = "正在下载"
				} else {
					diff := now - lastActive
					if diff < 60 {
						lastActiveStr = "刚刚"
					} else if diff < 3600 {
						lastActiveStr = fmt.Sprintf("%d分钟前", diff/60)
					} else {
						lastActiveStr = fmt.Sprintf("%d小时前", diff/3600)
					}
				}
			}

			peakInt := int64(peakBPS)
			if peakInt > maxPeak {
				maxPeak = peakInt
			}

			peakStr := "暂无下载记录"
			if peakInt > 0 {
				peakStr = formatBytes(peakInt) + "/s"
			}
			avgStr := "暂无下载记录"
			if avgBPS > 0 {
				avgStr = formatBytes(int64(avgBPS)) + "/s"
			}

			nodes = append(nodes, ProxyNode{
				Tag:  name,
				Name: name,
				Type: nodeType,
				Metrics24h: ProxyMetrics{
					PeakSpeedBPS:       peakInt,
					PeakSpeedStr:       peakStr,
					AvgSpeedBPS:        int64(avgBPS),
					AvgSpeedStr:        avgStr,
					TotalBytes24h:      totalBytes,
					TotalBytesStr:      formatBytes(int64(totalBytes)),
					ActiveSamplesCount: activeCount,
					LastActiveStr:      lastActiveStr,
					LastPingMS:         lastPing,
					IsPeakLeader:       false,
					EffectiveScore:     100.0,
				},
			})
		}
	}

	if maxPeak > 0 {
		for i := range nodes {
			if nodes[i].Metrics24h.PeakSpeedBPS == maxPeak {
				nodes[i].Metrics24h.IsPeakLeader = true
			}
		}
	}

	return nodes, currentSelected, nil
}

func (m *ProxyManager) SwitchProxy(ctx context.Context, group, nodeName string) error {
	if group == "" {
		group = "proxy"
	}
	body, _ := json.Marshal(map[string]string{"name": nodeName})
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, fmt.Sprintf("%s/proxies/%s", m.apiURL, group), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if group == "proxy" {
			return m.SwitchProxy(ctx, "GLOBAL", nodeName)
		}
		return fmt.Errorf("switch proxy returned status %d", resp.StatusCode)
	}

	m.SetActiveProxy(nodeName)
	return nil
}

func (m *ProxyManager) PingProxy(ctx context.Context, nodeName string) (int, error) {
	url := fmt.Sprintf("%s/proxies/%s/delay?timeout=5000&url=https://cp.cloudflare.com/generate_204", m.apiURL, nodeName)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var res struct {
		Delay int `json:"delay"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return 0, err
	}

	m.mu.Lock()
	stat, ok := m.nodeStats[nodeName]
	if !ok {
		stat = &NodeStat{Samples: make([]NodeSample, 0)}
		m.nodeStats[nodeName] = stat
	}
	dVal := res.Delay
	stat.LastPing = &dVal
	m.mu.Unlock()

	return res.Delay, nil
}

func (m *ProxyManager) StartWatchdog(ctx context.Context) {
	normalInterval := 300 * time.Second // 每 300 秒（5分钟）常规存活巡检
	retryInterval := 30 * time.Second   // 失败后 30 秒间隔复测
	maxRetries := 3                     // 连续 3 次复测全部失败才判定断网

	timer := time.NewTimer(normalInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			active := m.GetActiveProxy()
			if active == "" {
				_, cur, err := m.GetProxyList(ctx)
				if err == nil && cur != "" {
					active = cur
					m.SetActiveProxy(cur)
				}
			}
			if active == "" {
				timer.Reset(normalInterval)
				continue
			}

			// 1. 常规存活探针
			pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			delay, err := m.PingProxy(pingCtx, active)
			cancel()

			if err == nil && delay > 0 {
				// 节点完全健康，保持不变，进入下一个 300 秒周期
				timer.Reset(normalInterval)
				continue
			}

			// 2. 常规探活失败，进入 30s 间隔连续 3 次复测确认（防抖动，避免随机飘移 IP）
			confirmedDead := true
			for attempt := 1; attempt <= maxRetries; attempt++ {
				select {
				case <-ctx.Done():
					return
				case <-time.After(retryInterval):
				}

				pCtx, pCancel := context.WithTimeout(ctx, 5*time.Second)
				d, e := m.PingProxy(pCtx, active)
				pCancel()

				if e == nil && d > 0 {
					// 仅为瞬时网络抖动，已自动恢复，继续留在当前节点
					confirmedDead = false
					break
				}
			}

			// 3. 若 3 次连续 30s 复测全部超时/不可达（累计确认 ~90s），执行确定性顺位轮转切换（下一个有效节点，末尾回绕）
			if confirmedDead {
				nodes, cur, err := m.GetProxyList(ctx)
				if err == nil && len(nodes) > 0 {
					currentIdx := -1
					for i, n := range nodes {
						if n.Name == cur || n.Tag == cur {
							currentIdx = i
							break
						}
					}

					// 顺位寻找下一个存活的代理节点（最后一个切回第一个）
					for step := 1; step <= len(nodes); step++ {
						nextIdx := (currentIdx + step) % len(nodes)
						candidate := nodes[nextIdx]
						if candidate.Type == "Selector" || candidate.Type == "Fallback" || candidate.Type == "Direct" {
							continue
						}

						tCtx, tCancel := context.WithTimeout(ctx, 4*time.Second)
						tDelay, tErr := m.PingProxy(tCtx, candidate.Name)
						tCancel()

						if tErr == nil && tDelay > 0 {
							// 平滑顺位切换到下一个确认存活的节点
							_ = m.SwitchProxy(ctx, "proxy", candidate.Name)
							break
						}
					}
				}
			}

			// 切换完成或确认恢复后，重置回 300 秒长周期巡检
			timer.Reset(normalInterval)
		}
	}
}
