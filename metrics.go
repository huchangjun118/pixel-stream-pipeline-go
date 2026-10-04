package pixelstream

import "sync/atomic"

// metricsEventCap 是内存中保留的帧事件上限（环形覆盖），
// 既足够测试断言，又保证长跑不泄漏内存。
const metricsEventCap = 4096

// Metrics 暴露管线各环节的计数器与可复现事件流。
// 计数使用原子量；事件与按原因的丢帧统计使用同一把短锁。
type Metrics struct {
	submitted        atomic.Int64
	accepted         atomic.Int64
	dropped          atomic.Int64
	encoded          atomic.Int64
	keyFrames        atomic.Int64
	deltaFrames      atomic.Int64
	encodeFailures   atomic.Int64
	viewersTotal     atomic.Int64
	viewersClosed    atomic.Int64
	resyncs          atomic.Int64
	pliRequests      atomic.Int64
	sourceInterrupts atomic.Int64

	mu      mutex // 保护下面字段
	drops   map[DropReason]int64
	events  []FrameEvent
	evHead  int
	evFull  bool
	quality int
}

func newMetrics() *Metrics {
	return &Metrics{drops: make(map[DropReason]int64)}
}

// MetricsSnapshot 是某一时刻的一致快照（拷贝）。
type MetricsSnapshot struct {
	Submitted        int64
	Accepted         int64
	Dropped          int64
	Encoded          int64
	KeyFrames        int64
	DeltaFrames      int64
	EncodeFailures   int64
	ViewersTotal     int64
	ViewersClosed    int64
	Resyncs          int64
	PLIRequests      int64
	SourceInterrupts int64
	Drops            map[DropReason]int64
	CurrentQuality   int
}

func (m *Metrics) recordDrop(r DropReason) {
	if r == DropNone {
		return
	}
	m.dropped.Add(1)
	m.mu.Lock()
	m.drops[r]++
	m.mu.Unlock()
}

func (m *Metrics) recordEvent(e FrameEvent) {
	m.mu.Lock()
	if len(m.events) < metricsEventCap {
		m.events = append(m.events, e)
	} else {
		m.events[m.evHead] = e
		m.evHead = (m.evHead + 1) % metricsEventCap
		m.evFull = true
	}
	m.mu.Unlock()
}

func (m *Metrics) setQuality(lv int) {
	m.mu.Lock()
	m.quality = lv
	m.mu.Unlock()
}

// Snapshot 返回计数器与丢帧分布的拷贝。
func (m *Metrics) Snapshot() MetricsSnapshot {
	m.mu.Lock()
	drops := make(map[DropReason]int64, len(m.drops))
	for k, v := range m.drops {
		drops[k] = v
	}
	q := m.quality
	m.mu.Unlock()
	return MetricsSnapshot{
		Submitted:        m.submitted.Load(),
		Accepted:         m.accepted.Load(),
		Dropped:          m.dropped.Load(),
		Encoded:          m.encoded.Load(),
		KeyFrames:        m.keyFrames.Load(),
		DeltaFrames:      m.deltaFrames.Load(),
		EncodeFailures:   m.encodeFailures.Load(),
		ViewersTotal:     m.viewersTotal.Load(),
		ViewersClosed:    m.viewersClosed.Load(),
		Resyncs:          m.resyncs.Load(),
		PLIRequests:      m.pliRequests.Load(),
		SourceInterrupts: m.sourceInterrupts.Load(),
		Drops:            drops,
		CurrentQuality:   q,
	}
}

// Events 按发生顺序返回当前缓冲内的全部帧事件。
func (m *Metrics) Events() []FrameEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.evFull {
		out := make([]FrameEvent, len(m.events))
		copy(out, m.events)
		return out
	}
	out := make([]FrameEvent, metricsEventCap)
	copy(out, m.events[m.evHead:])
	copy(out[len(m.events)-m.evHead:], m.events[:m.evHead])
	return out
}
