package pipeline

import "sync/atomic"

// Metrics 汇总管线运行期计数，全部使用原子操作，可无锁并发读取。
type Metrics struct {
	FramesIn        atomic.Uint64 // 进入管线的帧数
	FramesSequenced atomic.Uint64 // 重排后输出的帧数
	FramesEncoded   atomic.Uint64 // 成功编码的帧数
	FramesBroadcast atomic.Uint64 // 扇出次数（按观看端计数）
	EncodeErrors    atomic.Uint64 // 编码失败次数

	dropped [dropReasonCount]atomic.Uint64
}

const dropReasonCount = int(DropEncodeError) + 1

// MetricsSnapshot 是某一时刻的只读快照。
type MetricsSnapshot struct {
	FramesIn        uint64
	FramesSequenced uint64
	FramesEncoded   uint64
	FramesBroadcast uint64
	EncodeErrors    uint64
	Dropped         map[DropReason]uint64 // 按原因区分的丢弃计数
}

func newMetrics() *Metrics { return &Metrics{} }

func (m *Metrics) drop(reason DropReason) {
	if int(reason) >= 1 && int(reason) < len(m.dropped) {
		m.dropped[int(reason)].Add(1)
	}
}

// Dropped 返回某类原因的总丢弃数。
func (m *Metrics) Dropped(reason DropReason) uint64 {
	if int(reason) >= 1 && int(reason) < len(m.dropped) {
		return m.dropped[int(reason)].Load()
	}
	return 0
}

// Snapshot 读取当前计数快照。
func (m *Metrics) Snapshot() MetricsSnapshot {
	s := MetricsSnapshot{
		FramesIn:        m.FramesIn.Load(),
		FramesSequenced: m.FramesSequenced.Load(),
		FramesEncoded:   m.FramesEncoded.Load(),
		FramesBroadcast: m.FramesBroadcast.Load(),
		EncodeErrors:    m.EncodeErrors.Load(),
		Dropped:         make(map[DropReason]uint64),
	}
	for r := DropDuplicate; r <= DropEncodeError; r++ {
		if v := m.Dropped(r); v > 0 {
			s.Dropped[r] = v
		}
	}
	return s
}
