package pipeline

import "time"

// Frame 是采集端产出的一帧原始画面。
type Frame struct {
	Seq        uint64    // 采集源单调递增的序号
	CapturedAt time.Time // 采集时间戳
	Width      int
	Height     int
	Pixels     []byte // RGBA，长度应为 Width*Height*4
}

// DropReason 标识一帧被丢弃的原因，所有丢弃点都必须给出可区分的原因。
type DropReason int

const (
	DropDuplicate        DropReason = iota + 1 // 重复帧（序号已见过）
	DropLate                                   // 迟到帧（序号小于已输出的最大序号）
	DropEncodeQueueFull                        // 编码队列积压，按策略丢弃
	DropAdaptiveThrottle                       // 自适应降帧率抽帧
	DropSubscriberSlow                         // 观看端消费过慢，跳过至下一关键帧
	DropEncodeError                            // 编码失败
)

func (r DropReason) String() string {
	switch r {
	case DropDuplicate:
		return "duplicate"
	case DropLate:
		return "late"
	case DropEncodeQueueFull:
		return "encode_queue_full"
	case DropAdaptiveThrottle:
		return "adaptive_throttle"
	case DropSubscriberSlow:
		return "subscriber_slow"
	case DropEncodeError:
		return "encode_error"
	}
	return "unknown"
}

// EncodedFrame 是编码后的字节流单元，可被多个观看端安全共享（只读）。
type EncodedFrame struct {
	Seq        uint64
	CapturedAt time.Time
	Keyframe   bool
	Quality    int
	Data       []byte
}
