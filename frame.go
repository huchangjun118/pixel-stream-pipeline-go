package pixelstream

import "time"

// FrameType 标识编码帧的类型。
type FrameType uint8

const (
	TypeUnknown FrameType = 0
	TypeKey     FrameType = 1 // 关键帧，可独立解码
	TypeDelta   FrameType = 2 // 增量帧，依赖同一条关键帧链
)

// RawFrame 是采集端产出的原始帧，携带采集时间戳。
// Pix 为像素负载，管线在入队时会做一次拷贝，后续不再原地修改。
type RawFrame struct {
	SourceID string
	Seq      uint64    // 采集序号（单调递增，允许乱序到达）
	PTS      time.Time // 采集时间戳，输出时间轴的唯一基准
	Width    int
	Height   int
	Pix      []byte
}

// EncodedPacket 是编码后的不可变数据包，可被多个观看端安全共享。
type EncodedPacket struct {
	OutputSeq uint64    // 输出时间轴上的单调序号
	KeyID     uint64    // 本帧所属关键帧链的关键帧 OutputSeq
	Type      FrameType // 关键帧 / 增量帧
	SourceSeq uint64    // 来源原始帧的采集序号
	PTS       time.Time // 保留原始采集时间戳，绝不重写
	Width     int
	Height    int
	QualityLv int    // 编码时采用的质量等级
	Payload   []byte // 不可变负载，多观看端共享同一份底层数组
}

// DropReason 是可区分的丢帧/降级原因。
type DropReason string

const (
	DropNone          DropReason = ""
	DropDuplicate     DropReason = "duplicate"     // 重复采集帧
	DropLate          DropReason = "late"          // 超过重排窗口的迟到帧
	DropInputFull     DropReason = "input_full"    // 采集输入队列满
	DropOrderedFull   DropReason = "ordered_full"  // 排序后、编码前队列满
	DropEncodeFailure DropReason = "encode_failed" // 编码多次重试仍失败
	DropAdaptive      DropReason = "adaptive"      // 自适应降帧
	DropViewerSlow    DropReason = "viewer_slow"   // 慢观看端积压触发降级
	DropShutdown      DropReason = "shutdown"      // 管线关闭中
)

// FrameOutcome 是一帧在管线中的最终结论。
type FrameOutcome string

const (
	OutcomeAccepted FrameOutcome = "accepted"
	OutcomeDropped  FrameOutcome = "dropped"
)

// FrameEvent 记录一帧（按采集序号）的可解释去向，用于测试与排障。
type FrameEvent struct {
	SourceSeq    uint64
	PTS          time.Time
	ArrivalOrder uint64
	Outcome      FrameOutcome
	Drop         DropReason
	OutputSeq    uint64
	Reordered    bool // 是否经过重排等待
	QualityLv    int
	Detail       string
}

// SubmitResult 是采集端提交一帧后的即时结果。
type SubmitResult struct {
	Accepted bool
	Drop     DropReason
}
