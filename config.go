package pixelstream

import "time"

// DropPolicy 定义编码前队列积压时的丢帧策略（作用于排序后、编码前）。
type DropPolicy string

const (
	// DropPolicyNone 不主动丢帧：队列满时回压到采集端（SubmitFrame 返回 input_full）。
	DropPolicyNone DropPolicy = "none"
	// DropPolicyLatest 丢弃最新到达的帧，保留较旧帧。
	DropPolicyLatest DropPolicy = "latest"
	// DropPolicyOldest 丢弃最旧的可丢帧（优先增量帧，绝不丢未播放的关键帧链头），
	// 丢弃后等待下一个关键帧重新对齐，保证输出仍可解码。
	DropPolicyOldest DropPolicy = "oldest"
)

// ViewerOverflowPolicy 定义单个观看端队列溢出时的策略。
type ViewerOverflowPolicy string

const (
	// ViewerBlock 等待慢观看端（会反压编码器，仅调试用）。
	ViewerBlock ViewerOverflowPolicy = "block"
	// ViewerDropOldest 丢弃该观看端队列中最旧的增量帧以腾位；
	// 若丢到破坏关键帧链则退化为 resync。
	ViewerDropOldest ViewerOverflowPolicy = "drop_oldest"
	// ViewerResync 清空该观看端积压，从下一个关键帧重新对齐（默认）。
	ViewerResync ViewerOverflowPolicy = "resync"
)

// Config 是管线全部可调参数。零值字段会在 New 时替换为默认值。
type Config struct {
	// InputQueueDepth 采集输入通道深度；采集端提交只受此队列约束，
	// 与观看端速度完全无关。
	InputQueueDepth int
	// OrderedQueueDepth 排序后、编码前队列深度。自适应与丢帧均围绕它工作。
	OrderedQueueDepth int
	// KeyFrameInterval 关键帧最大间隔（按 PTS 时间）。间隔到达边界时
	// 下一帧强制编码为关键帧。
	KeyFrameInterval time.Duration
	// ReorderWindow 帧到达后为等待迟到帧而暂缓输出的时间窗口（按到达时钟）。
	ReorderWindow time.Duration
	// ReorderMaxFrames 重排缓冲区最大帧数，防止窗口内无限缓存。
	ReorderMaxFrames int
	// LateGrace PTS 落后已输出最大 PTS 超过该宽限即判迟到，直接丢弃。
	LateGrace time.Duration
	// LatencyTarget 端到端延迟目标：有序队列中最老帧年龄超过该目标时，
	// 自适应控制器开始降质量/降帧率。
	LatencyTarget time.Duration
	// AdaptationPeriod 自适应评估周期。
	AdaptationPeriod time.Duration
	// EncoderRetryDelay 编码失败后重试前的等待（手动时钟下可精确推进）。
	EncoderRetryDelay time.Duration
	// MaxQualityLevel/MinQualityLevel 质量等级范围，越大越好。
	MaxQualityLevel int
	MinQualityLevel int
	// StoreKeyFrames 在 store 中保留的关键帧链数量，决定重连能回溯多远。
	StoreKeyFrames int
	// ViewerQueueDepth 每个观看端独立队列深度。
	ViewerQueueDepth int
	// DropPolicy 编码前积压策略。
	DropPolicy DropPolicy
	// ViewerPolicy 观看端溢出策略。
	ViewerPolicy ViewerOverflowPolicy
}

// DefaultConfig 返回生产可用的默认配置。
func DefaultConfig() Config {
	return Config{
		InputQueueDepth:   64,
		OrderedQueueDepth: 8,
		KeyFrameInterval:  time.Second,
		ReorderWindow:     50 * time.Millisecond,
		ReorderMaxFrames:  64,
		LateGrace:         200 * time.Millisecond,
		LatencyTarget:     200 * time.Millisecond,
		AdaptationPeriod:  200 * time.Millisecond,
		EncoderRetryDelay: 10 * time.Millisecond,
		MaxQualityLevel:   5,
		MinQualityLevel:   1,
		StoreKeyFrames:    4,
		ViewerQueueDepth:  30,
		DropPolicy:        DropPolicyOldest,
		ViewerPolicy:      ViewerResync,
	}
}

// withDefaults 用默认值填充零值字段，并做基本归一化。
func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.InputQueueDepth <= 0 {
		c.InputQueueDepth = d.InputQueueDepth
	}
	if c.OrderedQueueDepth <= 0 {
		c.OrderedQueueDepth = d.OrderedQueueDepth
	}
	if c.KeyFrameInterval <= 0 {
		c.KeyFrameInterval = d.KeyFrameInterval
	}
	if c.ReorderWindow <= 0 {
		c.ReorderWindow = d.ReorderWindow
	}
	if c.ReorderMaxFrames <= 0 {
		c.ReorderMaxFrames = d.ReorderMaxFrames
	}
	if c.LateGrace <= 0 {
		c.LateGrace = d.LateGrace
	}
	if c.LatencyTarget <= 0 {
		c.LatencyTarget = d.LatencyTarget
	}
	if c.AdaptationPeriod <= 0 {
		c.AdaptationPeriod = d.AdaptationPeriod
	}
	if c.EncoderRetryDelay <= 0 {
		c.EncoderRetryDelay = d.EncoderRetryDelay
	}
	if c.MaxQualityLevel <= 0 {
		c.MaxQualityLevel = d.MaxQualityLevel
	}
	if c.MinQualityLevel < 0 {
		c.MinQualityLevel = d.MinQualityLevel
	}
	if c.MinQualityLevel >= c.MaxQualityLevel {
		c.MinQualityLevel = d.MinQualityLevel
	}
	if c.StoreKeyFrames <= 0 {
		c.StoreKeyFrames = d.StoreKeyFrames
	}
	if c.ViewerQueueDepth <= 0 {
		c.ViewerQueueDepth = d.ViewerQueueDepth
	}
	if c.DropPolicy == "" {
		c.DropPolicy = d.DropPolicy
	}
	if c.ViewerPolicy == "" {
		c.ViewerPolicy = d.ViewerPolicy
	}
	return c
}
