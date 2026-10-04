package pipeline

import (
	"fmt"
	"time"
)

// DropPolicy 决定编码队列满时的丢弃策略。
type DropPolicy int

const (
	DropOldest DropPolicy = iota // 丢弃队列中最旧的帧（保新鲜度，低延迟优先）
	DropNewest                   // 丢弃新到的帧（保连续性）
)

func (p DropPolicy) String() string {
	if p == DropNewest {
		return "drop_newest"
	}
	return "drop_oldest"
}

// Config 是管线的全部可调参数。
type Config struct {
	ReorderWindow       int           // 乱序重排窗口（帧数），窗口内按序号排序输出
	EncodeQueueSize     int           // 编码队列容量，满时按 DropPolicy 丢弃
	SubscriberQueueSize int           // 每个观看端的队列容量
	KeyframeInterval    int           // 关键帧间隔（帧数）
	DropPolicy          DropPolicy    // 编码队列满时的丢弃策略
	MinQuality          int           // 自适应质量下限（0 最差）
	MaxQuality          int           // 自适应质量上限
	InitialQuality      int           // 初始质量
	LatencyTarget       time.Duration // 端到端延迟目标，自适应控制的依据
	AdaptiveEnabled     bool          // 是否启用自适应调节
}

// DefaultConfig 返回一组合理的默认配置。
func DefaultConfig() Config {
	return Config{
		ReorderWindow:       8,
		EncodeQueueSize:     16,
		SubscriberQueueSize: 8,
		KeyframeInterval:    30,
		DropPolicy:          DropOldest,
		MinQuality:          0,
		MaxQuality:          4,
		InitialQuality:      4,
		LatencyTarget:       200 * time.Millisecond,
		AdaptiveEnabled:     true,
	}
}

// Validate 校验配置合法性，返回第一个发现的错误。
func (c Config) Validate() error {
	if c.ReorderWindow < 0 {
		return fmt.Errorf("ReorderWindow must be >= 0, got %d", c.ReorderWindow)
	}
	if c.EncodeQueueSize <= 0 {
		return fmt.Errorf("EncodeQueueSize must be > 0, got %d", c.EncodeQueueSize)
	}
	if c.SubscriberQueueSize <= 0 {
		return fmt.Errorf("SubscriberQueueSize must be > 0, got %d", c.SubscriberQueueSize)
	}
	if c.KeyframeInterval <= 0 {
		return fmt.Errorf("KeyframeInterval must be > 0, got %d", c.KeyframeInterval)
	}
	if c.DropPolicy != DropOldest && c.DropPolicy != DropNewest {
		return fmt.Errorf("invalid DropPolicy %d", c.DropPolicy)
	}
	if c.MinQuality < 0 {
		return fmt.Errorf("MinQuality must be >= 0, got %d", c.MinQuality)
	}
	if c.MaxQuality < c.MinQuality {
		return fmt.Errorf("MaxQuality (%d) < MinQuality (%d)", c.MaxQuality, c.MinQuality)
	}
	if c.InitialQuality < c.MinQuality || c.InitialQuality > c.MaxQuality {
		return fmt.Errorf("InitialQuality (%d) out of [%d, %d]", c.InitialQuality, c.MinQuality, c.MaxQuality)
	}
	if c.LatencyTarget <= 0 {
		return fmt.Errorf("LatencyTarget must be > 0, got %v", c.LatencyTarget)
	}
	return nil
}
