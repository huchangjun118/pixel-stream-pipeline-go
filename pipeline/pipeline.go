package pipeline

import (
	"context"
	"sync"
	"time"
)

// nominalFrameInterval 是在尚未观测到两帧采集时间戳之前假定的采集间隔，
// 仅用于把延迟目标换算为队列帧预算，是固定常量，不影响可复现性。
const nominalFrameInterval = 16 * time.Millisecond

// encodeItem 携带进入编码队列的帧及其自适应决策结果。
type encodeItem struct {
	frame   Frame
	quality int
}

// Pipeline 串联 采集输入 -> 重排 -> 自适应节流 -> 编码 -> 扇出 的完整管线。
//
// 并发结构：ingest 与 encode 两个后台 goroutine 通过有界队列解耦；
// 观看端无论多慢都不会反向阻塞采集端（队列满即按策略丢弃）。
// Run 返回时两个 goroutine 均已退出、hub 已关闭，无泄漏。
type Pipeline struct {
	cfg   Config
	input chan Frame
	hub   *Hub
	m     *Metrics
}

// New 创建管线，配置非法时返回错误。
func New(cfg Config) (*Pipeline, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	p := &Pipeline{
		cfg:   cfg,
		input: make(chan Frame),
		m:     newMetrics(),
	}
	p.hub = NewHub(cfg.SubscriberQueueSize, func(subID int, ef EncodedFrame, r DropReason) {
		p.m.drop(r)
	})
	return p, nil
}

// Input 返回采集帧输入通道。关闭该通道表示采集源结束，管线排空后退出。
func (p *Pipeline) Input() chan<- Frame { return p.input }

// Subscribe 接入一个观看端，可在运行中随时调用（中途接入 / 重连）。
// 新端总是从最近一个关键帧开始，保证可独立解码。
func (p *Pipeline) Subscribe() *Subscriber { return p.hub.Subscribe() }

// Unsubscribe 断开一个观看端。
func (p *Pipeline) Unsubscribe(id int) { p.hub.Unsubscribe(id) }

// Run 启动管线后台任务，直到 ctx 取消或输入关闭且排空。
// 返回后保证不残留任何后台 goroutine 与未释放缓冲。
func (p *Pipeline) Run(ctx context.Context) error {
	seq := NewSequencer(p.cfg.ReorderWindow, func(f Frame, r DropReason) { p.m.drop(r) })
	enc := NewEncoder(p.cfg.KeyframeInterval, p.cfg.MaxQuality)
	ctrl := NewAdaptiveController(p.cfg)
	encodeQ := make(chan encodeItem, p.cfg.EncodeQueueSize)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		p.ingestLoop(ctx, seq, ctrl, encodeQ)
	}()
	go func() {
		defer wg.Done()
		p.encodeLoop(enc, encodeQ)
	}()
	wg.Wait()

	p.hub.Close()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

// ingestLoop 读取采集帧，重排、自适应节流后投入编码队列。
// 返回前关闭 encodeQ 通知编码端收尾。
func (p *Pipeline) ingestLoop(ctx context.Context, seq *Sequencer, ctrl *AdaptiveController, encodeQ chan encodeItem) {
	defer close(encodeQ)

	var (
		strideCount int
		lastCap     time.Time
		interval    = nominalFrameInterval
	)

	handle := func(f Frame) bool {
		p.m.FramesSequenced.Add(1)

		// 用采集时间戳估计帧间隔（确定性，不依赖墙钟），换算延迟预算。
		if !lastCap.IsZero() && f.CapturedAt.After(lastCap) {
			interval = f.CapturedAt.Sub(lastCap)
		}
		lastCap = f.CapturedAt
		budget := int(p.cfg.LatencyTarget / interval)
		if budget < 1 {
			budget = 1
		}

		quality, stride := ctrl.Sample(len(encodeQ), budget)
		strideCount++
		if stride > 1 && strideCount%stride != 0 {
			p.m.drop(DropAdaptiveThrottle)
			return true
		}

		item := encodeItem{frame: f, quality: quality}
		switch p.cfg.DropPolicy {
		case DropNewest:
			select {
			case encodeQ <- item:
			default:
				p.m.drop(DropEncodeQueueFull)
			}
		default: // DropOldest：顶掉最旧的帧，为新鲜帧让位
			select {
			case encodeQ <- item:
			default:
				select {
				case <-encodeQ: // 丢弃队首（最旧）
					p.m.drop(DropEncodeQueueFull)
				default:
				}
				select {
				case encodeQ <- item:
				default:
					p.m.drop(DropEncodeQueueFull)
				}
			}
		}
		return true
	}

	for {
		select {
		case <-ctx.Done():
			return
		case f, ok := <-p.input:
			if !ok {
				for _, of := range seq.Flush() {
					handle(of)
				}
				return
			}
			p.m.FramesIn.Add(1)
			for _, of := range seq.Push(f) {
				handle(of)
			}
		}
	}
}

// encodeLoop 串行编码并扇出。编码失败时跳过该帧并强制下一帧为关键帧，
// 保证观看端总能从下一个关键帧恢复，管线不进入半死状态。
func (p *Pipeline) encodeLoop(enc *Encoder, encodeQ <-chan encodeItem) {
	for item := range encodeQ {
		ef, err := enc.Encode(item.frame, item.quality)
		if err != nil {
			p.m.EncodeErrors.Add(1)
			p.m.drop(DropEncodeError)
			enc.ForceKeyframe()
			continue
		}
		p.m.FramesEncoded.Add(1)
		n := p.hub.Broadcast(ef)
		p.m.FramesBroadcast.Add(uint64(n))
	}
}

// Metrics 返回运行计数快照。
func (p *Pipeline) Metrics() MetricsSnapshot { return p.m.Snapshot() }
