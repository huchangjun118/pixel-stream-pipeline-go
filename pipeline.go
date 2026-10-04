package pixelstream

import (
	"context"
	"sync"
)

// Pipeline 是 采集 → 排序去重 → 编码 → 多观看端扇出 的完整管线。
type Pipeline struct {
	cfg     Config
	clock   Clock
	metrics *Metrics

	rb    *reorderBuffer
	ing   *ingress
	store *packetStore
	hub   *hub
	adapt *AdaptController
	enc   *encoderStage

	rawCh   chan RawFrame // SubmitFrame 写入，只由 forwarder 读
	input   chan RawFrame // forwarder 输出，供 ingress 消费
	ordered chan orderedFrame

	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once

	mu     sync.Mutex
	closed bool
}

// New 用默认系统时钟构造管线。
func New(cfg Config) *Pipeline {
	return NewWithClock(cfg.withDefaults(), SystemClock{})
}

// NewWithClock 用可注入时钟构造管线（测试用手动时钟，保证可复现）。
// 调用方需自行传入已归一化的配置；这里仍会再做一次默认值填充。
func NewWithClock(cfg Config, clock Clock) *Pipeline {
	cfg = cfg.withDefaults()
	m := newMetrics()
	rb := newReorderBuffer(cfg)
	store := newPacketStore(cfg)
	hb := newHub(cfg, m)
	adapt := NewAdaptController(cfg)
	ing := &ingress{cfg: cfg, clock: clock, metrics: m, rb: rb, adapt: adapt}
	enc := newEncoderStage(cfg, clock, m, adapt)
	p := &Pipeline{
		cfg: cfg, clock: clock, metrics: m,
		rb: rb, ing: ing, store: store, hub: hb, adapt: adapt, enc: enc,
		rawCh:   make(chan RawFrame, cfg.InputQueueDepth),
		input:   make(chan RawFrame, cfg.InputQueueDepth),
		ordered: make(chan orderedFrame, cfg.OrderedQueueDepth),
	}
	return p
}

// Start 启动全部后台任务。重复 Start 返回 nil（幂等）。
func (p *Pipeline) Start(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	// 1) 输入搬运：SubmitFrame 只写 rawCh，forwarder 是唯一关闭 input
	//    的地方。ctx 取消后停止转发，ingress/encoder 随之有序收尾。
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer close(p.input)
		for {
			select {
			case <-runCtx.Done():
				return
			case f, ok := <-p.rawCh:
				if !ok {
					return
				}
				select {
				case p.input <- f:
				case <-runCtx.Done():
					return
				}
			}
		}
	}()

	// 2) 排序去重阶段。
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.ing.run(runCtx, p.input, p.ordered)
	}()

	// 3) 编码与扇出阶段。
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.enc.run(runCtx, p.ordered, p.store, p.hub)
	}()
	return nil
}

// SubmitFrame 向管线提交一帧。
// 永不被慢速观看端阻塞：仅当输入队列满时才会返回 DropInputFull。
// 像素负载在此处拷贝，调用方之后可自由复用原缓冲。
func (p *Pipeline) SubmitFrame(f RawFrame) SubmitResult {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		p.metrics.recordDrop(DropShutdown)
		return SubmitResult{Accepted: false, Drop: DropShutdown}
	}
	p.mu.Unlock()

	p.metrics.submitted.Add(1)
	f.Pix = cloneBytes(f.Pix)
	select {
	case p.rawCh <- f:
		return SubmitResult{Accepted: true}
	default:
		p.metrics.recordDrop(DropInputFull)
		p.metrics.recordEvent(FrameEvent{
			SourceSeq: f.Seq,
			PTS:       f.PTS,
			Outcome:   OutcomeDropped,
			Drop:      DropInputFull,
		})
		return SubmitResult{Accepted: false, Drop: DropInputFull}
	}
}

// Emit 实现 FrameEmitter，供 FrameSource 直接向管线交付帧。
func (p *Pipeline) Emit(f RawFrame) SubmitResult { return p.SubmitFrame(f) }

// AddViewer 接入一个观看端，从最近关键帧开始播放（中途接入安全）。
func (p *Pipeline) AddViewer(ctx context.Context, id string) (*Viewer, error) {
	return p.hub.Add(ctx, id, p.store), nil
}

// RemoveViewer 主动断开观看端并释放其资源。
func (p *Pipeline) RemoveViewer(id string) { p.hub.remove(id) }

// RequestResync 请求某观看端从下一个关键帧重新对齐（重连恢复）。
func (p *Pipeline) RequestResync(id string) {
	if v, ok := p.hub.get(id); ok {
		v.requestResync()
		p.hub.requestPLI()
	}
}

// RequestKeyframe 请求编码器尽快产出关键帧（多观看端请求会被合并）。
func (p *Pipeline) RequestKeyframe() { p.hub.requestPLI() }

// Metrics 返回指标快照。
func (p *Pipeline) Metrics() MetricsSnapshot { return p.metrics.Snapshot() }

// Events 返回有序的帧去向事件（可解释每帧的输出顺序与丢弃原因）。
func (p *Pipeline) Events() []FrameEvent { return p.metrics.Events() }

// Close 优雅关闭：取消后台任务，等待在途帧处理或 ctx 超时，
// 关闭全部观看端，不遗留协程或缓冲。可重复调用。
func (p *Pipeline) Close(ctx context.Context) error {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.mu.Unlock()
		if p.cancel != nil {
			p.cancel()
		}
	})
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		p.hub.closeAll()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
