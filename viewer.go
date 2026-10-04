package pixelstream

import (
	"context"
	"sync"
)

// Viewer 是一个观看端，由两条解耦的路径构成：
//   - deliver 把编码包放入内部有界队列（永不阻塞编码器）；
//   - 独立 pusher 协程把队列搬到 Packets() 通道，速度由观看端读取决定。
//
// 慢观看端只会填满自己的内部队列并按策略丢弃/重对齐，不影响任何其他观看端。
type Viewer struct {
	id      string
	cfg     Config
	metrics *Metrics
	ctx     context.Context

	ch   chan EncodedPacket // Packets() 返回给观看端的通道（有缓冲）
	done chan struct{}
	stop chan struct{} // close() 时关闭，强制 pusher 退出

	mu      sync.Mutex
	cond    *sync.Cond
	queue   []EncodedPacket
	backlog []EncodedPacket // 接入/重连时的关键帧链历史，优先整段投递
	resync  bool            // 为 true 时丢弃增量帧，直到下一个关键帧
	closed  bool
	onClose func(id string) // ctx 结束时由看门狗回调（hub 用它移除自己）

	wg       sync.WaitGroup
	chMu     sync.Mutex
	chClosed bool
}

func newViewer(id string, cfg Config, m *Metrics, ctx context.Context) *Viewer {
	v := &Viewer{
		id:      id,
		cfg:     cfg,
		metrics: m,
		ctx:     ctx,
		ch:      make(chan EncodedPacket, cfg.ViewerQueueDepth),
		done:    make(chan struct{}),
		stop:    make(chan struct{}),
	}
	v.cond = sync.NewCond(&v.mu)
	return v
}

func (v *Viewer) start() {
	v.wg.Add(1)
	go v.loop()
	// 看门狗：观看端 ctx 取消（异常断开）时终止 pusher 并从 hub 注销，
	// 即使之后没有新的广播触发懒清理，也不会遗留协程或缓冲。
	go func() {
		select {
		case <-v.ctx.Done():
			v.terminate()
			if v.onClose != nil {
				v.onClose(v.id)
			}
		case <-v.stop:
		}
	}()
}

// terminate 幂等地终止 pusher（置关闭态并关闭 stop），但不关闭输出通道。
func (v *Viewer) terminate() {
	v.mu.Lock()
	already := v.closed
	if !already {
		v.closed = true
		v.queue = nil
		v.backlog = nil
		v.cond.Broadcast()
	}
	v.mu.Unlock()
	if !already {
		close(v.stop)
	}
	v.wg.Wait() // 等待 pusher 退出，保证无在途发送
}

func (v *Viewer) ID() string                    { return v.id }
func (v *Viewer) Packets() <-chan EncodedPacket { return v.ch }
func (v *Viewer) Done() <-chan struct{}         { return v.done }

// deliver 由 hub 扇出时调用，必须永不阻塞。
// 唯一例外是 ViewerPolicy=block（调试）：此时允许反压，等待队列空位。
func (v *Viewer) deliver(p EncodedPacket) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed || v.ctx.Err() != nil {
		return false
	}
	if v.cfg.ViewerPolicy == ViewerBlock {
		for len(v.queue) >= v.cfg.ViewerQueueDepth && !v.closed && v.ctx.Err() == nil {
			v.cond.Wait()
		}
		if v.closed || v.ctx.Err() != nil {
			return false
		}
	}
	v.enqueueLocked(p)
	v.cond.Signal()
	return true
}

// enqueueLocked 在持锁状态下把包放入队列，按溢出策略处理。
func (v *Viewer) enqueueLocked(p EncodedPacket) {
	// 重对齐中：只接受关键帧，其余增量帧一律跳过。
	if v.resync {
		if p.Type != TypeKey {
			return
		}
		v.resync = false
		v.queue = v.queue[:0]
	}
	if len(v.queue) < v.cfg.ViewerQueueDepth {
		v.queue = append(v.queue, p)
		return
	}
	switch v.cfg.ViewerPolicy {
	case ViewerDropOldest:
		v.dropOldestLocked(p)
	case ViewerResync:
		fallthrough
	default:
		v.queue = v.queue[:0]
		v.resync = true
		v.metrics.resyncs.Add(1)
		v.metrics.recordDrop(DropViewerSlow)
		if p.Type == TypeKey {
			v.resync = false
			v.queue = append(v.queue, p)
		}
	}
}

// dropOldestLocked 尝试丢弃不破坏关键帧链的最旧增量帧；
// 若队列最旧包就是链头关键帧（丢弃即断链），退化为重对齐，绝不花屏。
func (v *Viewer) dropOldestLocked(p EncodedPacket) {
	idx := -1
	headKey := v.queue[0].KeyID
	for i, q := range v.queue {
		if q.Type == TypeDelta && q.KeyID == headKey {
			idx = i
			break
		}
	}
	if idx < 0 {
		v.queue = v.queue[:0]
		v.resync = true
		v.metrics.resyncs.Add(1)
		v.metrics.recordDrop(DropViewerSlow)
		if p.Type == TypeKey {
			v.resync = false
			v.queue = append(v.queue, p)
		}
		return
	}
	v.metrics.recordDrop(DropViewerSlow)
	v.queue = append(v.queue[:idx], v.queue[idx+1:]...)
	v.queue = append(v.queue, p)
}

// seedBacklog 在 start 前一次性写入关键帧链历史（整段保留，不受实时
// 队列容量限制）。这是观看端"从可独立解码位置开始"的保证。
func (v *Viewer) seedBacklog(pkts []EncodedPacket) {
	v.mu.Lock()
	v.backlog = append(v.backlog, pkts...)
	v.mu.Unlock()
}

// requestResync 主动请求从下一个关键帧重新对齐（重连/画面异常恢复）。
func (v *Viewer) requestResync() {
	v.mu.Lock()
	v.queue = v.queue[:0]
	v.resync = true
	v.metrics.resyncs.Add(1)
	v.cond.Broadcast()
	v.mu.Unlock()
}

// loop 是唯一从内部队列搬到输出通道的协程。
// 它可能因观看端慢读而阻塞，但绝不持有锁、也不影响 deliver 的快速返回。
func (v *Viewer) loop() {
	defer v.wg.Done()
	defer close(v.done)
	for {
		v.mu.Lock()
		for len(v.backlog) == 0 && len(v.queue) == 0 && !v.closed && v.ctx.Err() == nil {
			v.cond.Wait()
		}
		// 优先整段发送历史 backlog，再发实时队列，顺序不变。
		var p EncodedPacket
		if len(v.backlog) > 0 {
			p = v.backlog[0]
			v.backlog = v.backlog[1:]
		} else if len(v.queue) > 0 {
			p = v.queue[0]
			v.queue = v.queue[1:]
			v.cond.Signal()
		} else {
			v.mu.Unlock()
			return
		}
		v.mu.Unlock()

		select {
		case v.ch <- p:
		case <-v.ctx.Done():
			return
		case <-v.stop:
			return
		}
	}
}

// closedCh 保证 ch 只被关闭一次。
// close 幂等关闭：终止 pusher、关闭输出通道并等待退出。
func (v *Viewer) close() {
	v.chMu.Lock()
	if v.chClosed {
		v.chMu.Unlock()
		v.terminate()
		return
	}
	v.chClosed = true
	v.chMu.Unlock()
	v.terminate()
	close(v.ch)
}
