package pixelstream

import (
	"container/heap"
	"context"
	"time"
)

// orderedFrame 是通过排序去重后交给编码器的帧。
type orderedFrame struct {
	frame        RawFrame
	arrivalOrder uint64 // 到达管线的次序
	reordered    bool   // 是否经过跨时刻等待（非本次到达立即输出）
	// 自适应结论在帧"到达排序阶段的那一刻"由单一协程算出并随帧携带，
	// 只依赖到达次序（当时堆内积压帧数），与后续编码/调度无关。
	adaptQuality int
	adaptSkip    bool
}

// reorderEntry 是重排序缓冲中的一帧。
type reorderEntry struct {
	frame        RawFrame
	arrivalOrder uint64
	deadline     time.Time // 到达时刻 + ReorderWindow，之后允许按 PTS 输出
	adaptQuality int
	adaptSkip    bool
}

// frameDecision 是帧到达瞬间确定的自适应结论。
type frameDecision struct {
	quality int
	skip    bool
}

// reorderBuffer 是单协程使用的纯逻辑：去重、迟到判定、按 PTS 排序。
// 不变量：取出的帧 PTS 严格递增（PTS 相同按 SourceSeq 升序），永不倒流。
type reorderBuffer struct {
	cfg Config

	h      reorderHeap
	seen   map[uint64]time.Time // SourceSeq -> PTS，用于去重（含已输出帧）
	inHeap map[uint64]bool      // 仍在重排堆中的帧
	maxPTS time.Time
	hasOut bool
}

func newReorderBuffer(cfg Config) *reorderBuffer {
	rb := &reorderBuffer{
		cfg:    cfg,
		seen:   make(map[uint64]time.Time),
		inHeap: make(map[uint64]bool),
	}
	heap.Init(&rb.h)
	return rb
}

// push 尝试接纳一帧，返回丢弃原因（DropNone 表示已进入缓冲）。
func (rb *reorderBuffer) push(f RawFrame, arrival uint64, now time.Time, dec frameDecision) DropReason {
	// 1) 重复帧：相同采集序号一定是重复，直接拒绝（不影响时间轴）。
	if _, dup := rb.seen[f.Seq]; dup {
		return DropDuplicate
	}
	// 2) 迟到帧：PTS 已落在已输出时间轴之后（含宽限），拒绝。
	if rb.hasOut && f.PTS.Add(rb.cfg.LateGrace).Before(rb.maxPTS) {
		return DropLate
	}
	rb.seen[f.Seq] = f.PTS
	rb.inHeap[f.Seq] = true
	heap.Push(&rb.h, reorderEntry{
		frame:        f,
		arrivalOrder: arrival,
		deadline:     now.Add(rb.cfg.ReorderWindow),
		adaptQuality: dec.quality,
		adaptSkip:    dec.skip,
	})
	return DropNone
}

func (rb *reorderBuffer) len() int { return rb.h.Len() }

// popDue 弹出一个可输出帧：到期或缓冲超限。空缓冲返回 ok=false。
func (rb *reorderBuffer) popDue(now time.Time, flushArrival uint64) (orderedFrame, bool) {
	if rb.h.Len() == 0 {
		return orderedFrame{}, false
	}
	top := &rb.h[0]
	due := !top.deadline.After(now)
	overCap := rb.h.Len() > rb.cfg.ReorderMaxFrames
	if !due && !overCap {
		return orderedFrame{}, false
	}
	e := heap.Pop(&rb.h).(reorderEntry)
	rb.maxPTS = e.frame.PTS
	rb.hasOut = true
	delete(rb.inHeap, e.frame.Seq)
	rb.pruneSeen()
	return orderedFrame{
		frame:        e.frame,
		arrivalOrder: e.arrivalOrder,
		reordered:    e.arrivalOrder != flushArrival,
		adaptQuality: e.adaptQuality,
		adaptSkip:    e.adaptSkip,
	}, true
}

// popOldest 强制弹出最旧帧（关闭排空用）。
func (rb *reorderBuffer) popOldest() (orderedFrame, bool) {
	if rb.h.Len() == 0 {
		return orderedFrame{}, false
	}
	e := heap.Pop(&rb.h).(reorderEntry)
	rb.maxPTS = e.frame.PTS
	rb.hasOut = true
	delete(rb.inHeap, e.frame.Seq)
	rb.pruneSeen()
	return orderedFrame{
		frame:        e.frame,
		arrivalOrder: e.arrivalOrder,
		reordered:    true,
		adaptQuality: e.adaptQuality,
		adaptSkip:    e.adaptSkip,
	}, true
}

// popNewest 弹出缓冲中最新的帧（DropPolicyLatest 用）。
func (rb *reorderBuffer) popNewest() (RawFrame, bool) {
	if rb.h.Len() == 0 {
		return RawFrame{}, false
	}
	idx := 0
	for i := 1; i < rb.h.Len(); i++ {
		if rb.h[i].frame.PTS.After(rb.h[idx].frame.PTS) ||
			(rb.h[i].frame.PTS.Equal(rb.h[idx].frame.PTS) && rb.h[i].frame.Seq > rb.h[idx].frame.Seq) {
			idx = i
		}
	}
	e := heap.Remove(&rb.h, idx).(reorderEntry)
	delete(rb.inHeap, e.frame.Seq)
	return e.frame, true
}

// pruneSeen 限制去重表大小：清理既不在堆中、又比堆中最旧帧更老的记录。
// 在窗帧与近期输出帧都保留，避免误收重复帧，同时防止长跑内存增长。
func (rb *reorderBuffer) pruneSeen() {
	limit := rb.cfg.ReorderMaxFrames*2 + 64
	if len(rb.seen) <= limit || rb.h.Len() == 0 {
		return
	}
	oldest := rb.h[0].frame.PTS
	for seq, pts := range rb.seen {
		if rb.inHeap[seq] {
			continue
		}
		if pts.Before(oldest) {
			delete(rb.seen, seq)
		}
	}
}

// reorderHeap 按 (PTS, SourceSeq) 升序的最小堆。
type reorderHeap []reorderEntry

func (h reorderHeap) Len() int { return len(h) }
func (h reorderHeap) Less(i, j int) bool {
	if h[i].frame.PTS.Equal(h[j].frame.PTS) {
		return h[i].frame.Seq < h[j].frame.Seq
	}
	return h[i].frame.PTS.Before(h[j].frame.PTS)
}
func (h reorderHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *reorderHeap) Push(x any)   { *h = append(*h, x.(reorderEntry)) }
func (h *reorderHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// ingress 驱动重排缓冲与编码前有界通道，执行编码前丢帧策略，
// 并在帧到达瞬间做出确定性的自适应结论。
type ingress struct {
	cfg     Config
	clock   Clock
	metrics *Metrics
	rb      *reorderBuffer
	arrival uint64
	adapt   *AdaptController
}

func newIngress(cfg Config, clock Clock, m *Metrics, adapt *AdaptController) *ingress {
	return &ingress{
		cfg: cfg, clock: clock, metrics: m,
		rb: newReorderBuffer(cfg), adapt: adapt,
	}
}

// run 消费输入帧，向 out 输出严格按 PTS 递增的有序帧，返回后关闭 out。
func (g *ingress) run(ctx context.Context, in <-chan RawFrame, out chan<- orderedFrame) {
	defer close(out)
	tick := g.clock.NewTicker(g.cfg.ReorderWindow)
	defer tick.Stop()

	for {
		select {
		case f, ok := <-in:
			if !ok {
				g.drain(out)
				return
			}
			g.arrival++
			arr := g.arrival
			now := g.clock.Now()
			dec := frameDecision{}
			if g.adapt != nil {
				// 唯一过载信号：本帧到达时堆中已积压的帧数（纯到达次序）。
				d := g.adapt.Decide(AdaptInput{
					BacklogAtArrival: g.rb.len(),
					PTS:              f.PTS,
				})
				dec.quality, dec.skip = d.QualityLevel, d.SkipFrame
			}
			if reason := g.rb.push(f, arr, now, dec); reason != DropNone {
				g.drop(f, reason, "")
				continue
			}
			if g.flush(now, arr, out, ctx) {
				g.drain(out)
				return
			}
		case <-tick.C():
			if g.flush(g.clock.Now(), g.arrival, out, ctx) {
				g.drain(out)
				return
			}
		case <-ctx.Done():
			g.abort(out)
			return
		}
	}
}

// flush 输出全部到期/超限帧。返回 true 表示 ctx 取消，调用方应转 drain。
func (g *ingress) flush(now time.Time, arr uint64, out chan<- orderedFrame, ctx context.Context) bool {
	for {
		of, ok := g.rb.popDue(now, arr)
		if !ok {
			return false
		}
		if stop := g.sendOne(of, out, ctx); stop {
			// 当前帧已被 popDue 弹出：先补送它再排空，避免丢失。
			out <- of
			g.drain(out)
			return true
		}
	}
}

// drain 在输入关闭路径上把剩余帧全部有序送出（不丢帧、不阻塞读输入）。
func (g *ingress) drain(out chan<- orderedFrame) {
	for {
		of, ok := g.rb.popOldest()
		if !ok {
			return
		}
		out <- of
	}
}

// abort 在 ctx 取消后非阻塞地放弃剩余帧（按 shutdown 记账），绝不等待
// 已停止消费的编码通道，避免协程泄漏。
func (g *ingress) abort(out chan<- orderedFrame) {
	for {
		of, ok := g.rb.popOldest()
		if !ok {
			return
		}
		select {
		case out <- of:
		default:
			g.drop(of.frame, DropShutdown, "cancel-drain-full")
			return
		}
	}
}

// sendOne 按策略把一帧送入编码通道。返回 true 表示因 ctx 取消需排空。
func (g *ingress) sendOne(of orderedFrame, out chan<- orderedFrame, ctx context.Context) bool {
	if g.cfg.DropPolicy == DropPolicyNone {
		select {
		case out <- of:
			return false
		case <-ctx.Done():
			return true
		}
	}
	// 有丢帧策略：永不阻塞。
	select {
	case out <- of:
		return false
	default:
	}
	switch g.cfg.DropPolicy {
	case DropPolicyLatest:
		if f, popped := g.rb.popNewest(); popped {
			g.drop(f, DropOrderedFull, "drop-policy=latest")
			return g.sendOneRetry(of, out, 1)
		}
		g.drop(of.frame, DropOrderedFull, "drop-policy=latest-solo")
		return false
	case DropPolicyOldest:
		// 当前最旧帧已弹出，直接丢弃以保新帧。
		g.drop(of.frame, DropOrderedFull, "drop-policy=oldest")
		return false
	default:
		g.drop(of.frame, DropOrderedFull, "unknown-policy")
		return false
	}
}

func (g *ingress) sendOneRetry(of orderedFrame, out chan<- orderedFrame, tries int) bool {
	const maxTries = 1024
	select {
	case out <- of:
		return false
	default:
	}
	if tries >= maxTries || g.rb.len() == 0 {
		g.drop(of.frame, DropOrderedFull, "drop-policy=latest-exhausted")
		return false
	}
	if f, popped := g.rb.popNewest(); popped {
		g.drop(f, DropOrderedFull, "drop-policy=latest-retry")
		return g.sendOneRetry(of, out, tries+1)
	}
	g.drop(of.frame, DropOrderedFull, "drop-policy=latest-exhausted")
	return false
}

func (g *ingress) drop(f RawFrame, reason DropReason, detail string) {
	g.metrics.recordDrop(reason)
	g.metrics.recordEvent(FrameEvent{
		SourceSeq: f.Seq,
		PTS:       f.PTS,
		Outcome:   OutcomeDropped,
		Drop:      reason,
		Detail:    detail,
	})
}
