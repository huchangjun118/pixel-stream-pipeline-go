package pixelstream

import (
	"context"
	"time"
)

// encoderStage 串行驱动编码器，保证：
//   - 输出 OutputSeq 单调、PTS 严格递增（输入由 ingress 保证有序）；
//   - 中途接入/重连/PLI/链断裂时强制关键帧，恢复后不重复不漏推；
//   - 编码失败就地退避重试，不丢弃该帧、不广播半成品；
//   - 自适应只改质量等级或整帧丢弃，绝不重写时间戳。
type encoderStage struct {
	cfg     Config
	clock   Clock
	codec   *FakeCodec
	metrics *Metrics
	adapt   *AdaptController

	// gateBeforeFrame 若非 nil，编码每帧前等待它（仅测试注入，确定性节流）。
	gateBeforeFrame <-chan struct{}
}

func newEncoderStage(cfg Config, clock Clock, m *Metrics, adapt *AdaptController) *encoderStage {
	return &encoderStage{
		cfg:     cfg,
		clock:   clock,
		codec:   NewFakeCodec(0, 0, cfg.MaxQualityLevel),
		metrics: m,
		adapt:   adapt,
	}
}

func (e *encoderStage) setQuality(lv int) {
	lv = clampInt(lv, e.cfg.MinQualityLevel, e.cfg.MaxQualityLevel)
	e.codec.setQuality(lv)
	e.metrics.setQuality(lv)
}

// run 消费有序帧直到 in 关闭；每帧严格处理一次（成功才广播）。
func (e *encoderStage) run(ctx context.Context, in <-chan orderedFrame, store *packetStore, hb *hub) {
	adaptTick := e.clock.NewTicker(e.cfg.AdaptationPeriod)
	defer adaptTick.Stop()

	for {
		select {
		case of, ok := <-in:
			if !ok {
				return
			}
			if e.gateBeforeFrame != nil {
				select {
				case <-e.gateBeforeFrame:
				case <-ctx.Done():
					e.drainCancelled(in)
					return
				}
			}
			e.handleFrame(ctx, &of, len(in), store, hb)

		case <-adaptTick.C():
			e.adaptTickFire(in)

		case <-ctx.Done():
			e.drainCancelled(in)
			return
		}
	}
}

func (e *encoderStage) handleFrame(ctx context.Context, of *orderedFrame, _ int, store *packetStore, hb *hub) {
	// 自适应结论在帧到达 ingress 的瞬间已确定性地算出并随帧携带；编码器只执行。
	if of.adaptQuality > 0 && of.adaptQuality != e.codec.qualityLv {
		e.setQuality(of.adaptQuality)
	}
	if of.adaptSkip {
		// 自适应降帧：整帧跳过，输出时间轴只是"少一帧"，不跳时刻、不重写 PTS。
		e.metrics.recordDrop(DropAdaptive)
		e.metrics.recordEvent(FrameEvent{
			SourceSeq: of.frame.Seq,
			PTS:       of.frame.PTS,
			Outcome:   OutcomeDropped,
			Drop:      DropAdaptive,
			QualityLv: e.codec.qualityLv,
			Detail:    "frame-rate reduction",
		})
		return
	}
	e.encodeWithRetry(ctx, of, store, hb)
}

// encodeWithRetry 就地退避重试直到成功或 ctx 取消。
// 期间不取后续帧，天然保证帧序与"恰好一次"广播。
func (e *encoderStage) encodeWithRetry(ctx context.Context, of *orderedFrame, store *packetStore, hb *hub) {
	recovering := false
	for {
		forceKey := recovering || e.pliPending(hb) || e.adapt.keyframeDue(of.frame.PTS)
		p, err := e.codec.Encode(ctx, of.frame, forceKey)
		if err != nil {
			if ctx.Err() != nil {
				e.metrics.recordDrop(DropShutdown)
				e.recordDropEvent(of, DropShutdown)
				return
			}
			e.metrics.encodeFailures.Add(1)
			recovering = true
			t := e.clock.NewTimer(e.cfg.EncoderRetryDelay)
			select {
			case <-ctx.Done():
				t.Stop()
				e.metrics.recordDrop(DropShutdown)
				e.recordDropEvent(of, DropShutdown)
				return
			case <-t.C():
			}
			continue
		}
		if p.Type == TypeKey {
			e.adapt.noteKeyframe(p.PTS)
			e.metrics.keyFrames.Add(1)
		} else {
			e.metrics.deltaFrames.Add(1)
		}
		store.add(p)
		e.metrics.encoded.Add(1)
		e.metrics.accepted.Add(1)
		e.metrics.recordEvent(FrameEvent{
			SourceSeq: of.frame.Seq,
			PTS:       p.PTS,
			Outcome:   OutcomeAccepted,
			OutputSeq: p.OutputSeq,
			Reordered: of.reordered,
			QualityLv: p.QualityLv,
		})
		hb.broadcast(p)
		return
	}
}

func (e *encoderStage) recordDropEvent(of *orderedFrame, r DropReason) {
	e.metrics.recordEvent(FrameEvent{
		SourceSeq: of.frame.Seq,
		PTS:       of.frame.PTS,
		Outcome:   OutcomeDropped,
		Drop:      r,
	})
}

// drainCancelled 在关闭时登记剩余排队帧，避免它们被静默吞掉。
func (e *encoderStage) drainCancelled(in <-chan orderedFrame) {
	for of := range in {
		e.metrics.recordDrop(DropShutdown)
		e.recordDropEvent(&of, DropShutdown)
	}
}

func (e *encoderStage) pliPending(hb *hub) bool {
	select {
	case <-hb.pliRequests():
		return true
	default:
		return false
	}
}

// adaptTickFire 保留周期性钩子；当前过载/恢复完全由帧自带的"到达积压"
// 驱动，因此周期 tick 不做任何依赖墙钟的改动，避免破坏可复现性。
func (e *encoderStage) adaptTickFire(_ <-chan orderedFrame) {}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

var _ = time.Now
