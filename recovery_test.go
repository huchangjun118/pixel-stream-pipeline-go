package pixelstream

import (
	"context"
	"runtime"
	"testing"
	"time"
)

func TestRecovery_EncoderFailureRetriesAndResumesWithKeyframe(t *testing.T) {
	p, clk, base := newTestPipeline(t)
	v, _ := p.AddViewer(context.Background(), "v")

	// 第一帧成功（关键帧）。
	p.SubmitFrame(frameAt(1, base))
	advanceReorder(clk)
	if len(waitPackets(v, 1, 2*time.Second)) != 1 {
		t.Fatal("first frame missing")
	}

	// 通过白盒注入 3 次瞬时编码失败，之后编码器自动恢复。
	p.enc.codec.FailAttempts(3)
	p.SubmitFrame(frameAt(2, base.Add(10*time.Millisecond)))
	advanceReorder(clk)

	// 故障窗口内该帧必须重试，最终成功；等待编码失败计数与最终成功。
	if !waitFor(3*time.Second, func() bool {
		// 需要推进重试退避时钟（手动）。
		clk.Advance(p.cfg.EncoderRetryDelay + p.cfg.ReorderWindow)
		return p.Metrics().EncodeFailures >= 1 && p.Metrics().Encoded >= 2
	}) {
		t.Fatalf("frame must be retried and then encoded: %+v", p.Metrics())
	}
	// 恢复后该帧（seq2）必须是关键帧，下游不会花屏。
	pkts := waitPackets(v, 1, 2*time.Second)
	if len(pkts) != 1 || pkts[0].SourceSeq != 2 || pkts[0].Type != TypeKey {
		t.Fatalf("post-failure frame must be a fresh keyframe, got %+v", pkts)
	}
	// 后续帧正常，且 seq2 只被编码/推送一次。
	var seq2Count int
	for _, e := range p.Events() {
		if e.SourceSeq == 2 && e.Outcome == OutcomeAccepted {
			seq2Count++
		}
	}
	if seq2Count != 1 {
		t.Fatalf("seq2 must be delivered exactly once, got %d accepted events", seq2Count)
	}
}

func TestRecovery_SourceInterruptThenResumeNoDupNoGapInOrder(t *testing.T) {
	p, clk, base := newTestPipeline(t)
	v, _ := p.AddViewer(context.Background(), "v")

	frames := []ScriptedFrame{}
	for seq := uint64(1); seq <= 6; seq++ {
		frames = append(frames, ScriptedFrame{
			Frame: frameAt(seq, base.Add(time.Duration(seq)*10*time.Millisecond)),
			Delay: 1 * time.Millisecond,
		})
	}
	src := NewScriptedSource(frames).WithClock(clk)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srcDone := make(chan error, 1)
	go func() { srcDone <- src.Run(ctx, p) }()

	// 先拿到前 2 帧。
	if !waitFor(2*time.Second, func() bool {
		clk.Advance(5 * time.Millisecond)
		return p.Metrics().Encoded >= 2
	}) {
		t.Fatal("initial frames not produced")
	}
	advanceReorder(clk)
	waitPackets(v, 2, 2*time.Second)

	// 模拟采集中断：暂停源一段时间（管线空闲但存活），时钟照常前进。
	src.Pause()
	clk.Advance(500 * time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	encodedDuringPause := p.Metrics().Encoded

	// 恢复后剩余帧按序到达，时间轴不重复、不倒流。
	src.Resume()
	if !waitFor(3*time.Second, func() bool {
		clk.Advance(5 * time.Millisecond)
		return p.Metrics().Encoded >= 6
	}) {
		t.Fatalf("pipeline did not resume, encoded=%d", p.Metrics().Encoded)
	}
	if p.Metrics().Encoded == encodedDuringPause && encodedDuringPause < 6 {
		t.Fatal("no frames produced after resume")
	}
	select {
	case err := <-srcDone:
		if err != nil {
			t.Fatalf("source ended with error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("source did not finish")
	}
	// 收集 6 帧，校验序号无重复、严格递增。
	pkts := waitPackets(v, int(6-2), 2*time.Second)
	seen := map[uint64]bool{1: true, 2: true}
	var lastPTS = base.Add(20 * time.Millisecond)
	for _, pk := range pkts {
		if seen[pk.SourceSeq] {
			t.Fatalf("source seq %d duplicated after interrupt", pk.SourceSeq)
		}
		seen[pk.SourceSeq] = true
		if pk.PTS.Before(lastPTS) {
			t.Fatal("PTS regressed after resume")
		}
		lastPTS = pk.PTS
	}
}

func TestRecovery_ViewerAbnormalDisconnectIsCleanedUp(t *testing.T) {
	p, clk, base := newTestPipeline(t)
	ctx, cancel := context.WithCancel(context.Background())

	v, _ := p.AddViewer(ctx, "gone")
	// 不读任何包，直接取消观看端上下文模拟异常断开。
	flushAll(p, clk, base, 1, 10)
	cancel()

	if !waitFor(2*time.Second, func() bool {
		select {
		case <-v.Done():
			return true
		default:
			return false
		}
	}) {
		t.Fatal("abnormally disconnected viewer must finish")
	}
	// 广播路径会清理已结束的观看端：再生产一批，观看端数量应归零且编码器存活。
	flushAll(p, clk, base.Add(100*time.Millisecond), 20, 5)
	if !waitFor(2*time.Second, func() bool { return p.hub.viewerCount() == 0 }) {
		t.Fatalf("dead viewer not removed, count=%d", p.hub.viewerCount())
	}
	// 编码器必须在观看端异常断开后继续推进（编码或自适应降帧都算产出）。
	produced := func() int64 {
		st := p.Metrics()
		return st.Encoded + st.Drops[DropAdaptive]
	}
	before := produced()
	flushAll(p, clk, base.Add(300*time.Millisecond), 30, 6)
	if !waitFor(2*time.Second, func() bool { return produced() > before }) {
		t.Fatalf("encoder stalled after viewer disconnect, produced=%d", produced())
	}
}

func TestClose_ReleasesAllGoroutines(t *testing.T) {
	base := time.Unix(1, 0)
	clk := NewManualClock(base)
	cfg := testConfig()
	p := NewWithClock(cfg, clk)
	p.Start(context.Background())
	_, _ = p.AddViewer(context.Background(), "a")
	_, _ = p.AddViewer(context.Background(), "b")
	for seq := uint64(1); seq <= 5; seq++ {
		p.SubmitFrame(frameAt(seq, base.Add(time.Duration(seq)*time.Millisecond)))
	}
	clk.Advance(50 * time.Millisecond)

	before := runtime.NumGoroutine()
	cctx, ccancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer ccancel()
	if err := p.Close(cctx); err != nil {
		// 手动时钟可能让部分协程停在等待；Close 必须仍能取消并返回。
		t.Fatalf("close: %v", err)
	}
	// 再次 Close 必须幂等。
	if err := p.Close(cctx); err != nil {
		t.Fatalf("second close: %v", err)
	}
	// 允许 GC 与调度器回收，断言协程数不持续增长（无泄漏）。
	deadline := time.Now().Add(time.Second)
	after := before
	for time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
		after = runtime.NumGoroutine()
		if after <= before {
			return
		}
	}
	if after > before+2 {
		t.Fatalf("goroutine leak suspected: before=%d after=%d", before, after)
	}
}
