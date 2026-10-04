package pixelstream

import (
	"context"
	"testing"
	"time"
)

func adaptTestConfig() Config {
	c := DefaultConfig()
	c.ReorderWindow = time.Hour // 帧留在重排序堆中堆积（仅超容量时放行最旧帧）
	c.KeyFrameInterval = time.Hour
	c.AdaptationPeriod = time.Hour
	c.EncoderRetryDelay = 5 * time.Millisecond
	c.MaxQualityLevel = 5
	c.MinQualityLevel = 1
	c.InputQueueDepth = 256
	c.OrderedQueueDepth = 256
	c.ReorderMaxFrames = 4 // 小容量：第 5 个突发帧到达即重负载，堆持续积压
	c.ViewerQueueDepth = 256
	c.StoreKeyFrames = 32
	c.DropPolicy = DropPolicyNone
	return c
}

func acceptedQualities(p *Pipeline) []int {
	var qs []int
	for _, e := range p.Events() {
		if e.Outcome == OutcomeAccepted {
			qs = append(qs, e.QualityLv)
		}
	}
	return qs
}

// runAdaptScenario 完全停住编码器，先突发灌入 24 帧制造确定的"到达积压"，
// 再放行让其消费。靠后帧到达时前方已排多帧（与调度无关的纯到达次序信号），
// 自适应必然降质并在重负载阶段降帧。
func runAdaptScenario(t *testing.T) ([]int, *Pipeline) {
	t.Helper()
	base := time.Unix(1000, 0)
	clk := NewManualClock(base)
	p := NewWithClock(adaptTestConfig(), clk)
	gate := make(chan struct{})
	p.setEncodeGate(gate)
	p.Start(context.Background())
	t.Cleanup(func() { p.Close(context.Background()) })
	_, _ = p.AddViewer(context.Background(), "v")

	// 编码器完全不消费：突发提交 + 极小排序窗口，帧在有序通道排队。
	for seq := uint64(1); seq <= 24; seq++ {
		p.SubmitFrame(frameAt(seq, base.Add(time.Duration(seq)*time.Millisecond)))
	}
	// 排序窗口设为 1h：突发期间帧在堆内堆积（仅超容量放行最旧帧），
	// 使后到帧到达时前方积压确定地达到重负载阈值。
	time.Sleep(20 * time.Millisecond)
	// 提交完成后一次性跨过排序窗口，把残余堆帧放行（手动时钟无真实耗时）。
	clk.Advance(time.Hour + time.Millisecond)

	// 放行编码器处理全部帧。
	go func() {
		for i := 0; i < 24; i++ {
			gate <- struct{}{}
		}
	}()
	waitFor(3*time.Second, func() bool {
		st := p.Metrics()
		return st.Encoded+st.Drops[DropAdaptive] >= 24
	})
	return acceptedQualities(p), p
}

func TestAdaptation_ReducesQualityUnderBacklog(t *testing.T) {
	qs, _ := runAdaptScenario(t)
	if len(qs) == 0 {
		t.Fatal("no frames encoded")
	}
	minQ := qs[0]
	sawReduction := false
	for _, q := range qs {
		if q < minQ {
			sawReduction = true
			minQ = q
		}
	}
	if !sawReduction {
		t.Fatalf("expected quality reduction under backlog, qualities=%v", qs)
	}
}

func TestAdaptation_HeavyBacklogSkipsFrames(t *testing.T) {
	_, p := runAdaptScenario(t)
	if p.Metrics().Drops[DropAdaptive] == 0 {
		t.Fatalf("expected deterministic frame skipping at heavy backlog, drops=%v",
			p.Metrics().Drops)
	}
}

// 可复现性在纯控制器层严格保证：固定的到达积压序列必得逐帧一致的决策。
func TestAdaptation_ReproducibleSameInput(t *testing.T) {
	depths := []int{0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 0, 0, 0}
	run := func() (qs []int, skips []bool) {
		cfg := DefaultConfig()
		cfg.ReorderMaxFrames = 4
		a := NewAdaptController(cfg)
		for _, d := range depths {
			dec := a.Decide(AdaptInput{BacklogAtArrival: d, CurrentQuality: 5})
			qs = append(qs, dec.QualityLevel)
			skips = append(skips, dec.SkipFrame)
		}
		return
	}
	q1, s1 := run()
	q2, s2 := run()
	for i := range q1 {
		if q1[i] != q2[i] || s1[i] != s2[i] {
			t.Fatalf("non-reproducible decision at frame %d: (%d,%v) vs (%d,%v)\n%v\n%v",
				i, q1[i], s1[i], q2[i], s2[i], q1, q2)
		}
	}
}

func TestAdaptation_TimelineNeverJumps(t *testing.T) {
	base := time.Unix(1000, 0)
	clk := NewManualClock(base)
	p := NewWithClock(adaptTestConfig(), clk)
	gate := make(chan struct{})
	p.setEncodeGate(gate)
	p.Start(context.Background())
	defer p.Close(context.Background())
	v, _ := p.AddViewer(context.Background(), "v")

	for seq := uint64(1); seq <= 24; seq++ {
		p.SubmitFrame(frameAt(seq, base.Add(time.Duration(seq)*time.Millisecond)))
	}
	time.Sleep(20 * time.Millisecond)
	clk.Advance(time.Hour + time.Millisecond)
	go func() {
		for i := 0; i < 24; i++ {
			gate <- struct{}{}
		}
	}()
	waitFor(3*time.Second, func() bool {
		st := p.Metrics()
		return st.Encoded+st.Drops[DropAdaptive] >= 24
	})
	n := int(p.Metrics().Encoded)
	pkts := waitPackets(v, n, time.Second)
	if len(pkts) != n {
		t.Fatalf("want %d packets got %d", n, len(pkts))
	}
	for i := 1; i < len(pkts); i++ {
		if !pkts[i].PTS.After(pkts[i-1].PTS) {
			t.Fatalf("PTS must strictly increase despite adaptation: %v then %v",
				pkts[i-1].PTS, pkts[i].PTS)
		}
		if pkts[i].OutputSeq <= pkts[i-1].OutputSeq {
			t.Fatal("OutputSeq must increase")
		}
		want := base.Add(time.Duration(pkts[i].SourceSeq) * time.Millisecond)
		if !pkts[i].PTS.Equal(want) {
			t.Fatalf("PTS rewritten: src=%d got=%v want=%v",
				pkts[i].SourceSeq, pkts[i].PTS, want)
		}
	}
}

func TestAdaptController_DeterministicLevels(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReorderMaxFrames = 4
	cfg.MaxQualityLevel = 5
	cfg.MinQualityLevel = 1
	a, b := NewAdaptController(cfg), NewAdaptController(cfg)
	depths := []int{0, 1, 2, 3, 4, 5, 6, 0, 0}
	var ra, rb []int
	for _, d := range depths {
		ra = append(ra, a.Decide(AdaptInput{BacklogAtArrival: d, CurrentQuality: 5}).QualityLevel)
		rb = append(rb, b.Decide(AdaptInput{BacklogAtArrival: d, CurrentQuality: 5}).QualityLevel)
	}
	for i := range ra {
		if ra[i] != rb[i] {
			t.Fatalf("decision not reproducible at depth %d: %v vs %v", depths[i], ra, rb)
		}
	}
}
