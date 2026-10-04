package pipeline

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func pipeCfg() Config {
	cfg := DefaultConfig()
	cfg.AdaptiveEnabled = false
	cfg.InitialQuality = cfg.MaxQuality // 无损
	cfg.KeyframeInterval = 4
	cfg.ReorderWindow = 4
	cfg.EncodeQueueSize = 64
	cfg.SubscriberQueueSize = 64
	return cfg
}

func solidFrame(seq uint64, v byte) Frame {
	p := make([]byte, 4*4*4)
	for i := range p {
		p[i] = v + byte(i%3)
	}
	return Frame{
		Seq:        seq,
		CapturedAt: time.Unix(0, int64(seq)*int64(16*time.Millisecond)),
		Width:      4, Height: 4, Pixels: p,
	}
}

// runPipeline 启动管线，送入全部帧后关闭输入，返回订阅端收到的全部帧。
func runPipeline(t *testing.T, cfg Config, sub **Subscriber, feed func(in chan<- Frame)) []EncodedFrame {
	t.Helper()
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	s := p.Subscribe()
	*sub = s

	done := make(chan error, 1)
	go func() { done <- p.Run(context.Background()) }()
	feed(p.Input())
	close(p.Input())
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pipeline did not stop")
	}

	var got []EncodedFrame
	for f := range s.C {
		got = append(got, f)
	}
	return got
}

func feedFrames(frames []Frame) func(chan<- Frame) {
	return func(in chan<- Frame) {
		for _, f := range frames {
			in <- f
		}
	}
}

func mustDecodeAll(t *testing.T, efs []EncodedFrame) []Frame {
	t.Helper()
	var dec Decoder
	out := make([]Frame, 0, len(efs))
	for i, ef := range efs {
		f, err := dec.Decode(ef)
		if err != nil {
			t.Fatalf("decode idx=%d seq=%d key=%v: %v", i, ef.Seq, ef.Keyframe, err)
		}
		out = append(out, f)
	}
	return out
}

func TestPipelineEndToEndLossless(t *testing.T) {
	var frames []Frame
	for i := uint64(1); i <= 20; i++ {
		frames = append(frames, solidFrame(i, byte(i)))
	}
	var sub *Subscriber
	got := runPipeline(t, pipeCfg(), &sub, feedFrames(frames))
	if len(got) != 20 {
		t.Fatalf("got %d frames, want 20", len(got))
	}
	decoded := mustDecodeAll(t, got)
	for i, f := range decoded {
		if !bytes.Equal(f.Pixels, frames[i].Pixels) {
			t.Fatalf("seq=%d pixels mismatch", f.Seq)
		}
		if got[i].Keyframe != (i%4 == 0) {
			t.Fatalf("idx=%d keyframe=%v, want %v", i, got[i].Keyframe, i%4 == 0)
		}
	}
}

func TestPipelineOutOfOrderDuplicateLate(t *testing.T) {
	var frames []Frame
	// 乱序 + 重复 + 迟到（3 在时间轴越过后才到）。
	seqs := []uint64{1, 2, 5, 4, 4, 6, 7, 8, 3, 9, 10, 1}
	for _, s := range seqs {
		frames = append(frames, solidFrame(s, byte(s)))
	}
	cfg := pipeCfg()
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	sub := p.Subscribe()
	done := make(chan error, 1)
	go func() { done <- p.Run(context.Background()) }()
	feedFrames(frames)(p.Input())
	close(p.Input())
	<-done

	var got []EncodedFrame
	for f := range sub.C {
		got = append(got, f)
	}
	// 输出序号严格递增，重复与迟到帧不得出现。
	for i := 1; i < len(got); i++ {
		if got[i].Seq <= got[i-1].Seq {
			t.Fatalf("output not monotonic: %v", got)
		}
	}
	for _, f := range got {
		if f.Seq == 3 || f.Seq == 1 && f.CapturedAt != got[0].CapturedAt && len(got) > 1 && f.Seq != got[0].Seq {
			t.Fatalf("unexpected frame seq=%d", f.Seq)
		}
	}
	mustDecodeAll(t, got)

	m := p.Metrics()
	if m.Dropped[DropDuplicate] == 0 {
		t.Fatalf("want duplicate drops, got %v", m.Dropped)
	}
	if m.Dropped[DropLate] == 0 {
		t.Fatalf("want late drops, got %v", m.Dropped)
	}
}

func TestPipelineBackpressureSlowSubscriber(t *testing.T) {
	cfg := pipeCfg()
	cfg.SubscriberQueueSize = 2
	cfg.KeyframeInterval = 8
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	slow := p.Subscribe() // 永不消费
	fast := p.Subscribe()

	done := make(chan error, 1)
	go func() { done <- p.Run(context.Background()) }()

	// 采集端快速推 500 帧，不得被慢端阻塞。
	feedDone := make(chan struct{})
	go func() {
		for i := uint64(1); i <= 500; i++ {
			p.Input() <- solidFrame(i, byte(i))
		}
		close(feedDone)
	}()
	select {
	case <-feedDone:
	case <-time.After(3 * time.Second):
		t.Fatal("producer blocked by slow subscriber")
	}
	close(p.Input())
	<-done

	// 快端持续消费（在另一个 goroutine 中）……这里简化：快端也未消费，
	// 两个端都会触发慢端丢弃，但采集端从未阻塞即验证了解耦。
	m := p.Metrics()
	if m.Dropped[DropSubscriberSlow] == 0 {
		t.Fatalf("want subscriber_slow drops, got %v", m.Dropped)
	}
	// 慢端收到的帧里，关键帧之后不允许紧跟无法解码的增量帧。
	var dec Decoder
	for f := range slow.C {
		if _, err := dec.Decode(f); err != nil {
			t.Fatalf("slow sub received undecodable stream: %v", err)
		}
	}
	_ = fast
}

func TestPipelineLateJoinAndReconnect(t *testing.T) {
	cfg := pipeCfg()
	cfg.KeyframeInterval = 4
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- p.Run(context.Background()) }()

	go feedFrames(func() []Frame {
		var fs []Frame
		for i := uint64(1); i <= 30; i++ {
			fs = append(fs, solidFrame(i, byte(i)))
		}
		return fs
	}())(p.Input())

	// 送入一部分后中途接入。
	time.Sleep(50 * time.Millisecond)
	late := p.Subscribe()
	time.Sleep(50 * time.Millisecond)
	// 模拟异常断开并重连。
	p.Unsubscribe(late.ID)
	re := p.Subscribe()

	close(p.Input())
	<-done

	var lateGot, reGot []EncodedFrame
	for f := range late.C {
		lateGot = append(lateGot, f)
	}
	for f := range re.C {
		reGot = append(reGot, f)
	}
	if len(lateGot) == 0 || !lateGot[0].Keyframe {
		t.Fatalf("late join must start at keyframe: %+v", lateGot)
	}
	if len(reGot) == 0 || !reGot[0].Keyframe {
		t.Fatalf("reconnect must start at keyframe: %+v", reGot)
	}
	// 从接入点起必须能完整解码，无花屏。
	mustDecodeAll(t, lateGot)
	mustDecodeAll(t, reGot)
}

func TestPipelineAdaptiveUnderLoad(t *testing.T) {
	cfg := pipeCfg()
	cfg.AdaptiveEnabled = true
	cfg.EncodeQueueSize = 1
	cfg.LatencyTarget = time.Millisecond // 预算极小，轻易触发降级
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	sub := p.Subscribe()
	done := make(chan error, 1)
	go func() { done <- p.Run(context.Background()) }()
	for i := uint64(1); i <= 2000; i++ {
		p.Input() <- solidFrame(i, byte(i))
	}
	close(p.Input())
	<-done

	var got []EncodedFrame
	for f := range sub.C {
		got = append(got, f)
	}
	m := p.Metrics()
	totalDrops := m.Dropped[DropEncodeQueueFull] + m.Dropped[DropAdaptiveThrottle]
	if totalDrops == 0 {
		t.Fatalf("want drops under load, got %v", m.Dropped)
	}
	// 输出时间轴不得跳变：序号严格递增。
	for i := 1; i < len(got); i++ {
		if got[i].Seq <= got[i-1].Seq {
			t.Fatalf("timeline jumped: %v at %d", got[i-1].Seq, i)
		}
	}
	mustDecodeAll(t, got)
}

func TestPipelineEncodeErrorRecovery(t *testing.T) {
	cfg := pipeCfg()
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	sub := p.Subscribe()
	done := make(chan error, 1)
	go func() { done <- p.Run(context.Background()) }()

	p.Input() <- solidFrame(1, 1)
	bad := solidFrame(2, 2)
	bad.Pixels = bad.Pixels[:3] // 损坏的帧 -> 编码失败
	p.Input() <- bad
	p.Input() <- solidFrame(3, 3)
	p.Input() <- solidFrame(4, 4)
	close(p.Input())
	<-done

	m := p.Metrics()
	if m.EncodeErrors != 1 {
		t.Fatalf("EncodeErrors=%d, want 1", m.EncodeErrors)
	}
	if m.Dropped[DropEncodeError] != 1 {
		t.Fatalf("drops=%v", m.Dropped)
	}
	var got []EncodedFrame
	for f := range sub.C {
		got = append(got, f)
	}
	// 失败帧之后的下一帧必须是关键帧，观看端可恢复。
	if len(got) != 3 || got[0].Seq != 1 || got[1].Seq != 3 || got[2].Seq != 4 {
		t.Fatalf("got %v", got)
	}
	if !got[1].Keyframe {
		t.Fatal("frame after encode error must be keyframe")
	}
	mustDecodeAll(t, got)
}

func TestPipelineSourceInterruptDrain(t *testing.T) {
	cfg := pipeCfg()
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	sub := p.Subscribe()
	done := make(chan error, 1)
	go func() { done <- p.Run(context.Background()) }()
	for i := uint64(1); i <= 50; i++ {
		p.Input() <- solidFrame(i, byte(i))
	}
	close(p.Input()) // 采集源中断
	if err := <-done; err != nil {
		t.Fatalf("run after source close: %v", err)
	}
	var got []EncodedFrame
	for f := range sub.C {
		got = append(got, f)
	}
	// 排空语义：50 帧全部送达，不漏推不重复。
	if len(got) != 50 {
		t.Fatalf("got %d frames, want 50 (no loss, no dup)", len(got))
	}
	m := p.Metrics()
	if m.FramesEncoded != 50 {
		t.Fatalf("encoded=%d", m.FramesEncoded)
	}
}

func TestPipelineCancelNoLeak(t *testing.T) {
	cfg := pipeCfg()
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	sub := p.Subscribe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	for i := uint64(1); i <= 10; i++ {
		p.Input() <- solidFrame(i, byte(i))
	}
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pipeline did not stop after cancel")
	}
	// 订阅通道必须被关闭（无悬挂接收方）。
	select {
	case _, ok := <-sub.C:
		for ok {
			_, ok = <-sub.C
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber channel not closed")
	}
}

func TestPipelineReproducible(t *testing.T) {
	cfg := pipeCfg()
	var frames []Frame
	for i := uint64(1); i <= 40; i++ {
		frames = append(frames, solidFrame(i, byte(i*3)))
	}
	run := func() []EncodedFrame {
		var sub *Subscriber
		return runPipeline(t, cfg, &sub, feedFrames(frames))
	}
	a, b := run(), run()
	if len(a) != len(b) {
		t.Fatalf("len %d != %d", len(a), len(b))
	}
	for i := range a {
		if a[i].Seq != b[i].Seq || a[i].Keyframe != b[i].Keyframe ||
			a[i].Quality != b[i].Quality || !bytes.Equal(a[i].Data, b[i].Data) {
			t.Fatalf("frame %d differs between runs", i)
		}
	}
}
