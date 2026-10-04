package pixelstream

import (
	"context"
	"testing"
	"time"
)

// testConfig 返回适合手动时钟快速推进的配置。
func testConfig() Config {
	c := DefaultConfig()
	c.ReorderWindow = 10 * time.Millisecond
	c.LateGrace = 5 * time.Millisecond
	c.KeyFrameInterval = 100 * time.Millisecond
	c.LatencyTarget = 50 * time.Millisecond
	c.AdaptationPeriod = 20 * time.Millisecond
	c.EncoderRetryDelay = 5 * time.Millisecond
	c.InputQueueDepth = 16
	c.OrderedQueueDepth = 4
	c.StoreKeyFrames = 4
	c.ViewerQueueDepth = 8
	return c
}

func newTestPipeline(t *testing.T) (*Pipeline, *ManualClock, time.Time) {
	t.Helper()
	base := time.Unix(1000, 0)
	clk := NewManualClock(base)
	cfg := testConfig()
	p := NewWithClock(cfg, clk)
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := p.Close(ctx); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return p, clk, base
}

func pix(b byte) []byte { return []byte{b, b + 1, b + 2, b + 3} }

func frameAt(seq uint64, ts time.Time) RawFrame {
	return RawFrame{SourceID: "cam", Seq: seq, PTS: ts, Width: 2, Height: 2, Pix: pix(byte(seq))}
}

// advanceReorder 分两步推进手动时钟跨过排序窗口，消除"推进时刻早于
// 重排协程订阅 ticker/帧刚入队"的启动竞态，保证确定性。
func advanceReorder(clk *ManualClock) {
	time.Sleep(2 * time.Millisecond)
	clk.Advance(8 * time.Millisecond)
	time.Sleep(2 * time.Millisecond)
	clk.Advance(10 * time.Millisecond)
}

// waitPackets 从 viewer 收集至多 n 个包，或在真实墙钟超时后返回已收到的。
func waitPackets(v *Viewer, n int, timeout time.Duration) []EncodedPacket {
	deadline := time.Now().Add(timeout)
	var out []EncodedPacket
	for len(out) < n && time.Now().Before(deadline) {
		select {
		case p := <-v.Packets():
			out = append(out, p)
		case <-time.After(5 * time.Millisecond):
		}
	}
	return out
}

func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

func TestPipeline_OutOfOrderTimelineExplainable(t *testing.T) {
	p, clk, base := newTestPipeline(t)
	v, _ := p.AddViewer(context.Background(), "v1")

	// 乱序提交（按到达次序）：seq/PTS 为 2,0,1。
	p.SubmitFrame(frameAt(2, base.Add(20*time.Millisecond)))
	p.SubmitFrame(frameAt(0, base))
	p.SubmitFrame(frameAt(1, base.Add(10*time.Millisecond)))
	advanceReorder(clk)

	pkts := waitPackets(v, 3, 2*time.Second)
	if len(pkts) != 3 {
		t.Fatalf("got %d packets", len(pkts))
	}
	for i := 1; i < len(pkts); i++ {
		if pkts[i].OutputSeq <= pkts[i-1].OutputSeq {
			t.Fatalf("OutputSeq must increase: %v", pkts)
		}
		if pkts[i].PTS.Before(pkts[i-1].PTS) {
			t.Fatalf("output PTS must not regress: %v before %v", pkts[i].PTS, pkts[i-1].PTS)
		}
	}
	if pkts[0].SourceSeq != 0 || pkts[1].SourceSeq != 1 || pkts[2].SourceSeq != 2 {
		t.Fatalf("source order = %d,%d,%d", pkts[0].SourceSeq, pkts[1].SourceSeq, pkts[2].SourceSeq)
	}
	// 首包必须是关键帧，新观看端可独立解码。
	if pkts[0].Type != TypeKey {
		t.Fatalf("first packet must be keyframe, got %v", pkts[0].Type)
	}
}

func TestPipeline_DuplicateAndLateDrops(t *testing.T) {
	p, clk, base := newTestPipeline(t)
	v, _ := p.AddViewer(context.Background(), "v1")

	// 正常两帧并推进到已输出。
	p.SubmitFrame(frameAt(1, base))
	p.SubmitFrame(frameAt(2, base.Add(10*time.Millisecond)))
	advanceReorder(clk)
	if len(waitPackets(v, 2, 2*time.Second)) != 2 {
		t.Fatal("missing initial frames")
	}

	// 重复帧（相同采集序号）。重复/迟到由排序阶段异步判定，
	// SubmitResult 只保证"未被慢观看端阻塞、已进入输入队列"。
	p.SubmitFrame(frameAt(1, base))

	// 迟到帧（PTS 远早于已输出位置）。
	p.SubmitFrame(frameAt(50, base.Add(90*time.Millisecond)))
	advanceReorder(clk)
	waitPackets(v, 1, 2*time.Second) // seq50 先成为已输出
	p.SubmitFrame(frameAt(3, base.Add(5*time.Millisecond)))
	advanceReorder(clk)

	// 给排序协程处理重复帧的时间。
	if !waitFor(2*time.Second, func() bool {
		st := p.Metrics()
		return st.Drops[DropDuplicate] >= 1 && st.Drops[DropLate] >= 1
	}) {
		t.Fatalf("drop counters = %+v", p.Metrics().Drops)
	}

	// 事件流中两类原因可区分。
	var sawDup, sawLate bool
	for _, e := range p.Events() {
		if e.Outcome != OutcomeDropped {
			continue
		}
		sawDup = sawDup || e.Drop == DropDuplicate
		sawLate = sawLate || e.Drop == DropLate
	}
	if !sawDup || !sawLate {
		t.Fatalf("events must explain both drops: dup=%v late=%v", sawDup, sawLate)
	}
}
