package pixelstream

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// flushAll 小批量提交并配合手动时钟推进，确保 n 帧全部通过排序窗口并编码。
// 每批帧数不超过有序队列深度，批间等待该批产出，过程确定可复现。
func flushAll(p *Pipeline, clk *ManualClock, base time.Time, fromSeq uint64, n int) {
	batch := p.cfg.OrderedQueueDepth
	if batch > n {
		batch = n
	}
	var produced uint64
	for produced < uint64(n) {
		k := batch
		if uint64(k) > uint64(n)-produced {
			k = n - int(produced)
		}
		for j := 0; j < k; j++ {
			seq := fromSeq + produced
			p.SubmitFrame(frameAt(seq, base.Add(time.Duration(seq)*time.Millisecond)))
			produced++
		}
		time.Sleep(2 * time.Millisecond)
		clk.Advance(12 * time.Millisecond) // 跨过排序窗口
		want := int64(produced)
		waitFor(3*time.Second, func() bool {
			st := p.Metrics()
			return st.Encoded+st.Drops[DropAdaptive] >= want
		})
	}
	waitFor(3*time.Second, func() bool {
		st := p.Metrics()
		return st.Encoded+st.Drops[DropAdaptive] >= int64(n)
	})
}

func waitEncoded(p *Pipeline, n int64) bool {
	return waitFor(3*time.Second, func() bool { return p.Metrics().Encoded >= n })
}

func TestBackpressure_SlowViewerDoesNotBlockFastOrEncoder(t *testing.T) {
	p, clk, base := newTestPipeline(t)

	slow, _ := p.AddViewer(context.Background(), "slow")
	fast, _ := p.AddViewer(context.Background(), "fast")
	// fast 全速读取。
	var fastGot atomic.Int64
	go func() {
		for range fast.Packets() {
			fastGot.Add(1)
		}
	}()

	// 阶段一：slow 完全停读，制造积压；编码器与 fast 都不应受影响。
	flushAll(p, clk, base, 1, 40)
	// 慢观看端不得阻塞编码器：编码或自适应降帧都算"已处理"，必须达到 40。
	if !waitFor(3*time.Second, func() bool {
		st := p.Metrics()
		return st.Encoded+st.Drops[DropAdaptive] >= 40
	}) {
		st := p.Metrics()
		t.Fatalf("encoder blocked by slow viewer, processed=%d encoded=%d",
			st.Encoded+st.Drops[DropAdaptive], st.Encoded)
	}
	// fast 必须收到"实际编码"的全部包（自适应跳掉的帧谁都收不到，属正常）。
	wantFast := p.Metrics().Encoded
	if !waitFor(2*time.Second, func() bool { return fastGot.Load() >= wantFast }) {
		t.Fatalf("fast viewer starved: got %d want encoded=%d", fastGot.Load(), wantFast)
	}
	st := p.Metrics()
	if st.Drops[DropViewerSlow] == 0 && st.Resyncs == 0 {
		t.Fatalf("slow viewer must drop/resync under backlog, metrics=%+v", st)
	}

	// 阶段二：slow 恢复读取并主动重对齐，应能在下一个关键帧后继续干净播放。
	p.RequestResync("slow")
	flushAll(p, clk, base.Add(500*time.Millisecond), 101, 6)
	gotKey := false
loop:
	for {
		select {
		case pk := <-slow.Packets():
			if pk.Type == TypeKey {
				gotKey = true
				break loop
			}
		case <-time.After(500 * time.Millisecond):
			break loop
		}
	}
	if !gotKey {
		t.Fatal("slow viewer must resume from a fresh keyframe")
	}
}

func TestBackpressure_InputQueueFullDistinguishable(t *testing.T) {
	cfg := testConfig()
	cfg.InputQueueDepth = 2
	cfg.DropPolicy = DropPolicyNone // 编码队列满时回压，配合极深有序队列隔离原因
	cfg.OrderedQueueDepth = 1024
	base := time.Unix(7, 0)
	clk := NewManualClock(base)
	p := NewWithClock(cfg, clk)
	p.Start(context.Background())
	defer p.Close(context.Background())

	// 不推进时钟：帧全部堆在重排缓冲与输入/有序队列外的 rawCh，
	// rawCh 容量 2，连续提交应在若干次后得到 input_full。
	sawFull := false
	for i := uint64(1); i <= 2000; i++ {
		r := p.SubmitFrame(frameAt(i, base.Add(time.Duration(i)*time.Millisecond)))
		if !r.Accepted {
			if r.Drop != DropInputFull {
				t.Fatalf("want input_full, got %v", r.Drop)
			}
			sawFull = true
			break
		}
	}
	if !sawFull {
		t.Fatal("expected input_full under backpressure")
	}
	if !waitFor(2*time.Second, func() bool { return p.Metrics().Drops[DropInputFull] >= 1 }) {
		t.Fatal("input_full drop must be counted")
	}
}

func TestPayloadSharedAcrossViewers(t *testing.T) {
	p, clk, base := newTestPipeline(t)
	v1, _ := p.AddViewer(context.Background(), "v1")
	v2, _ := p.AddViewer(context.Background(), "v2")
	p.SubmitFrame(frameAt(1, base))
	advanceReorder(clk)
	pk1 := waitPackets(v1, 1, 2*time.Second)
	pk2 := waitPackets(v2, 1, 2*time.Second)
	if len(pk1) != 1 || len(pk2) != 1 {
		t.Fatalf("missing packets: %d %d", len(pk1), len(pk2))
	}
	// 同一份编码结果：底层字节数组必须是同一引用，证明安全零拷贝复用。
	if &pk1[0].Payload[0] != &pk2[0].Payload[0] {
		t.Fatal("encoded payload must be shared (same backing array) across viewers")
	}
}
