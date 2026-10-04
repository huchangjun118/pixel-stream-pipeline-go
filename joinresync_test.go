package pixelstream

import (
	"context"
	"testing"
	"time"
)

func TestViewer_MidJoinStartsAtKeyframe(t *testing.T) {
	p, clk, base := newTestPipeline(t)
	early, _ := p.AddViewer(context.Background(), "early")

	flushAll(p, clk, base, 1, 25) // 跨过 100ms 关键帧间隔（每 1ms PTS）
	// 中途接入：首帧必须是关键帧，且其后能完整解码（无花屏）。
	late, _ := p.AddViewer(context.Background(), "late")
	pkts := waitPackets(late, 3, 2*time.Second)
	if len(pkts) == 0 {
		t.Fatal("late viewer got nothing")
	}
	if pkts[0].Type != TypeKey {
		t.Fatalf("mid-join first packet must be keyframe, got %v", pkts[0].Type)
	}
	baseline := pkts[0]
	if baseline.Type != TypeKey {
		t.Fatalf("first backfilled packet must be keyframe, got %v", baseline.Type)
	}
	if _, err := decodePacket(baseline, nil); err != nil {
		t.Fatalf("keyframe decode: %v", err)
	}
	for i := 1; i < len(pkts); i++ {
		pk := pkts[i]
		if pk.Type == TypeKey {
			baseline = pk
			if _, err := decodePacket(pk, nil); err != nil {
				t.Fatalf("keyframe decode: %v", err)
			}
			continue
		}
		if pk.KeyID != baseline.OutputSeq {
			t.Fatalf("delta must chain to backfilled keyframe: key=%d deltaKey=%d",
				baseline.OutputSeq, pk.KeyID)
		}
		if _, err := decodePacket(pk, &baseline); err != nil {
			t.Fatalf("backfilled chain must decode cleanly: %v", err)
		}
	}
	// early 观看端也一直在正常播放，没有被新接入影响。
	if n := len(waitPackets(early, 1, time.Second)); n == 0 {
		t.Fatal("existing viewer should keep streaming")
	}
}

func TestViewer_ReconnectResyncNoCorruption(t *testing.T) {
	p, clk, base := newTestPipeline(t)

	// 第一次接入并播放一段。
	v, _ := p.AddViewer(context.Background(), "u")
	flushAll(p, clk, base, 1, 5)
	if n := len(waitPackets(v, 5, 2*time.Second)); n == 0 {
		t.Fatal("first session got no packets")
	}

	// 模拟异常断开。
	p.RemoveViewer("u")
	if !waitFor(2*time.Second, func() bool {
		select {
		case <-v.Done():
			return true
		default:
			return false
		}
	}) {
		t.Fatal("disconnected viewer must finish")
	}

	// 继续产出，之后同 id 重连（序号连续，时间轴也后移，避免被判迟到）。
	flushAll(p, clk, base.Add(300*time.Millisecond), 6, 20)
	v2, _ := p.AddViewer(context.Background(), "u")
	pkts := waitPackets(v2, 3, 2*time.Second)
	if len(pkts) == 0 {
		t.Fatal("reconnected viewer got nothing")
	}
	if pkts[0].Type != TypeKey {
		t.Fatalf("reconnect must resume from keyframe, got type=%v seq=%d", pkts[0].Type, pkts[0].SourceSeq)
	}
	// 背填序列必须从关键帧开始、逐帧链式可解码，且顺序与 PTS 单调。
	k := pkts[0]
	if k.Type != TypeKey {
		t.Fatalf("reconnect first packet must be keyframe")
	}
	if _, err := decodePacket(k, nil); err != nil {
		t.Fatalf("keyframe must decode standalone: %v", err)
	}
	lastPTS := k.PTS
	for i := 1; i < len(pkts); i++ {
		pk := pkts[i]
		if pk.PTS.Before(lastPTS) {
			t.Fatal("reconnect timeline regressed")
		}
		lastPTS = pk.PTS
		if pk.Type == TypeKey {
			k = pk
			if _, err := decodePacket(pk, nil); err != nil {
				t.Fatalf("keyframe must decode standalone: %v", err)
			}
			continue
		}
		if pk.KeyID != k.OutputSeq {
			t.Fatalf("delta key chain mismatch: delta=%d key=%d", pk.KeyID, k.OutputSeq)
		}
		if _, err := decodePacket(pk, &k); err != nil {
			t.Fatalf("reconnected chain must decode without corruption: %v", err)
		}
	}

	// 重连后新产生的帧同样干净可解码，且不会重复推送已经背填过的 OutputSeq。
	seen := map[uint64]bool{}
	for _, pk := range pkts {
		if seen[pk.OutputSeq] {
			t.Fatalf("packet %d delivered twice after reconnect", pk.OutputSeq)
		}
		seen[pk.OutputSeq] = true
	}
	flushAll(p, clk, base.Add(600*time.Millisecond), 26, 6)
	more := waitPackets(v2, 6, 2*time.Second)
	for _, pk := range more {
		if seen[pk.OutputSeq] {
			t.Fatalf("packet %d duplicated across backfill/live boundary", pk.OutputSeq)
		}
		seen[pk.OutputSeq] = true
		curKey := k
		if pk.Type == TypeKey {
			curKey = pk
		} else if pk.KeyID != curKey.OutputSeq && pk.KeyID != k.OutputSeq {
			t.Fatalf("post-reconnect chain broken: %d", pk.KeyID)
		}
		if pk.Type == TypeKey {
			k = pk
		}
		if _, err := decodePacket(pk, &curKey); err != nil {
			t.Fatalf("post-reconnect live frame must decode: %v", err)
		}
	}
}

func TestViewer_RequestResyncAlignsAtNextKeyframe(t *testing.T) {
	p, clk, base := newTestPipeline(t)
	v, _ := p.AddViewer(context.Background(), "v")
	flushAll(p, clk, base, 1, 3)
	if n := len(waitPackets(v, 3, 2*time.Second)); n == 0 {
		t.Fatal("initial frames missing")
	}
	keysBefore := p.Metrics().KeyFrames

	// 观看端主动请求重对齐（同时合并请求一个即时关键帧）。
	p.RequestResync("v")
	flushAll(p, clk, base.Add(200*time.Millisecond), 4, 4)

	// 持续读取直到收到一个"新产出"的关键帧；收到前的增量帧应已被丢弃。
	deadline := time.Now().Add(2 * time.Second)
	gotFreshKey := false
	for time.Now().Before(deadline) && !gotFreshKey {
		select {
		case pk := <-v.Packets():
			if pk.Type == TypeKey && p.Metrics().KeyFrames > keysBefore {
				gotFreshKey = true
			}
		case <-time.After(10 * time.Millisecond):
		}
	}
	if !gotFreshKey {
		t.Fatalf("resync must deliver a fresh keyframe (keys=%d before=%d)",
			p.Metrics().KeyFrames, keysBefore)
	}
}
