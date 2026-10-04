package pixelstream

import (
	"testing"
	"time"
)

func mkFrame(seq uint64, pts time.Time, pix byte) RawFrame {
	return RawFrame{SourceID: "cam", Seq: seq, PTS: pts, Width: 2, Height: 2, Pix: []byte{pix, pix, pix, pix}}
}

func TestReorder_OutOfOrderThenSorted(t *testing.T) {
	base := time.Unix(0, 0)
	cfg := DefaultConfig()
	cfg.ReorderWindow = 10 * time.Millisecond
	cfg.LateGrace = 5 * time.Millisecond
	clk := NewManualClock(base)
	rb := newReorderBuffer(cfg)

	// 乱序到达：seq1(t10), seq0(t0), seq2(t20)
	pushOrDrop(t, rb, mkFrame(1, base.Add(10*time.Millisecond), 1), 1, clk.Now())
	pushOrDrop(t, rb, mkFrame(0, base, 0), 2, clk.Now())
	pushOrDrop(t, rb, mkFrame(2, base.Add(20*time.Millisecond), 2), 3, clk.Now())

	// 窗口未到：不能输出（给迟到帧留时间）。
	if _, ok := rb.popDue(clk.Now(), 3); ok {
		t.Fatal("expected no frame before reorder window")
	}
	clk.Advance(11 * time.Millisecond)
	var seqOrder []uint64
	var ptsOrder []int64
	for {
		of, ok := rb.popDue(clk.Now(), 3)
		if !ok {
			break
		}
		seqOrder = append(seqOrder, of.frame.Seq)
		ptsOrder = append(ptsOrder, of.frame.PTS.UnixNano())
	}
	want := []uint64{0, 1, 2}
	if len(seqOrder) != 3 {
		t.Fatalf("got %v", seqOrder)
	}
	for i := range want {
		if seqOrder[i] != want[i] {
			t.Fatalf("seq order = %v, want %v", seqOrder, want)
		}
	}
	for i := 1; i < len(ptsOrder); i++ {
		if ptsOrder[i] < ptsOrder[i-1] {
			t.Fatalf("PTS must be non-regressing: %v", ptsOrder)
		}
	}
}

func TestReorder_Duplicate(t *testing.T) {
	base := time.Unix(0, 0)
	cfg := DefaultConfig()
	clk := NewManualClock(base)
	rb := newReorderBuffer(cfg)
	f := mkFrame(5, base, 9)
	if r := rb.push(f, 1, clk.Now(), frameDecision{}); r != DropNone {
		t.Fatalf("first push: %v", r)
	}
	if r := rb.push(f, 2, clk.Now(), frameDecision{}); r != DropDuplicate {
		t.Fatalf("duplicate push reason = %v, want duplicate", r)
	}
}

func TestReorder_LateFrame(t *testing.T) {
	base := time.Unix(0, 0)
	cfg := DefaultConfig()
	cfg.ReorderWindow = 5 * time.Millisecond
	cfg.LateGrace = 2 * time.Millisecond
	clk := NewManualClock(base)
	rb := newReorderBuffer(cfg)

	rb.push(mkFrame(10, base.Add(100*time.Millisecond), 0), 1, clk.Now(), frameDecision{})
	clk.Advance(6 * time.Millisecond)
	if _, ok := rb.popDue(clk.Now(), 1); !ok {
		t.Fatal("expected first frame due")
	}
	// 远早于已输出 PTS 的迟到帧：拒绝，绝不允许时间轴倒流。
	r := rb.push(mkFrame(9, base.Add(50*time.Millisecond), 0), 2, clk.Now(), frameDecision{})
	if r != DropLate {
		t.Fatalf("late reason = %v, want late", r)
	}
	// 恰好在宽限内的帧：作为正常帧进入缓冲（之后若更旧再由排序处理）。
	r2 := rb.push(mkFrame(11, base.Add(101*time.Millisecond), 0), 3, clk.Now(), frameDecision{})
	if r2 != DropNone {
		t.Fatalf("in-grace frame should be accepted, got %v", r2)
	}
}

func TestReorder_OverCapacityFlushes(t *testing.T) {
	base := time.Unix(0, 0)
	cfg := DefaultConfig()
	cfg.ReorderMaxFrames = 2
	cfg.ReorderWindow = time.Hour // 时间窗口几乎不到期，强制走超限分支
	clk := NewManualClock(base)
	rb := newReorderBuffer(cfg)
	for i := 0; i < 3; i++ {
		rb.push(mkFrame(uint64(i), base.Add(time.Duration(i)*time.Millisecond), 0), uint64(i+1), clk.Now(), frameDecision{})
	}
	if _, ok := rb.popDue(clk.Now(), 3); !ok {
		t.Fatal("expected over-capacity flush")
	}
}

func pushOrDrop(t *testing.T, rb *reorderBuffer, f RawFrame, arr uint64, now time.Time) {
	t.Helper()
	if r := rb.push(f, arr, now, frameDecision{}); r != DropNone {
		t.Fatalf("push seq=%d: unexpected drop %v", f.Seq, r)
	}
}
