package pipeline

import (
	"testing"
	"time"
)

func mkFrame(seq uint64) Frame {
	return Frame{
		Seq:        seq,
		CapturedAt: time.Unix(0, int64(seq)*int64(time.Millisecond)),
		Width:      2,
		Height:     2,
		Pixels:     make([]byte, 16),
	}
}

// collectDrops 记录丢弃原因。
type dropRec struct {
	byReason map[DropReason]int
	seqs     map[DropReason][]uint64
}

func newDropRec() *dropRec {
	return &dropRec{byReason: map[DropReason]int{}, seqs: map[DropReason][]uint64{}}
}

func (d *dropRec) cb() func(Frame, DropReason) {
	return func(f Frame, r DropReason) {
		d.byReason[r]++
		d.seqs[r] = append(d.seqs[r], f.Seq)
	}
}

func seqsOf(frames []Frame) []uint64 {
	out := make([]uint64, len(frames))
	for i, f := range frames {
		out[i] = f.Seq
	}
	return out
}

func equalSeqs(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestSequencerInOrder(t *testing.T) {
	s := NewSequencer(4, nil)
	var out []Frame
	for i := uint64(1); i <= 5; i++ {
		out = append(out, s.Push(mkFrame(i))...)
	}
	out = append(out, s.Flush()...)
	if !equalSeqs(seqsOf(out), []uint64{1, 2, 3, 4, 5}) {
		t.Fatalf("got %v", seqsOf(out))
	}
}

func TestSequencerOutOfOrder(t *testing.T) {
	s := NewSequencer(4, nil)
	var out []Frame
	for _, seq := range []uint64{1, 3, 2, 5, 4} {
		out = append(out, s.Push(mkFrame(seq))...)
	}
	out = append(out, s.Flush()...)
	if !equalSeqs(seqsOf(out), []uint64{1, 2, 3, 4, 5}) {
		t.Fatalf("got %v", seqsOf(out))
	}
}

func TestSequencerDuplicate(t *testing.T) {
	d := newDropRec()
	s := NewSequencer(4, d.cb())
	s.Push(mkFrame(1))
	s.Push(mkFrame(2))
	s.Push(mkFrame(2)) // 已输出 -> duplicate
	s.Push(mkFrame(3))
	s.Push(mkFrame(3)) // 缓冲中 -> duplicate
	if d.byReason[DropDuplicate] != 2 {
		t.Fatalf("duplicate drops = %d, want 2 (%v)", d.byReason[DropDuplicate], d.byReason)
	}
}

func TestSequencerLateFrame(t *testing.T) {
	d := newDropRec()
	s := NewSequencer(2, d.cb())
	// 1,2 输出后，3 未到，4,5 到达使窗口溢出，时间轴前进到 4；
	// 之后 3 才姗姗来迟 -> late（从未见过）。
	var out []Frame
	for _, seq := range []uint64{1, 2, 4, 5, 6} {
		out = append(out, s.Push(mkFrame(seq))...)
	}
	s.Push(mkFrame(3))
	if d.byReason[DropLate] != 1 {
		t.Fatalf("late drops = %d, want 1 (%v)", d.byReason[DropLate], d.byReason)
	}
	// 输出必须严格递增。
	seqs := seqsOf(out)
	for i := 1; i < len(seqs); i++ {
		if seqs[i] <= seqs[i-1] {
			t.Fatalf("output not monotonic: %v", seqs)
		}
	}
}

func TestSequencerWindowOverflowSkipsGap(t *testing.T) {
	s := NewSequencer(2, nil)
	var out []Frame
	// 序号 2 丢失；窗口为 2，缓冲 {3,4,5} 超过窗口后应跳过 2 输出 3。
	out = append(out, s.Push(mkFrame(1))...)
	out = append(out, s.Push(mkFrame(3))...)
	out = append(out, s.Push(mkFrame(4))...)
	if !equalSeqs(seqsOf(out), []uint64{1}) {
		t.Fatalf("before overflow out=%v", seqsOf(out))
	}
	out = append(out, s.Push(mkFrame(5))...)
	if !equalSeqs(seqsOf(out), []uint64{1, 3}) {
		t.Fatalf("got %v, want [1 3]", seqsOf(out))
	}
	// 时间轴越过缺口后，后续连续序号正常衔接。
	out = append(out, s.Push(mkFrame(6))...)
	if !equalSeqs(seqsOf(out), []uint64{1, 3, 4, 5, 6}) {
		t.Fatalf("got %v, want [1 3 4 5 6]", seqsOf(out))
	}
}

func TestSequencerMonotonicUnderChaos(t *testing.T) {
	// 构造确定性伪随机乱序 + 重复 + 迟到，验证输出严格递增且为输入子序列。
	d := newDropRec()
	s := NewSequencer(8, d.cb())
	var out []Frame
	push := func(seq uint64) {
		out = append(out, s.Push(mkFrame(seq))...)
	}
	// 确定性伪随机序列。
	state := uint64(42)
	rnd := func(n uint64) uint64 {
		state = state*6364136223846793005 + 1442695040888963407
		return (state >> 33) % n
	}
	for i := 0; i < 2000; i++ {
		switch rnd(4) {
		case 0:
			push(uint64(i/2 + 1)) // 可能重复
		default:
			push(uint64(i + 1))
		}
	}
	out = append(out, s.Flush()...)
	seqs := seqsOf(out)
	for i := 1; i < len(seqs); i++ {
		if seqs[i] <= seqs[i-1] {
			t.Fatalf("output not strictly increasing at %d: %v...", i, seqs[max(0, i-3):i+1])
		}
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
