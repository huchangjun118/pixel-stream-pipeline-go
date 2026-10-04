package pipeline

import (
	"testing"
	"time"
)

func ef(seq uint64, key bool) EncodedFrame {
	return EncodedFrame{Seq: seq, Keyframe: key, Data: []byte{byte(seq)}}
}

func drain(sub *Subscriber, max int) []EncodedFrame {
	var out []EncodedFrame
	for len(out) < max {
		select {
		case f, ok := <-sub.C:
			if !ok {
				return out
			}
			out = append(out, f)
		case <-time.After(100 * time.Millisecond):
			return out
		}
	}
	return out
}

func TestHubFanoutIndependentProgress(t *testing.T) {
	h := NewHub(4, nil)
	fast := h.Subscribe()
	slow := h.Subscribe()

	h.Broadcast(ef(1, true))
	h.Broadcast(ef(2, false))

	// 快端立即消费。
	got := drain(fast, 2)
	if len(got) != 2 || got[0].Seq != 1 || got[1].Seq != 2 {
		t.Fatalf("fast sub got %v", got)
	}
	// 慢端尚未消费，之后仍能取到（各自独立进度）。
	got = drain(slow, 2)
	if len(got) != 2 {
		t.Fatalf("slow sub got %d frames", len(got))
	}
	h.Close()
}

func TestHubSharedEncodedFrame(t *testing.T) {
	h := NewHub(4, nil)
	a := h.Subscribe()
	b := h.Subscribe()
	frame := ef(1, true)
	h.Broadcast(frame)
	ga, gb := drain(a, 1)[0], drain(b, 1)[0]
	// 同一份编码结果被复用，底层数组不得拷贝。
	if &ga.Data[0] != &gb.Data[0] {
		t.Fatal("encoded data should be shared, not copied")
	}
	h.Close()
}

func TestHubSlowSubscriberResyncsAtKeyframe(t *testing.T) {
	var drops []uint64
	h := NewHub(2, func(id int, f EncodedFrame, r DropReason) {
		if r != DropSubscriberSlow {
			t.Fatalf("unexpected reason %v", r)
		}
		drops = append(drops, f.Seq)
	})
	sub := h.Subscribe() // 从不消费，队列容量 2

	h.Broadcast(ef(1, true))  // 入队
	h.Broadcast(ef(2, false)) // 入队，队列满
	h.Broadcast(ef(3, false)) // 丢弃 -> 失同步
	h.Broadcast(ef(4, false)) // 失同步中，跳过
	h.Broadcast(ef(5, true))  // 关键帧 -> 清空积压 1,2，重新同步

	// 丢弃序列：3（队满）、1 和 2（关键帧前清空积压）。
	wantDrops := []uint64{3, 1, 2}
	if len(drops) != len(wantDrops) {
		t.Fatalf("drops = %v, want %v", drops, wantDrops)
	}
	for i := range drops {
		if drops[i] != wantDrops[i] {
			t.Fatalf("drops = %v, want %v", drops, wantDrops)
		}
	}
	got := drain(sub, 10)
	// 队列中应只剩 5；绝不能出现丢了参考帧的 3,4（否则花屏）。
	want := []uint64{5}
	if len(got) != len(want) {
		t.Fatalf("got %v, want seqs %v", got, want)
	}
	for i, f := range got {
		if f.Seq != want[i] {
			t.Fatalf("got %v, want seqs %v", got, want)
		}
	}
	h.Close()
}

func TestHubLateJoinStartsAtKeyframe(t *testing.T) {
	h := NewHub(16, nil)
	early := h.Subscribe()
	h.Broadcast(ef(1, true))
	h.Broadcast(ef(2, false))
	h.Broadcast(ef(3, false))

	// 中途接入：应从最近关键帧（1）起的完整 GOP 开始。
	late := h.Subscribe()
	got := drain(late, 10)
	if len(got) != 3 || got[0].Seq != 1 || !got[0].Keyframe {
		t.Fatalf("late join got %v", got)
	}

	// 新关键帧后接入：只应看到新 GOP。
	h.Broadcast(ef(4, true))
	h.Broadcast(ef(5, false))
	later := h.Subscribe()
	got = drain(later, 10)
	if len(got) != 2 || got[0].Seq != 4 || !got[0].Keyframe {
		t.Fatalf("later join got %v", got)
	}
	h.Close()
	_ = early
}

func TestHubReconnectGetsCleanGOP(t *testing.T) {
	h := NewHub(16, nil)
	sub := h.Subscribe()
	h.Broadcast(ef(1, true))
	h.Broadcast(ef(2, false))
	drain(sub, 2)

	// 模拟异常断开并重连：旧订阅丢弃，新订阅从关键帧开始。
	h.Unsubscribe(sub.ID)
	if _, ok := <-sub.C; ok {
		t.Fatal("unsubscribed channel should be closed")
	}
	h.Broadcast(ef(3, false))
	h.Broadcast(ef(4, true))
	re := h.Subscribe()
	got := drain(re, 10)
	if len(got) != 1 || got[0].Seq != 4 || !got[0].Keyframe {
		t.Fatalf("reconnect got %v", got)
	}
	h.Close()
}

func TestHubCloseIdempotent(t *testing.T) {
	h := NewHub(2, nil)
	sub := h.Subscribe()
	h.Broadcast(ef(1, true))
	h.Close()
	h.Close() // 幂等
	got := drain(sub, 10)
	if len(got) != 1 {
		t.Fatalf("got %d frames after close", len(got))
	}
	// Close 后广播被忽略，不应 panic。
	h.Broadcast(ef(2, false))
}
