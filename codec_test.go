package pixelstream

import (
	"context"
	"testing"
	"time"
)

func TestFakeCodec_KeyframeIndependentlyDecodable(t *testing.T) {
	c := NewFakeCodec(2, 2, 5)
	f := mkFrame(0, time.Unix(0, 0), 7)
	p, err := c.Encode(context.Background(), f, true)
	if err != nil {
		t.Fatal(err)
	}
	if p.Type != TypeKey {
		t.Fatalf("want key, got %v", p.Type)
	}
	if p.OutputSeq != 1 || p.KeyID != 1 {
		t.Fatalf("unexpected ids: %+v", p)
	}
	b, err := decodePacket(p, nil)
	if err != nil {
		t.Fatalf("keyframe must decode without baseline: %v", err)
	}
	if len(b) != 4 || b[0] != 7 {
		t.Fatalf("decoded bytes wrong: %v", b)
	}
}

func TestFakeCodec_DeltaNeedsMatchingKeyframe(t *testing.T) {
	c := NewFakeCodec(2, 2, 5)
	ctx := context.Background()
	k, _ := c.Encode(ctx, mkFrame(1, time.Unix(0, 0), 1), true)
	d, err := c.Encode(ctx, mkFrame(2, time.Unix(1, 0), 2), false)
	if err != nil {
		t.Fatal(err)
	}
	if d.Type != TypeDelta || d.KeyID != k.OutputSeq {
		t.Fatalf("delta chain wrong: %+v", d)
	}
	// 无基线不可解码（证明缺关键帧时只能等待，不能花屏）。
	if _, err := decodePacket(d, nil); err == nil {
		t.Fatal("delta without baseline must fail to decode")
	}
	// 错误链基线也不行。
	other := k
	other.OutputSeq = 999
	if _, err := decodePacket(d, &other); err == nil {
		t.Fatal("delta with mismatched key id must fail")
	}
	// 正确基线可解码。
	if _, err := decodePacket(d, &k); err != nil {
		t.Fatalf("delta with baseline must decode: %v", err)
	}
}

func TestFakeCodec_ForceKeyAfterGap(t *testing.T) {
	c := NewFakeCodec(2, 2, 5)
	ctx := context.Background()
	c.Encode(ctx, mkFrame(1, time.Unix(0, 0), 1), true)
	// seq 跳到 6（中间丢帧，链断裂）：必须自动产生关键帧。
	p, err := c.Encode(ctx, mkFrame(6, time.Unix(5, 0), 1), false)
	if err != nil {
		t.Fatal(err)
	}
	if p.Type != TypeKey {
		t.Fatalf("chain break must force keyframe, got %v", p.Type)
	}
}

func TestFakeCodec_Deterministic(t *testing.T) {
	mk := func() (*FakeCodec, RawFrame) {
		return NewFakeCodec(2, 2, 3), mkFrame(3, time.Unix(7, 0), 4)
	}
	c1, f1 := mk()
	c2, f2 := mk()
	p1, _ := c1.Encode(context.Background(), f1, true)
	p2, _ := c2.Encode(context.Background(), f2, true)
	if string(p1.Payload) != string(p2.Payload) {
		t.Fatal("same input + same config must yield identical bytes")
	}
}

func TestFakeCodec_FailureInjected(t *testing.T) {
	c := NewFakeCodec(2, 2, 5)
	c.FailUntilSeq(2)
	ctx := context.Background()
	if _, err := c.Encode(ctx, mkFrame(1, time.Unix(0, 0), 1), true); err == nil {
		t.Fatal("expected injected failure")
	}
	if _, err := c.Encode(ctx, mkFrame(3, time.Unix(3, 0), 1), true); err != nil {
		t.Fatalf("frame after fail window should succeed: %v", err)
	}
}
