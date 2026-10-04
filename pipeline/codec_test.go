package pipeline

import (
	"bytes"
	"testing"
)

// gradientFrame 生成内容确定、逐帧变化的测试帧。
func gradientFrame(seq uint64, w, h int) Frame {
	p := make([]byte, w*h*4)
	for i := range p {
		p[i] = byte((i + int(seq)*7) % 256)
	}
	return Frame{Seq: seq, Width: w, Height: h, Pixels: p}
}

func encodeAll(t *testing.T, enc *Encoder, frames []Frame, quality int) []EncodedFrame {
	t.Helper()
	out := make([]EncodedFrame, 0, len(frames))
	for _, f := range frames {
		ef, err := enc.Encode(f, quality)
		if err != nil {
			t.Fatalf("encode seq=%d: %v", f.Seq, err)
		}
		out = append(out, ef)
	}
	return out
}

func TestCodecRoundTripLossless(t *testing.T) {
	enc := NewEncoder(5, 4) // maxQuality=4, quality=4 -> shift=0 无损
	frames := []Frame{
		gradientFrame(1, 4, 4), gradientFrame(2, 4, 4), gradientFrame(3, 4, 4),
		gradientFrame(4, 4, 4), gradientFrame(5, 4, 4), gradientFrame(6, 4, 4),
	}
	efs := encodeAll(t, enc, frames, 4)

	// 关键帧间隔为 5：第 1、6 帧应为关键帧。
	if !efs[0].Keyframe || !efs[5].Keyframe {
		t.Fatalf("keyframe flags wrong: %v %v", efs[0].Keyframe, efs[5].Keyframe)
	}
	for _, ef := range efs[1:5] {
		if ef.Keyframe {
			t.Fatalf("seq=%d should be delta", ef.Seq)
		}
	}

	var dec Decoder
	for i, ef := range efs {
		got, err := dec.Decode(ef)
		if err != nil {
			t.Fatalf("decode seq=%d: %v", ef.Seq, err)
		}
		if !bytes.Equal(got.Pixels, frames[i].Pixels) {
			t.Fatalf("seq=%d pixels mismatch", ef.Seq)
		}
	}
}

func TestCodecDeltaNeedsKeyframe(t *testing.T) {
	enc := NewEncoder(10, 4)
	efs := encodeAll(t, enc, []Frame{gradientFrame(1, 2, 2), gradientFrame(2, 2, 2)}, 4)

	// 解码器从增量帧开始必须报 ErrNeedKeyframe，而不是产出花屏。
	var dec Decoder
	if _, err := dec.Decode(efs[1]); err != ErrNeedKeyframe {
		t.Fatalf("want ErrNeedKeyframe, got %v", err)
	}
	// 从关键帧开始则正常。
	if _, err := dec.Decode(efs[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := dec.Decode(efs[1]); err != nil {
		t.Fatal(err)
	}
}

func TestCodecDeterministic(t *testing.T) {
	frames := []Frame{gradientFrame(1, 8, 8), gradientFrame(2, 8, 8), gradientFrame(3, 8, 8)}
	run := func() []EncodedFrame {
		return encodeAll(t, NewEncoder(3, 4), frames, 3)
	}
	a, b := run(), run()
	for i := range a {
		if a[i].Keyframe != b[i].Keyframe || !bytes.Equal(a[i].Data, b[i].Data) {
			t.Fatalf("run not reproducible at frame %d", i)
		}
	}
}

func TestCodecQualityChangeForcesKeyframe(t *testing.T) {
	enc := NewEncoder(100, 4)
	encodeAll(t, enc, []Frame{gradientFrame(1, 2, 2)}, 4)
	efs := encodeAll(t, enc, []Frame{gradientFrame(2, 2, 2)}, 2)
	if !efs[0].Keyframe {
		t.Fatal("quality change should force keyframe")
	}
}

func TestCodecForceKeyframe(t *testing.T) {
	enc := NewEncoder(100, 4)
	encodeAll(t, enc, []Frame{gradientFrame(1, 2, 2)}, 4)
	enc.ForceKeyframe()
	efs := encodeAll(t, enc, []Frame{gradientFrame(2, 2, 2)}, 4)
	if !efs[0].Keyframe {
		t.Fatal("ForceKeyframe not honored")
	}
}

func TestCodecRejectsBadFrame(t *testing.T) {
	enc := NewEncoder(10, 4)
	if _, err := enc.Encode(Frame{Seq: 1, Width: 2, Height: 2, Pixels: []byte{1, 2, 3}}, 4); err == nil {
		t.Fatal("want error for bad pixel buffer")
	}
	if _, err := enc.Encode(Frame{Seq: 1, Width: 0, Height: 2, Pixels: nil}, 4); err == nil {
		t.Fatal("want error for bad size")
	}
}

func TestCodecLossyRoundTrip(t *testing.T) {
	// quality=0, maxQuality=4 -> shift=4，像素低 4 位被量化。
	enc := NewEncoder(10, 4)
	efs := encodeAll(t, enc, []Frame{gradientFrame(1, 2, 2)}, 0)
	var dec Decoder
	got, err := dec.Decode(efs[0])
	if err != nil {
		t.Fatal(err)
	}
	for i, v := range got.Pixels {
		if v&0x0F != 0 {
			t.Fatalf("pixel %d not quantized: %08b", i, v)
		}
	}
}
