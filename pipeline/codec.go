package pipeline

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// 编码模型：
//   - 质量等级 quality ∈ [MinQuality, MaxQuality]，位移量 shift = maxQuality - quality，
//     每个像素字节丢弃低 shift 位（量化），shift=0 时无损；
//   - 关键帧：量化像素直接 RLE 压缩；
//   - 增量帧：量化像素与上一帧的量化重建值逐字节 XOR 后 RLE 压缩；
//   - 质量变化、尺寸变化或显式 ForceKeyframe 时输出关键帧；
//   - 编解码均为纯函数式确定性逻辑：相同输入帧序列 + 相同质量序列 => 相同字节输出。
//
// 字节格式：'P' 'S' version(1) flags(bit0=keyframe) shift width(4) height(4) payload(RLE)。

const (
	codecMagic0   = 'P'
	codecMagic1   = 'S'
	codecVersion  = 1
	flagKeyframe  = 1 << 0
	codecHeaderSz = 13
)

// ErrNeedKeyframe 表示在没有参考帧时收到增量帧。
var ErrNeedKeyframe = errors.New("pipeline: delta frame without reference, need keyframe")

// Encoder 是关键帧/增量帧编码器。非并发安全，由管线单 goroutine 使用。
type Encoder struct {
	keyframeInterval int
	maxQuality       int

	ref      []byte // 上一帧量化后的重建像素
	width    int
	height   int
	quality  int
	hasRef   bool
	sinceKey int  // 距上个关键帧的帧数
	forceKey bool // 强制下一帧为关键帧
}

// NewEncoder 创建编码器。keyframeInterval 为关键帧间隔，maxQuality 用于
// 把质量等级换算为量化位移（shift = maxQuality - quality）。
func NewEncoder(keyframeInterval, maxQuality int) *Encoder {
	if keyframeInterval < 1 {
		keyframeInterval = 1
	}
	return &Encoder{keyframeInterval: keyframeInterval, maxQuality: maxQuality}
}

// Encode 编码一帧。quality 为质量等级（越大越好）。返回的 EncodedFrame 可只读共享。
func (e *Encoder) Encode(f Frame, quality int) (EncodedFrame, error) {
	if f.Width <= 0 || f.Height <= 0 {
		return EncodedFrame{}, fmt.Errorf("pipeline: invalid frame size %dx%d", f.Width, f.Height)
	}
	if len(f.Pixels) != f.Width*f.Height*4 {
		return EncodedFrame{}, fmt.Errorf("pipeline: pixel buffer size %d, want %d", len(f.Pixels), f.Width*f.Height*4)
	}
	shift := e.maxQuality - quality
	if shift < 0 {
		shift = 0
	}
	if shift > 7 {
		shift = 7
	}

	quant := quantize(f.Pixels, uint(shift))
	keyframe := e.forceKey ||
		!e.hasRef ||
		e.sinceKey+1 >= e.keyframeInterval ||
		e.quality != quality ||
		e.width != f.Width || e.height != f.Height

	var payload []byte
	if keyframe {
		payload = rleEncode(quant)
	} else {
		delta := make([]byte, len(quant))
		for i := range quant {
			delta[i] = quant[i] ^ e.ref[i]
		}
		payload = rleEncode(delta)
	}

	buf := make([]byte, codecHeaderSz+len(payload))
	buf[0] = codecMagic0
	buf[1] = codecMagic1
	buf[2] = codecVersion
	if keyframe {
		buf[3] = flagKeyframe
	}
	buf[4] = byte(shift)
	binary.BigEndian.PutUint32(buf[5:9], uint32(f.Width))
	binary.BigEndian.PutUint32(buf[9:13], uint32(f.Height))
	copy(buf[codecHeaderSz:], payload)

	// 更新参考帧为量化重建值（与解码端看到的一致）。
	e.ref = quant
	e.width, e.height = f.Width, f.Height
	e.quality = quality
	e.hasRef = true
	e.forceKey = false
	if keyframe {
		e.sinceKey = 0
	} else {
		e.sinceKey++
	}

	return EncodedFrame{
		Seq:        f.Seq,
		CapturedAt: f.CapturedAt,
		Keyframe:   keyframe,
		Quality:    quality,
		Data:       buf,
	}, nil
}

// ForceKeyframe 强制下一帧编码为关键帧（用于观看端接入或异常恢复）。
func (e *Encoder) ForceKeyframe() { e.forceKey = true }

// Decoder 用于解码验证，可从任意关键帧开始独立解码。非并发安全。
type Decoder struct {
	ref    []byte
	width  int
	height int
	hasRef bool
}

// Decode 解码一帧，返回重建帧（像素为量化后的值）。
func (d *Decoder) Decode(ef EncodedFrame) (Frame, error) {
	if len(ef.Data) < codecHeaderSz {
		return Frame{}, errors.New("pipeline: encoded frame too short")
	}
	b := ef.Data
	if b[0] != codecMagic0 || b[1] != codecMagic1 || b[2] != codecVersion {
		return Frame{}, errors.New("pipeline: bad codec magic or version")
	}
	keyframe := b[3]&flagKeyframe != 0
	w := int(binary.BigEndian.Uint32(b[5:9]))
	h := int(binary.BigEndian.Uint32(b[9:13]))
	n := w * h * 4

	raw, err := rleDecode(b[codecHeaderSz:], n)
	if err != nil {
		return Frame{}, err
	}

	var pixels []byte
	if keyframe {
		pixels = raw
	} else {
		if !d.hasRef || d.width != w || d.height != h {
			return Frame{}, ErrNeedKeyframe
		}
		pixels = make([]byte, n)
		for i := range raw {
			pixels[i] = raw[i] ^ d.ref[i]
		}
	}
	d.ref = pixels
	d.width, d.height = w, h
	d.hasRef = true

	return Frame{
		Seq:        ef.Seq,
		CapturedAt: ef.CapturedAt,
		Width:      w,
		Height:     h,
		Pixels:     pixels,
	}, nil
}

// quantize 返回丢弃低 shift 位后的像素副本。
func quantize(src []byte, shift uint) []byte {
	if shift == 0 {
		out := make([]byte, len(src))
		copy(out, src)
		return out
	}
	out := make([]byte, len(src))
	mask := byte(0xFF << shift)
	for i, v := range src {
		out[i] = v & mask
	}
	return out
}

// rleEncode 行程编码：(count, value) 对，count ∈ [1,255]。
func rleEncode(src []byte) []byte {
	out := make([]byte, 0, len(src)/2+2)
	for i := 0; i < len(src); {
		v := src[i]
		n := 1
		for i+n < len(src) && src[i+n] == v && n < 255 {
			n++
		}
		out = append(out, byte(n), v)
		i += n
	}
	return out
}

func rleDecode(src []byte, want int) ([]byte, error) {
	out := make([]byte, 0, want)
	for i := 0; i+1 < len(src); i += 2 {
		n := int(src[i])
		v := src[i+1]
		for j := 0; j < n; j++ {
			out = append(out, v)
		}
	}
	if len(src)%2 != 0 {
		return nil, errors.New("pipeline: corrupt RLE stream (odd length)")
	}
	if len(out) != want {
		return nil, fmt.Errorf("pipeline: decoded %d bytes, want %d", len(out), want)
	}
	return out, nil
}
