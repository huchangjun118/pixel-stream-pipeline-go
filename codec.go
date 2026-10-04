package pixelstream

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
)

// ErrEncoderFailed 表示编码器对某帧编码失败（测试可注入）。
var ErrEncoderFailed = errors.New("encoder failed")

// Encoder 是关键帧/增量帧编码模型的抽象。
type Encoder interface {
	Encode(ctx context.Context, f RawFrame, forceKey bool) (EncodedPacket, error)
}

// FakeCodec 是确定性的内存"编码器"：
//   - 关键帧（K）记录完整像素快照与质量等级，可独立解码；
//   - 增量帧（P）记录相对本链最近关键帧基线的逐字节差异，只能在 K 之后解码；
//   - 输出字节仅由 (像素内容, 质量等级, 帧类型) 决定，相同输入必然产生相同字节，
//     从而满足可复现要求，也让观看端能严格校验"缺基线不可解码"。
type FakeCodec struct {
	width        int
	height       int
	qualityLv    int
	failUntilSeq uint64
	failAttempts int // >0 时，接下来 N 次编码尝试失败，随后自动恢复

	hasChain      bool
	keyOutputSeq  uint64
	outSeq        uint64
	lastSourceSeq uint64
	lastPTSValid  bool
	lastPTS       int64
	baselinePix   []byte
	baselineW     int
	baselineH     int
}

func NewFakeCodec(width, height, qualityLv int) *FakeCodec {
	return &FakeCodec{width: width, height: height, qualityLv: qualityLv}
}

const (
	magicKey   byte = 'K'
	magicDelta byte = 'P'
)

// Encode 编码一帧。forceKey 为 true 时强制开始新的关键帧链。
// 帧链断裂（SourceSeq 不连续）时自动插入关键帧，避免下游花屏/错位。
func (c *FakeCodec) Encode(ctx context.Context, f RawFrame, forceKey bool) (EncodedPacket, error) {
	if err := ctx.Err(); err != nil {
		return EncodedPacket{}, err
	}
	if c.failAttempts > 0 {
		c.failAttempts--
		return EncodedPacket{}, fmt.Errorf("%w: transient attempt left=%d seq=%d",
			ErrEncoderFailed, c.failAttempts, f.Seq)
	}
	if c.failUntilSeq > 0 && f.Seq <= c.failUntilSeq {
		return EncodedPacket{}, fmt.Errorf("%w: sourceSeq=%d", ErrEncoderFailed, f.Seq)
	}

	chainBreak := c.hasChain && f.Seq != c.lastSourceSeq+1
	ptsRegress := c.lastPTSValid && f.PTS.UnixNano() < c.lastPTS
	isKey := forceKey || !c.hasChain || chainBreak || ptsRegress

	c.outSeq++
	p := EncodedPacket{
		OutputSeq: c.outSeq,
		Type:      TypeDelta,
		SourceSeq: f.Seq,
		PTS:       f.PTS,
		Width:     f.Width,
		Height:    f.Height,
		QualityLv: c.qualityLv,
	}

	if isKey {
		c.keyOutputSeq = c.outSeq
		c.baselinePix = append(c.baselinePix[:0], f.Pix...)
		c.baselineW, c.baselineH = f.Width, f.Height
		p.Type = TypeKey
		p.KeyID = c.outSeq
		p.Payload = c.encodeKeyframe(f)
	} else {
		p.Type = TypeDelta
		p.KeyID = c.keyOutputSeq
		p.Payload = c.encodeDelta(f)
	}

	c.hasChain = true
	c.lastSourceSeq = f.Seq
	c.lastPTS = f.PTS.UnixNano()
	c.lastPTSValid = true
	return p, nil
}

// setQuality 在不中断时间轴、不重写 PTS 的前提下调整质量等级。
func (c *FakeCodec) setQuality(lv int) { c.qualityLv = lv }

// FailUntilSeq 注入故障：SourceSeq <= seq 的帧强制编码失败（测试用）。
func (c *FakeCodec) FailUntilSeq(seq uint64) { c.failUntilSeq = seq }

// FailAttempts 注入瞬时故障：接下来 n 次编码尝试失败，之后自动恢复。
// 用于验证"编码失败→退避重试→关键帧恢复且不重复不漏推"。
func (c *FakeCodec) FailAttempts(n int) { c.failAttempts = n }

// effectiveBytes 按质量等级对像素做确定性截断/抽样：质量越低字节越少，
// 仅改变编码"体积/精度"，绝不改变时间戳，也不跳时间轴。
func (c *FakeCodec) effectiveBytes(pix []byte) []byte {
	lv := c.qualityLv
	if lv < 1 {
		lv = 1
	}
	step := 1 << max(0, 5-lv) // lv=5 -> 1, lv=1 -> 16
	if step <= 1 {
		return pix
	}
	out := make([]byte, 0, len(pix)/step+1)
	for i := 0; i < len(pix); i += step {
		out = append(out, pix[i])
	}
	return out
}

func (c *FakeCodec) encodeKeyframe(f RawFrame) []byte {
	b := effective(f.Pix, c.qualityLv)
	buf := make([]byte, 0, 16+len(b))
	buf = append(buf, magicKey)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(f.Width))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(f.Height))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(c.qualityLv))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(b)))
	buf = append(buf, b...)
	return buf
}

func (c *FakeCodec) encodeDelta(f RawFrame) []byte {
	b := effective(f.Pix, c.qualityLv)
	base := effective(c.baselinePix, c.qualityLv)
	h := fnv.New64a()
	h.Write(b)
	buf := make([]byte, 0, 32)
	buf = append(buf, magicDelta)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(f.Width))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(f.Height))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(c.qualityLv))
	buf = binary.LittleEndian.AppendUint64(buf, h.Sum64())
	n := min(len(b), len(base))
	var diffCount uint32
	// 先占位，最后回填差异计数。
	idx := len(buf)
	buf = binary.LittleEndian.AppendUint32(buf, 0)
	for i := 0; i < n; i++ {
		if b[i] != base[i] {
			buf = binary.LittleEndian.AppendUint32(buf, uint32(i))
			buf = append(buf, b[i])
			diffCount++
		}
	}
	binary.LittleEndian.PutUint32(buf[idx:idx+4], diffCount)
	return buf
}

func effective(pix []byte, lv int) []byte {
	step := 1 << max(0, 5-lv)
	if step <= 1 {
		return pix
	}
	out := make([]byte, 0, len(pix)/step+1)
	for i := 0; i < len(pix); i += step {
		out = append(out, pix[i])
	}
	return out
}

var (
	errNeedKeyframe = errors.New("delta packet without matching keyframe baseline")
	errBadPayload   = errors.New("malformed codec payload")
)

// decodePacket 供测试侧"解码"校验：
//   - 关键帧可独立解码，返回其快照字节；
//   - 增量帧必须携带 KeyID 与基线一致的关键帧，否则返回错误
//     （以此证明缺基线时不会花屏，只能等待关键帧对齐）。
func decodePacket(p EncodedPacket, baselineKey *EncodedPacket) ([]byte, error) {
	if len(p.Payload) == 0 {
		return nil, errBadPayload
	}
	switch p.Payload[0] {
	case magicKey:
		if len(p.Payload) < 17 {
			return nil, errBadPayload
		}
		n := binary.LittleEndian.Uint32(p.Payload[13:17])
		if uint32(len(p.Payload)-17) < n {
			return nil, errBadPayload
		}
		out := make([]byte, n)
		copy(out, p.Payload[17:17+n])
		return out, nil
	case magicDelta:
		const deltaHeaderLen = 25
		if baselineKey == nil || baselineKey.Type != TypeKey || baselineKey.OutputSeq != p.KeyID {
			return nil, errNeedKeyframe
		}
		if len(p.Payload) < deltaHeaderLen {
			return nil, errBadPayload
		}
		// 校验 diff 记录区长度与 diffCount 自洽（每条记录 5 字节）。
		diffCount := binary.LittleEndian.Uint32(p.Payload[21:25])
		if uint32(len(p.Payload)-deltaHeaderLen) != diffCount*5 {
			return nil, errBadPayload
		}
		// 解码"成功"：返回基线在该质量下的有效字节，模拟重建。
		if len(baselineKey.Payload) < 17 {
			return nil, errBadPayload
		}
		n := binary.LittleEndian.Uint32(baselineKey.Payload[13:17])
		out := make([]byte, n)
		copy(out, baselineKey.Payload[17:17+n])
		return out, nil
	default:
		return nil, errBadPayload
	}
}
