package pixelstream

import "sync"

// packetStore 保留最近若干条完整的关键帧链（关键帧 + 其后全部增量帧），
// 供观看端中途接入或重连时，从一个可独立解码的关键帧位置开始。
// EncodedPacket 不可变，多观看端共享同一引用，零拷贝扇出。
type packetStore struct {
	cfg Config

	mu      sync.Mutex
	packets []EncodedPacket // 按 OutputSeq 单调
	keySeqs []uint64        // 已保留链的关键帧 OutputSeq（升序）
}

func newPacketStore(cfg Config) *packetStore {
	return &packetStore{cfg: cfg}
}

// add 追加已编码包；当关键帧链超过 StoreKeyFrames 时，淘汰最旧的整条链。
func (s *packetStore) add(p EncodedPacket) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.packets = append(s.packets, p)
	if p.Type == TypeKey {
		s.keySeqs = append(s.keySeqs, p.OutputSeq)
	}
	for len(s.keySeqs) > s.cfg.StoreKeyFrames {
		// 最旧链：从 keySeqs[0]（含）到 keySeqs[1]（不含）。
		dropStart := s.keySeqs[0]
		dropEnd := s.keySeqs[1]
		s.keySeqs = s.keySeqs[1:]
		cut := 0
		for cut < len(s.packets) && s.packets[cut].OutputSeq < dropEnd && s.packets[cut].OutputSeq >= dropStart {
			cut++
		}
		if cut > 0 {
			s.packets = append([]EncodedPacket(nil), s.packets[cut:]...)
		}
	}
}

// latestKeyframe 返回最近的关键帧。
func (s *packetStore) latestKeyframe() (EncodedPacket, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := len(s.packets) - 1; i >= 0; i-- {
		if s.packets[i].Type == TypeKey {
			return s.packets[i], true
		}
	}
	return EncodedPacket{}, false
}

// replaySince 返回最近关键帧链中 OutputSeq > afterSeq 的包拷贝序列。
// 保证非空结果的第一个包一定是关键帧，使观看端从可独立解码位置开始。
func (s *packetStore) replaySince(afterSeq uint64) []EncodedPacket {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.packets) == 0 {
		return nil
	}
	// 找到最近关键帧，并回退到其链头位置。
	begin := 0
	found := false
	for i := len(s.packets) - 1; i >= 0; i-- {
		if s.packets[i].Type == TypeKey {
			begin = i
			found = true
			break
		}
	}
	if !found {
		return nil
	}
	out := make([]EncodedPacket, 0, len(s.packets)-begin)
	for _, p := range s.packets[begin:] {
		if p.OutputSeq <= afterSeq {
			continue
		}
		out = append(out, p)
	}
	return out
}
