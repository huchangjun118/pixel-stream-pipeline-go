package pixelstream

import "testing"

func TestStore_KeepsRecentChainsAndReplaysFromKey(t *testing.T) {
	cfg := DefaultConfig()
	cfg.StoreKeyFrames = 2
	s := newPacketStore(cfg)
	mk := func(seq uint64, typ FrameType) EncodedPacket {
		return EncodedPacket{OutputSeq: seq, KeyID: seq, Type: typ}
	}
	// 三条链：K1 P2 | K3 P4 | K5
	s.add(mk(1, TypeKey))
	s.add(mk(2, TypeDelta))
	s.add(mk(3, TypeKey))
	s.add(mk(4, TypeDelta))
	s.add(mk(5, TypeKey))

	got := s.replaySince(0)
	if len(got) == 0 || got[0].Type != TypeKey || got[0].OutputSeq != 5 {
		t.Fatalf("replay must start at newest keyframe 5, got %+v", got)
	}
	// 最旧链 seq1/2 应已被淘汰。
	for _, p := range got {
		if p.OutputSeq < 3 {
			t.Fatalf("old chain packet %d should have been evicted", p.OutputSeq)
		}
	}
}
