package pipeline

// Sequencer 把乱序、重复、迟到的输入帧整理成严格按序号递增的输出流。
// 它是纯同步组件，不含任何后台任务；相同输入序列必然产生相同输出序列。
//
// 语义：
//   - 输出序号严格递增，时间轴绝不倒流；
//   - 已输出过的序号再次到达 -> DropDuplicate；
//   - 序号小于等于已输出最大值但从未见过 -> DropLate；
//   - 缓冲窗口满时，直接输出最小序号帧（跳过缺失序号），保证时间轴持续推进。
type Sequencer struct {
	window int
	onDrop func(Frame, DropReason)

	buf         map[uint64]Frame // 乱序缓冲
	started     bool
	lastEmitted uint64

	recent    map[uint64]struct{} // 最近输出的序号，用于区分重复与迟到
	recentOrd []uint64            // recent 的 FIFO 顺序
}

// NewSequencer 创建重排器，window 为乱序缓冲窗口（帧数）。
func NewSequencer(window int, onDrop func(Frame, DropReason)) *Sequencer {
	if window < 1 {
		window = 1
	}
	return &Sequencer{
		window: window,
		onDrop: onDrop,
		buf:    make(map[uint64]Frame),
		recent: make(map[uint64]struct{}),
	}
}

func (s *Sequencer) drop(f Frame, r DropReason) {
	if s.onDrop != nil {
		s.onDrop(f, r)
	}
}

func (s *Sequencer) markEmitted(seq uint64) {
	s.recent[seq] = struct{}{}
	s.recentOrd = append(s.recentOrd, seq)
	// 保留最近 4*window 个输出序号用于重复判定，超出部分按迟到处理。
	limit := 4 * s.window
	for len(s.recentOrd) > limit {
		delete(s.recent, s.recentOrd[0])
		s.recentOrd = s.recentOrd[1:]
	}
}

// Push 输入一帧，返回当前可以按序输出的零或多帧。
func (s *Sequencer) Push(f Frame) []Frame {
	if s.started && f.Seq <= s.lastEmitted {
		if _, ok := s.recent[f.Seq]; ok {
			s.drop(f, DropDuplicate)
		} else {
			s.drop(f, DropLate)
		}
		return nil
	}
	if _, ok := s.buf[f.Seq]; ok {
		s.drop(f, DropDuplicate)
		return nil
	}
	s.buf[f.Seq] = f

	var out []Frame
	emit := func(seq uint64) {
		out = append(out, s.buf[seq])
		delete(s.buf, seq)
		s.started = true
		s.lastEmitted = seq
		s.markEmitted(seq)
	}

	if !s.started {
		// 首帧直接确定时间轴起点。
		emit(f.Seq)
	}
	// 连续序号立即输出。
	for {
		next := s.lastEmitted + 1
		if _, ok := s.buf[next]; !ok {
			break
		}
		emit(next)
	}
	// 窗口溢出：输出最小序号帧，跳过缺失序号，保证时间轴前进。
	for len(s.buf) > s.window {
		emit(s.minSeq())
	}
	return out
}

// Flush 在输入结束时排空缓冲，按序号升序返回剩余帧。
func (s *Sequencer) Flush() []Frame {
	var out []Frame
	for len(s.buf) > 0 {
		seq := s.minSeq()
		out = append(out, s.buf[seq])
		delete(s.buf, seq)
		s.started = true
		s.lastEmitted = seq
		s.markEmitted(seq)
	}
	return out
}

func (s *Sequencer) minSeq() uint64 {
	first := true
	var min uint64
	for seq := range s.buf {
		if first || seq < min {
			min = seq
			first = false
		}
	}
	return min
}
