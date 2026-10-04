package pixelstream

import (
	"context"
	"sync"
	"time"
)

// FrameEmitter 是采集源向管线交付帧的出口。
type FrameEmitter interface {
	Emit(RawFrame) SubmitResult
}

// FrameSource 抽象本地画面源（含模拟的不稳定节奏与中断）。
type FrameSource interface {
	Run(ctx context.Context, emit FrameEmitter) error
}

// ScriptedFrame 描述脚本源中的一帧。
type ScriptedFrame struct {
	Frame RawFrame
	Delay time.Duration // 相对上一帧产出后的等待（可任意不稳定，可为 0）
}

// ScriptedSource 按脚本以可控的（不稳定）节奏产出帧；
// Pause/Resume 模拟采集中断与恢复。等待经 Clock 完成，
// 配合 ManualClock 可做确定性测试。
type ScriptedSource struct {
	clock  Clock
	frames []ScriptedFrame

	mu     sync.Mutex
	paused bool
	resume chan struct{}
}

// NewScriptedSource 用脚本帧构造源，默认使用系统时钟。
func NewScriptedSource(frames []ScriptedFrame) *ScriptedSource {
	return &ScriptedSource{clock: SystemClock{}, frames: frames, resume: make(chan struct{})}
}

// WithClock 替换时钟（测试注入手动时钟）。
func (s *ScriptedSource) WithClock(c Clock) *ScriptedSource {
	s.clock = c
	return s
}

// Pause 模拟采集中断：下一次产出前阻塞，直到 Resume。
func (s *ScriptedSource) Pause() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.paused {
		s.paused = true
		s.resume = make(chan struct{})
	}
}

// Resume 恢复采集。
func (s *ScriptedSource) Resume() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.paused {
		s.paused = false
		close(s.resume)
	}
}

// Run 依次产出全部帧。返回 nil 表示脚本正常结束；ctx 取消时返回 ctx.Err()。
func (s *ScriptedSource) Run(ctx context.Context, emit FrameEmitter) error {
	for _, sf := range s.frames {
		if sf.Delay > 0 {
			timer := s.clock.NewTimer(sf.Delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C():
			}
		}
		if err := s.waitResumed(ctx); err != nil {
			return err
		}
		// 拷贝像素，模拟采集边界，调用方与管线互不持有可变数据。
		f := sf.Frame
		if sf.Frame.Pix != nil {
			pix := make([]byte, len(sf.Frame.Pix))
			copy(pix, sf.Frame.Pix)
			f.Pix = pix
		}
		emit.Emit(f)
	}
	return nil
}

func (s *ScriptedSource) waitResumed(ctx context.Context) error {
	for {
		s.mu.Lock()
		paused := s.paused
		ch := s.resume
		s.mu.Unlock()
		if !paused {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
	}
}
