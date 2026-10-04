package pixelstream

import (
	"sync"
	"time"
)

// Clock 抽象时间与定时器，便于用手动时钟做确定性测试。
// 生产使用 SystemClock（墙钟）；测试使用 ManualClock（只有显式 Advance 才前进）。
type Clock interface {
	Now() time.Time
	NewTicker(d time.Duration) Ticker
	NewTimer(d time.Duration) Timer
}

// Ticker 与 time.Ticker 语义一致，但可由手动时钟驱动。
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// Timer 与 time.Timer 语义一致，但可由手动时钟驱动。
type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

// ---------------------------------------------------------------------------
// SystemClock
// ---------------------------------------------------------------------------

type systemTicker struct{ t *time.Ticker }

func (t *systemTicker) C() <-chan time.Time { return t.t.C }
func (t *systemTicker) Stop()               { t.t.Stop() }

type systemTimer struct{ t *time.Timer }

func (t *systemTimer) C() <-chan time.Time { return t.t.C }
func (t *systemTimer) Stop() bool          { return t.t.Stop() }

// SystemClock 使用真实墙钟，用于生产路径。
type SystemClock struct{}

func (SystemClock) Now() time.Time                   { return time.Now() }
func (SystemClock) NewTicker(d time.Duration) Ticker { return &systemTicker{t: time.NewTicker(d)} }
func (SystemClock) NewTimer(d time.Duration) Timer   { return &systemTimer{t: time.NewTimer(d)} }

// ---------------------------------------------------------------------------
// ManualClock
// ---------------------------------------------------------------------------

type manualEntry struct {
	due   time.Time
	every time.Duration // >0 表示 ticker
	ch    chan time.Time
	stop  bool
}

// ManualClock 是测试用手动时钟：只有 Advance 被调用时时间才前进，
// 到期的 ticker/timer 同步触发。相同的 Advance 序列必然产生相同的触发序列，
// 从而保证相同输入下管线行为可复现。
type ManualClock struct {
	mu  sync.Mutex
	now time.Time
	seq int
	all map[int]*manualEntry
}

// NewManualClock 从 start 时刻创建手动时钟。
func NewManualClock(start time.Time) *ManualClock {
	return &ManualClock{now: start, all: make(map[int]*manualEntry)}
}

func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

type manualTicker struct {
	c  *ManualClock
	id int
	ch chan time.Time
}

func (t *manualTicker) C() <-chan time.Time { return t.ch }
func (t *manualTicker) Stop() {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	delete(t.c.all, t.id)
}

type manualTimer struct {
	c  *ManualClock
	id int
	ch chan time.Time
}

func (t *manualTimer) C() <-chan time.Time { return t.ch }
func (t *manualTimer) Stop() bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	e, ok := t.c.all[t.id]
	if !ok || e.stop {
		return false
	}
	e.stop = true
	delete(t.c.all, t.id)
	return true
}

func (c *ManualClock) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		panic("non-positive interval for NewTicker")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	id := c.seq
	ch := make(chan time.Time, 1) // size=1：触发合并，慢消费者不会堆积多个时刻
	c.all[id] = &manualEntry{due: c.now.Add(d), every: d, ch: ch}
	return &manualTicker{c: c, id: id, ch: ch}
}

func (c *ManualClock) NewTimer(d time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	id := c.seq
	ch := make(chan time.Time, 1)
	c.all[id] = &manualEntry{due: c.now.Add(d), ch: ch}
	return &manualTimer{c: c, id: id, ch: ch}
}

// Advance 把时钟推进 d，并在新的时刻触发所有到期项。
// 单个 Advance 内，ticker 最多投递一次（合并），timer 触发后移除。
func (c *ManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	var fires []fireEntry
	for id, e := range c.all {
		if e.stop {
			delete(c.all, id)
			continue
		}
		if !now.Before(e.due) {
			fires = append(fires, fireEntry{id, e})
		}
	}
	// 按到期时刻、再按创建顺序触发，保证确定性。
	// 简单起见按 id 排序（id 即创建顺序）；同一次 Advance 内时刻差异不影响语义。
	sortFires(fires)
	for _, f := range fires {
		e := f.e
		if e.every > 0 {
			// 合并：仅当消费者已取走上一次触发时才投递。
			select {
			case e.ch <- now:
				e.due = e.due.Add(e.every)
				// 若已经落后多次，对齐到 now 之后的下一个到期点。
				for !now.Before(e.due) {
					e.due = e.due.Add(e.every)
				}
			default:
				e.due = now.Add(e.every)
			}
		} else {
			select {
			case e.ch <- now:
			default:
			}
			delete(c.all, f.id)
		}
	}
	c.mu.Unlock()
}

type fireEntry struct {
	id int
	e  *manualEntry
}

func sortFires(f []fireEntry) {
	// 插入排序，规模很小。
	for i := 1; i < len(f); i++ {
		for j := i; j > 0 && f[j-1].id > f[j].id; j-- {
			f[j-1], f[j] = f[j], f[j-1]
		}
	}
}
