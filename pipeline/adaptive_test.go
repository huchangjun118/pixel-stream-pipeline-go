package pipeline

import "testing"

func testCfg() Config {
	cfg := DefaultConfig()
	cfg.EncodeQueueSize = 16
	cfg.MinQuality = 0
	cfg.MaxQuality = 4
	cfg.InitialQuality = 4
	return cfg
}

func TestAdaptiveDegradesUnderPressure(t *testing.T) {
	cfg := testCfg()
	c := NewAdaptiveController(cfg)
	// 持续高队列深度：质量应逐步降到 MinQuality，之后步长加大。
	q, s := c.Sample(16, 100)
	if q != 3 || s != 1 {
		t.Fatalf("after 1st high sample: q=%d s=%d", q, s)
	}
	c.Sample(16, 100)
	c.Sample(16, 100)
	c.Sample(16, 100) // q=0
	q, s = c.Sample(16, 100)
	if q != 0 || s != 2 {
		t.Fatalf("at min quality should increase stride: q=%d s=%d", q, s)
	}
	c.Sample(16, 100)
	q, s = c.Sample(16, 100)
	if q != 0 || s != 4 {
		t.Fatalf("stride should cap at 4: q=%d s=%d", q, s)
	}
}

func TestAdaptiveRecoversWithHysteresis(t *testing.T) {
	cfg := testCfg()
	c := NewAdaptiveController(cfg)
	for i := 0; i < 4; i++ {
		c.Sample(16, 100) // 降到 q=0
	}
	if c.Quality() != 0 {
		t.Fatalf("q=%d", c.Quality())
	}
	// 低水位不足 recoverAfter 次不恢复。
	for i := 0; i < recoverAfter-1; i++ {
		c.Sample(0, 100)
	}
	if c.Quality() != 0 {
		t.Fatalf("recovered too early: q=%d", c.Quality())
	}
	c.Sample(0, 100) // 第 recoverAfter 次：stride 已是 1，直接升质量
	if c.Stride() != 1 || c.Quality() != 1 {
		t.Fatalf("should recover quality: q=%d s=%d", c.Quality(), c.Stride())
	}
	for i := 0; i < recoverAfter; i++ {
		c.Sample(0, 100)
	}
	if c.Quality() != 2 {
		t.Fatalf("q=%d, want 2", c.Quality())
	}
}

func TestAdaptiveLatencyBudgetTightensWatermark(t *testing.T) {
	cfg := testCfg()
	c := NewAdaptiveController(cfg)
	// 预算只有 2 帧时，深度 2 即触发降级（默认水位是 12）。
	q, _ := c.Sample(2, 2)
	if q != 3 {
		t.Fatalf("budget should tighten watermark: q=%d", q)
	}
}

func TestAdaptiveDeterministic(t *testing.T) {
	cfg := testCfg()
	samples := []int{0, 5, 12, 16, 16, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 16}
	run := func() [][2]int {
		c := NewAdaptiveController(cfg)
		var out [][2]int
		for _, s := range samples {
			q, st := c.Sample(s, 100)
			out = append(out, [2]int{q, st})
		}
		return out
	}
	a, b := run(), run()
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("sample %d: %v != %v", i, a[i], b[i])
		}
	}
}

func TestAdaptiveDisabled(t *testing.T) {
	cfg := testCfg()
	cfg.AdaptiveEnabled = false
	c := NewAdaptiveController(cfg)
	for i := 0; i < 20; i++ {
		q, s := c.Sample(16, 1)
		if q != cfg.InitialQuality || s != 1 {
			t.Fatalf("disabled controller changed state: q=%d s=%d", q, s)
		}
	}
}
