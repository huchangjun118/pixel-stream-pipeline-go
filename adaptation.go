package pixelstream

import "time"

// AdaptController 决定质量等级与是否跳帧。为满足"相同输入与相同配置下
// 输出必须可复现"，所有过载信号都来自帧数据本身，绝不读取真实墙钟或
// 依赖协程调度时刻：
//
//	过载信号 = backlogAtArrival，即一帧进入排序缓冲那一刻，它前面已排了
//	多少帧。该值在帧到达时就被确定，后续无论编码协程何时运行都不会改变。
//
// 调整只作用于后续帧的编码质量或整帧取舍，绝不重写 PTS，因此输出时间轴
// 不跳变、可逐帧复现。
type AdaptController struct {
	cfg Config

	mu              mutex
	lastKeyPTS      time.Time
	lastKeyPTSValid bool
	quality         int
	skipPhase       int
}

func NewAdaptController(cfg Config) *AdaptController {
	return &AdaptController{cfg: cfg, quality: cfg.MaxQualityLevel}
}

// AdaptInput 是一次决策的全部输入（均为可复现数据）。
type AdaptInput struct {
	// BacklogAtArrival 该帧到达排序缓冲时，其前面已排队的帧数。
	// 由到达次序唯一确定，与协程调度无关，是确定性的拥塞信号。
	BacklogAtArrival int
	PTS              time.Time
	CurrentQuality   int
}

// AdaptDecision 是决策结论。
type AdaptDecision struct {
	QualityLevel int
	SkipFrame    bool
}

// Decide 依据"到达积压"做带迟滞的确定性决策：
//   - 积压越过容量阈值则降级，积压很大时在最低质量上确定性降帧率；
//   - 连续 lowWater 帧无积压才恢复一级，避免震荡。
func (a *AdaptController) Decide(in AdaptInput) AdaptDecision {
	a.mu.Lock()
	defer a.mu.Unlock()
	q := a.quality
	if q == 0 {
		q = in.CurrentQuality
	}
	// 唯一过载信号：帧到达时其前方已排队帧数。该值由"到达次序"唯一确定，
	// 与编码器/协程调度无关，从而保证逐帧可复现。
	depth := in.BacklogAtArrival
	capDepth := a.cfg.ReorderMaxFrames
	if capDepth <= 0 {
		capDepth = 1
	}
	heavy := depth >= capDepth*3/4
	mid := depth >= capDepth/2
	light := depth >= capDepth/4

	switch {
	case heavy:
		q = a.cfg.MinQualityLevel
		// 最低质量时确定性降帧率：每 4 帧跳 1 帧。
		a.skipPhase++
	case mid:
		if q > a.cfg.MinQualityLevel+1 {
			q -= 2
		} else if q > a.cfg.MinQualityLevel {
			q--
		}
		a.skipPhase = 0
	case light:
		if q > a.cfg.MinQualityLevel {
			q--
		}
		a.skipPhase = 0
	default:
		// 无积压：每来一帧恢复一级（最多到最高），帧率始终保持。
		if q < a.cfg.MaxQualityLevel {
			q++
		}
		a.skipPhase = 0
	}
	a.quality = q
	skip := heavy && a.skipPhase%4 == 0
	return AdaptDecision{QualityLevel: q, SkipFrame: skip}
}

// keyframeDue 判断距上一关键帧是否已到达关键帧间隔（按 PTS，确定性）。
func (a *AdaptController) keyframeDue(pts time.Time) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.lastKeyPTSValid {
		return true
	}
	return pts.Sub(a.lastKeyPTS) >= a.cfg.KeyFrameInterval
}

func (a *AdaptController) noteKeyframe(pts time.Time) {
	a.mu.Lock()
	a.lastKeyPTS = pts
	a.lastKeyPTSValid = true
	a.mu.Unlock()
}

func (a *AdaptController) qualityLevel() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.quality
}
