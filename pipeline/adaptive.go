package pipeline

// AdaptiveController 根据编码队列深度（端到端延迟的代理指标）调节质量与帧率。
//
// 它是确定性状态机：相同的样本序列必然产生相同的决策序列，
// 因此相同输入与相同配置下管线输出可复现。
//
// 决策规则（high = min(3/4 队列容量, 延迟目标换算的帧预算)，low = high/4）：
//   - 队列深度 >= high：立即降一级质量；质量已到 MinQuality 则加大抽帧步长；
//   - 队列深度 <= low 且连续 recoverAfter 次：先减小抽帧步长，再升一级质量；
//   - 调节只改变后续帧的质量/抽帧，不回改历史，时间轴不会跳变。
type AdaptiveController struct {
	cfg Config

	quality int
	stride  int
	lowRun  int // 连续低水位样本计数
}

const (
	maxStride    = 4 // 最大抽帧步长（每 4 帧取 1 帧）
	recoverAfter = 8 // 连续低水位多少次才恢复一级
)

// NewAdaptiveController 创建自适应控制器。
func NewAdaptiveController(cfg Config) *AdaptiveController {
	return &AdaptiveController{
		cfg:     cfg,
		quality: cfg.InitialQuality,
		stride:  1,
	}
}

// Sample 输入一次观测，返回当前应采用的质量与抽帧步长。
// queueDepth 为当前编码队列深度；budget 为按延迟目标与采集帧间隔换算出的
// 队列帧预算（队列中积压超过 budget 帧即意味着端到端延迟超出目标）。
// stride=1 表示不抽帧，stride=k 表示每 k 帧只取 1 帧。
func (c *AdaptiveController) Sample(queueDepth, budget int) (quality int, stride int) {
	if !c.cfg.AdaptiveEnabled {
		return c.quality, c.stride
	}
	high := c.cfg.EncodeQueueSize * 3 / 4
	if budget < high {
		high = budget
	}
	if high < 1 {
		high = 1
	}
	low := high / 4

	switch {
	case queueDepth >= high:
		c.lowRun = 0
		if c.quality > c.cfg.MinQuality {
			c.quality--
		} else if c.stride < maxStride {
			c.stride++
		}
	case queueDepth <= low:
		c.lowRun++
		if c.lowRun >= recoverAfter {
			c.lowRun = 0
			if c.stride > 1 {
				c.stride--
			} else if c.quality < c.cfg.MaxQuality {
				c.quality++
			}
		}
	default:
		c.lowRun = 0
	}
	return c.quality, c.stride
}

// Quality 返回当前质量等级。
func (c *AdaptiveController) Quality() int { return c.quality }

// Stride 返回当前抽帧步长。
func (c *AdaptiveController) Stride() int { return c.stride }
