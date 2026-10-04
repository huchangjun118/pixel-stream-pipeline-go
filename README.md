# pixel-stream-pipeline-go

一套像素流的**采集 → 重排 → 编码 → 扇出**管线。画面源以不稳定的节奏经内存通道产出帧，
管线把画面编码成字节流并推送给多个观看端，端到端延迟与观看连续性均可控可测。

```
采集端 --Frame--> [Sequencer] --有序帧--> [自适应节流] --encodeItem--> [Encoder] --EncodedFrame--> [Hub] ==> 观看端 × N
                   乱序重排                 质量/帧率调节    关键帧/增量帧        每端独立队列
```

## 快速开始

```go
cfg := pipeline.DefaultConfig()
p, _ := pipeline.New(cfg)

fast := p.Subscribe()          // 观看端可随时接入
go p.Run(context.Background()) // 启动管线

p.Input() <- pipeline.Frame{Seq: 1, CapturedAt: time.Now(), Width: 8, Height: 8, Pixels: px}
close(p.Input())               // 关闭输入 = 采集源结束，管线排空后退出
```

运行演示：`go run ./cmd/demo`（内存通道模拟不稳定采集源 + 一快一慢两个观看端）。
运行测试：`go test -race ./...`

## 时间轴语义（重排器）

- 每帧携带采集序号 `Seq` 与采集时间戳 `CapturedAt`；输出序号**严格递增**，时间轴绝不倒流。
- 乱序帧在 `ReorderWindow` 窗口内缓冲重排；窗口溢出时跳过缺失序号直接输出最小序号帧，
  保证时间轴持续推进（缺失帧按丢失处理，不会卡住后续帧）。
- 重复帧（序号已输出或已在缓冲中）丢弃，计 `DropDuplicate`；
  迟到帧（序号小于已输出最大值且从未见过）丢弃，计 `DropLate`。

## 编码模型

确定性的关键帧/增量帧模型（`pipeline/codec.go`）：

- **量化**：质量等级 `quality ∈ [MinQuality, MaxQuality]`，位移 `shift = MaxQuality - quality`，
  每个像素字节丢弃低 `shift` 位；`quality == MaxQuality` 时无损。
- **关键帧**：量化像素直接 RLE 压缩，可独立解码。
- **增量帧**：量化像素与上一帧的量化重建值逐字节 XOR 后 RLE 压缩，必须基于参考帧解码。
- 每 `KeyframeInterval` 帧、质量变化、尺寸变化、编码失败恢复或显式 `ForceKeyframe` 时输出关键帧。
- **可复现性**：编解码无任何随机数与墙钟依赖，
  相同输入帧序列 + 相同质量序列 ⇒ 相同字节输出（见 `TestPipelineReproducible`）。
- 解码端在没有参考帧时收到增量帧会返回 `ErrNeedKeyframe`，而不是产出花屏。

## 回压语义

采集端与编码端、编码端与观看端全部经有界队列解耦，**任何慢环节都不会阻塞采集端**：

| 积压点 | 策略 | 丢弃原因 |
| --- | --- | --- |
| 编码队列满 | `DropOldest`：顶掉最旧帧保新鲜度；`DropNewest`：丢弃新帧保连续性 | `DropEncodeQueueFull` |
| 自适应抽帧 | 负载高时每 `stride` 帧只取 1 帧 | `DropAdaptiveThrottle` |
| 观看端队列满 | 丢弃该帧并使该端**失同步**，跳过后续增量帧直到下一关键帧自动恢复；恢复时清空其积压 | `DropSubscriberSlow` |
| 编码失败 | 跳过该帧并强制下一帧为关键帧 | `DropEncodeError` |

观看端侧的关键不变式：**失同步的观看端绝不会收到无法解码的增量帧**（无花屏、无错位），
且慢端只影响自己，不拖垮快端与编码器。同一份 `EncodedFrame` 对所有观看端只读共享，零拷贝。

## 中途接入与重连

Hub 缓存最近一个关键帧起的 GOP。`Subscribe()` 可在任意时刻调用，
新观看端从 GOP 缓存（或下一个关键帧）开始消费，总是落在可独立解码的位置。
异常断开后重新 `Subscribe()` 即完成重连，不会重复推送已消费内容，也不会漏推新 GOP。

## 自适应调节

`AdaptiveController` 是**确定性状态机**（相同样本序列 ⇒ 相同决策序列）：

- 延迟目标换算：`budget = LatencyTarget / 采集帧间隔`（帧间隔取自采集时间戳，不依赖墙钟），
  高水位 `high = min(3/4 × EncodeQueueSize, budget)`，低水位 `low = high/4`。
- 队列深度 ≥ 高水位：立即降一级质量；质量触底后加大抽帧步长（最大 4）。
- 队列深度 ≤ 低水位且连续 8 次：先减步长、再升质量（滞回防抖）。
- 调节只作用于后续帧，不回改历史，**时间轴不会跳变**。

## 配置项

| 字段 | 含义 | 默认 |
| --- | --- | --- |
| `ReorderWindow` | 乱序重排窗口（帧） | 8 |
| `EncodeQueueSize` | 编码队列容量 | 16 |
| `SubscriberQueueSize` | 每个观看端队列容量 | 8 |
| `KeyframeInterval` | 关键帧间隔（帧） | 30 |
| `DropPolicy` | 编码队列满时 `DropOldest` / `DropNewest` | `DropOldest` |
| `MinQuality` / `MaxQuality` / `InitialQuality` | 质量等级范围与初值 | 0 / 4 / 4 |
| `LatencyTarget` | 端到端延迟目标（换算队列预算） | 200ms |
| `AdaptiveEnabled` | 是否启用自适应调节 | true |

## 故障处理与生命周期

- **采集源中断**：关闭 `Input()` 通道，管线排空重排缓冲与编码队列后退出，不漏推不重复。
- **编码失败**：跳过坏帧、计数、强制下一帧关键帧，管线继续运行，不进入半死状态。
- **观看端异常断开**：`Unsubscribe(id)` 释放其队列；不断开也不影响其他端。
- **取消与退出**：`Run(ctx)` 在 ctx 取消或输入排空后返回，返回时两个后台 goroutine
  均已退出、所有订阅通道已关闭，无泄漏的后台任务或未释放缓冲。
- **可观测性**：`Metrics()` 返回输入/重排/编码/扇出计数与按原因区分的丢弃计数。

## 测试覆盖

`go test -race ./...` 覆盖：时间戳乱序 / 重复 / 迟到、窗口溢出跳缺口、编解码往返与
确定性、增量帧缺参考报错、回压与慢端重同步、中途接入与重连、负载自适应降级与恢复滞回、
编码失败恢复、采集源中断排空、取消无泄漏、端到端可复现性。
