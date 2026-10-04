# Pixel Stream Pipeline (Go)

一套纯内存、确定性可测的像素流 **采集 → 排序去重 → 编码 → 多观看端分发** 管线。
画面源在本地以不稳定节奏产出帧，管线把帧编码为关键帧/增量帧字节流并扇出给
多个观看端；端到端延迟、观看连续性、丢帧原因与自适应行为都可观测、可复现。
输入用内存通道 / 本地回环（`ScriptedSource` + 手动时钟）模拟，无真实网络依赖。

## 架构与数据流

```
 FrameSource (不稳定节奏/可中断)
      │ Emit / SubmitFrame(RawFrame{PTS, Seq, Pix})
      ▼
 rawCh ──forwarder──► ingress
                       │  ① 输入背压（有界 rawCh，满则返回 input_full）
                       │  ② 去重（相同采集序号）
                       │  ③ 迟到判定（PTS 落后已输出轴 + LateGrace 则丢弃）
                       │  ④ 按 (PTS, Seq) 重排序，保证严格递增
                       │  ⑤ 到达瞬间做确定性自适应决策（随帧携带）
                       ▼
                 orderedCh（有界，按 DropPolicy 回压/丢帧）
                       │
                       ▼
                 encoderStage（单协程串行）
                       │  关键帧/增量帧编码（FakeCodec）
                       │  PLI 强制关键帧、失败退避重试（不丢不重）
                       ├──► packetStore（最近 N 条关键帧链，供中途接入/重连）
                       └──► hub.broadcast
                                 │ 不可变包零拷贝扇出
                 ┌───────────────┼───────────────┐
                 ▼               ▼               ▼
              Viewer A        Viewer B        Viewer C
           （独立有界队列   （独立进度、     （溢出各自丢弃/
             与发送协程）     慢者不影响他人）   关键帧重对齐）
```

文件职责：

| 文件 | 职责 |
| --- | --- |
| `frame.go` | `RawFrame`、`EncodedPacket`、帧类型、`DropReason`、`FrameEvent` 等核心类型 |
| `clock.go` | `Clock`：`SystemClock`（生产）与 `ManualClock`（测试，只有显式 `Advance` 才走表） |
| `config.go` | 全部配置项、默认值与归一化 |
| `metrics.go` | 原子计数器、按原因的丢帧统计、环形帧事件流 |
| `source.go` | `FrameSource` 接口与可暂停/恢复、节奏不稳定的 `ScriptedSource` |
| `ingress.go` | 重排序缓冲（去重/迟到/排序）、编码前丢帧策略、到达点自适应 |
| `codec.go` | 确定性内存编解码器：关键帧可独立解码，增量帧依赖关键帧链，含故障注入 |
| `encoder.go` | 串行编码、PLI、失败重试、质量调整、写 store 并广播 |
| `adaptation.go` | 纯数据、确定性的自适应控制器（降质量 / 降帧率） |
| `store.go` | 最近关键帧链环形缓存，保证中途接入/重连从关键帧开始 |
| `viewer.go` | 单观看端：有界内部队列 + backlog + 独立发送协程 |
| `hub.go` | 观看端管理、关键帧背填、非阻塞扇出、PLI 合并 |
| `pipeline.go` | 生命周期编排、提交/接入 API、优雅关闭 |

## 1. 时间轴正确性（需求 1）

- 每帧携带**采集时间戳 `PTS`** 与单调**采集序号 `Seq`**；`PTS` 在整条管线中
  绝不被重写（测试 `TestAdaptation_TimelineNeverJumps` 逐帧校验）。
- 输出包带单调的 `OutputSeq`，并保留原始 `PTS` 与 `SourceSeq`。
- **重复帧**：相同 `Seq` 一律拒绝，记 `duplicate`。
- **乱序帧**：进入按 `(PTS, Seq)` 排序的小顶堆，在 `ReorderWindow` 到期或
  超过 `ReorderMaxFrames` 时按序弹出，弹出顺序 `PTS` 严格递增。
- **迟到帧**：若 `PTS + LateGrace` 早于已输出的最大 PTS，直接拒绝，记 `late`，
  输出时间轴绝不倒流。
- 每帧最终去向都写入 `Metrics.Events()`（accepted/dropped、原因、OutputSeq、
  是否重排、质量等级），任意一帧在输出中的顺序与时间都可解释。

## 2. 编码与推流解耦、回压与丢帧（需求 2、4）

- 采集端 `SubmitFrame` 只与有界输入通道 `rawCh` 交互，**与观看端速度完全无关**；
  队列满时立即返回 `DropInputFull`，调用方永不被慢观看端阻塞。
- 排序后、编码前是另一条有界通道 `orderedCh`，满时按 `DropPolicy` 处理：
  - `none`：回压（阻塞排序阶段，最终反映为输入侧 `input_full`）。
  - `latest`：保旧弃新，丢弃缓冲中最新帧。
  - `oldest`（默认）：保新弃旧，丢弃最旧帧。
  - 丢帧原因分别记 `input_full` / `ordered_full`，互不混淆。
- **观看端完全解耦**：每个 `Viewer` 有独立有界内部队列与独立发送协程。
  hub 对每个观看端的 `deliver` 都**非阻塞**；慢观看端只会让自己的队列触发策略，
  不阻塞编码器，也不影响其他观看端（`TestBackpressure_SlowViewerDoesNotBlock...`）。
  观看端侧溢出原因记 `viewer_slow`。
- `EncodedPacket.Payload` 在编码边界定型后不再修改，多个观看端共享**同一底层
  字节数组**（零拷贝，`TestPayloadSharedAcrossViewers` 校验同一引用）。

## 3. 关键帧 / 增量帧模型（需求 3）

- `FakeCodec`：
  - **关键帧 `K`**：记录完整像素快照（含尺寸与质量等级），可**独立解码**；
  - **增量帧 `P`**：携带所属关键帧 `KeyID` 与相对该链基线的差异指纹，
    **只能在匹配的关键帧之后解码**。
- 关键帧在以下情况强制产生：链首第一帧、距上一关键帧达到 `KeyFrameInterval`、
  检测到采集序号断裂（中间丢帧）、收到 PLI、编码失败恢复后的首帧。
- `packetStore` 保留最近 `StoreKeyFrames` 条**完整**关键帧链。观看端接入/重连时
  从最近关键帧开始整段背填（`Viewer.backlog`），收到的第一帧必为关键帧，
  其后增量帧链式可解码——`decodePacket` 在缺基线/链不匹配时显式报错，
  测试以此证明重连后不会花屏或错位。
- PLI（关键帧请求）通道容量为 1，多个观看端的并发请求自动**合并**且不阻塞。

## 4. 多观看端独立进度（需求 4）

- 观看端按各自速度从 `Packets()` 通道消费；快端与慢端互不影响。
- 慢端溢出策略 `ViewerOverflowPolicy`：
  - `resync`（默认）：清空积压，丢弃随后的增量帧直到下一个关键帧重新对齐；
  - `drop_oldest`：丢弃不破坏关键帧链的最旧增量帧；若会断链则退化为 `resync`；
  - `block`：允许反压（仅调试，会影响编码器）。
- `RequestResync(id)` 可让观看端在画面异常/重连时主动在下一个关键帧对齐。

## 5. 负载自适应（需求 5）

- 过载信号是 **`BacklogAtArrival`：一帧进入排序缓冲那一刻，它前面已排队的
  帧数**。该值只由“帧到达次序”决定，不读取真实墙钟、不依赖协程调度时刻。
- 决策在 **ingress 单协程、帧到达的瞬间**做出，结论（质量等级 / 是否跳帧）
  随该帧携带到编码器执行。因此：
  - **调整不产生时间轴跳变**：只改后续帧的编码质量等级，或在重负载时
    确定性地整帧跳过（`adaptive`）；从不重写 PTS，输出只是“少一些帧”。
  - **相同输入 + 相同配置必可复现**：给定相同帧序列得到逐帧一致的质量/跳帧
    轨迹（`TestAdaptation_ReproducibleSameInput`、
    `TestAdaptController_DeterministicLevels`）。
- 带迟滞的档位：到达积压达到容量的 1/4、1/2、3/4 分别降 1 级、降 2 级、压到
  最低质量并每 4 帧确定性跳过 1 帧；无积压时逐帧恢复一级，避免震荡。

## 6. 异常处理与恰好一次（需求 6）

- **采集中断**：`ScriptedSource.Pause/Resume`；管线在无帧时空闲存活，恢复后
  继续按序处理，不重复推送（`TestRecovery_SourceInterruptThenResume...`）。
- **编码失败**：`FakeCodec.FailAttempts(n)` 注入瞬时故障后，编码阶段在该帧
  上按 `EncoderRetryDelay` 退避重试，期间不取后续帧；恢复后的首帧强制关键帧。
  每帧只有编码成功才广播一次——不重复、不漏推
  （`TestRecovery_EncoderFailureRetriesAndResumesWithKeyframe`）。
- **观看端异常断开**：观看端 `ctx` 取消即由看门狗终止其发送协程并从 hub 注销，
  编码器与其他观看端不受影响（`TestRecovery_ViewerAbnormalDisconnectIsCleanedUp`）。
- 任何阶段都不会让管线进入半死状态；关闭时未能处理的排队帧记 `shutdown`。

## 7. 配置、取消与资源释放（需求 7）

`Config`（见 `config.go`，零值字段在 `New` 时替换为默认值）：

| 配置项 | 默认 | 含义 |
| --- | --- | --- |
| `InputQueueDepth` | 64 | 采集输入通道深度；采集端只受它约束 |
| `OrderedQueueDepth` | 8 | 排序后/编码前通道深度，编码前回压围绕它工作 |
| `KeyFrameInterval` | 1s | 关键帧最大间隔（按 PTS） |
| `ReorderWindow` | 50ms | 为等待迟到帧而暂缓输出的窗口（按时钟） |
| `ReorderMaxFrames` | 64 | 重排缓冲最大帧数，超过即强制按序输出 |
| `LateGrace` | 200ms | PTS 落后已输出轴超过该宽限即判迟到 |
| `LatencyTarget` | 200ms | 端到端延迟目标（暴露为自适应档位基准） |
| `AdaptationPeriod` | 200ms | 自适应评估周期（当前过载由帧驱动，周期仅保留钩子） |
| `EncoderRetryDelay` | 10ms | 编码失败后的退避重试间隔 |
| `MaxQualityLevel` / `MinQualityLevel` | 5 / 1 | 质量等级范围，越大越好 |
| `StoreKeyFrames` | 4 | 保留的关键帧链数量，决定重连可回溯多远 |
| `ViewerQueueDepth` | 30 | 每个观看端独立队列深度 |
| `DropPolicy` | oldest | 编码前积压策略：none/latest/oldest |
| `ViewerPolicy` | resync | 观看端溢出策略：block/drop_oldest/resync |

关闭：`Pipeline.Close(ctx)` 取消全部后台任务，等待在途帧处理或 `ctx` 超时，
关闭全部观看端并等待协程退出、释放缓冲；可重复调用。
`TestClose_ReleasesAllGoroutines` 校验关闭后协程数不增长（无泄漏）。

## 8. 可观测性

`Metrics()` 快照包含：提交/接受/丢弃/编码帧数、关键帧数、增量帧数、
编码失败数、观看端接入与关闭数、重对齐次数、PLI 次数，以及
**按 `DropReason` 分类的丢帧计数**。`Events()` 返回环形缓冲内每帧的去向事件。

## 测试

```bash
go test ./...            # 全量
go test -race ./...      # 竞态检测
go test -count=10 ./...  # 稳定性（测试含手动时钟，结果确定）
```

覆盖：时间戳乱序/重复/迟到、回压与丢弃原因区分、慢观看端隔离与编码结果复用、
中途接入与重连的关键帧对齐、负载自适应的降质/降帧/可复现/时间轴不跳变、
采集中断/编码失败/观看端异常断开的恢复、关闭协程泄漏。
