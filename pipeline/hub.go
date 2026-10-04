package pipeline

import "sync"

// Subscriber 代表一个观看端，各自以独立进度消费。
// C 在 Unsubscribe 或 Hub.Close 后关闭，表示流结束。
type Subscriber struct {
	ID int
	C  <-chan EncodedFrame

	ch     chan EncodedFrame
	synced bool // false 表示处于失同步状态，等待下一个关键帧
}

// Hub 把同一份编码结果扇出给多个观看端。
//
// 语义：
//   - EncodedFrame 只读共享，绝不拷贝，多端复用安全；
//   - 新端接入时回放最近一个关键帧起的 GOP 缓存，保证从可独立解码位置开始；
//   - 某端队列满时丢弃该帧并使其进入失同步状态，跳过后续增量帧直到下一个
//     关键帧自动恢复 —— 慢端不会收到无法解码的增量帧（无花屏），也不拖垮快端。
type Hub struct {
	queueSize int
	onDrop    func(subID int, ef EncodedFrame, reason DropReason)

	mu     sync.Mutex
	subs   map[int]*Subscriber
	nextID int
	gop    []EncodedFrame // 最近一个关键帧起的缓存（含关键帧）
	closed bool
}

// NewHub 创建扇出中心，queueSize 为每个观看端的队列容量。
func NewHub(queueSize int, onDrop func(subID int, ef EncodedFrame, reason DropReason)) *Hub {
	if queueSize < 1 {
		queueSize = 1
	}
	return &Hub{
		queueSize: queueSize,
		onDrop:    onDrop,
		subs:      make(map[int]*Subscriber),
	}
}

// Subscribe 接入一个观看端。新端从最近一个关键帧开始，保证可独立解码。
// 可在运行中随时调用（中途接入 / 重连）。
func (h *Hub) Subscribe() *Subscriber {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.nextID++
	sub := &Subscriber{ID: h.nextID, ch: make(chan EncodedFrame, h.queueSize)}
	sub.C = sub.ch
	sub.synced = true
	// 回放 GOP 缓存；放不下则进入失同步，等下一个关键帧。
	for _, ef := range h.gop {
		select {
		case sub.ch <- ef:
		default:
			sub.synced = false
			// 已放入的增量帧不可用，清空重来。
			for len(sub.ch) > 0 {
				<-sub.ch
			}
			break
		}
		if !sub.synced {
			break
		}
	}
	if h.closed {
		close(sub.ch)
		return sub
	}
	h.subs[sub.ID] = sub
	return sub
}

// Unsubscribe 断开一个观看端并释放其资源。
func (h *Hub) Unsubscribe(id int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if sub, ok := h.subs[id]; ok {
		delete(h.subs, id)
		close(sub.ch)
	}
}

// Broadcast 向所有观看端分发一帧。编码结果只读共享，绝不拷贝。
// 返回成功投递的观看端数量。
func (h *Hub) Broadcast(ef EncodedFrame) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return 0
	}
	if ef.Keyframe {
		h.gop = h.gop[:0]
	}
	h.gop = append(h.gop, ef)

	delivered := 0
	for _, sub := range h.subs {
		if !sub.synced {
			if !ef.Keyframe {
				continue // 失同步中：跳过增量帧，等关键帧恢复
			}
			// 关键帧是重新同步的唯一机会：清空积压（旧帧只会让延迟更大），
			// 保证关键帧一定入队。被清空的帧同样计入慢端丢弃。
			for len(sub.ch) > 0 {
				stale := <-sub.ch
				if h.onDrop != nil {
					h.onDrop(sub.ID, stale, DropSubscriberSlow)
				}
			}
			sub.synced = true
		}
		select {
		case sub.ch <- ef:
			delivered++
		default:
			// 慢端：丢弃本帧并进入失同步，直到下一个关键帧。
			sub.synced = false
			if h.onDrop != nil {
				h.onDrop(sub.ID, ef, DropSubscriberSlow)
			}
		}
	}
	return delivered
}

// Close 关闭所有观看端通道。幂等。
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	for id, sub := range h.subs {
		close(sub.ch)
		delete(h.subs, id)
	}
}

// SubscriberCount 返回当前观看端数量（用于测试与观测）。
func (h *Hub) SubscriberCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}
