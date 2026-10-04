package pixelstream

import (
	"context"
	"sync"
)

// hub 管理观看端集合，并把同一个不可变 EncodedPacket 扇出给全部观看端。
// 扇出对每个观看端都是非阻塞的（由 Viewer 内部有界队列兜底），
// 因此任何慢观看端都不会拖垮编码器或其他观看端。
type hub struct {
	cfg     Config
	metrics *Metrics

	mu      sync.Mutex
	viewers map[string]*Viewer

	pliMu sync.Mutex
	pli   chan struct{} // size=1，合并多次关键帧请求
}

func newHub(cfg Config, m *Metrics) *hub {
	return &hub{
		cfg:     cfg,
		metrics: m,
		viewers: make(map[string]*Viewer),
		pli:     make(chan struct{}, 1),
	}
}

// Add 注册观看端，并用 store 中最近的关键帧链做初次对齐：
// 新观看端收到的第一帧一定是可独立解码的关键帧，其后顺序补齐增量帧，
// 因此中途接入不会花屏。相同 id 重复接入会先关闭旧观看端（视为重连）。
func (h *hub) Add(ctx context.Context, id string, store *packetStore) *Viewer {
	v := newViewer(id, h.cfg, h.metrics, ctx)
	// 观看端自身 ctx 结束（异常断开）时，从 hub 注销并计入关闭指标。
	v.onClose = func(removedID string) {
		if old, ok := h.viewers[removedID]; ok && old == v {
			h.remove(removedID)
		}
	}

	backfill := store.replaySince(0)
	h.mu.Lock()
	if old := h.viewers[id]; old != nil {
		h.mu.Unlock()
		old.close()
		h.metrics.viewersClosed.Add(1)
		h.mu.Lock()
	}
	h.viewers[id] = v
	h.metrics.viewersTotal.Add(1)
	h.mu.Unlock()

	// 在启动 pusher 前整段写入关键帧链历史，保证第一批包从关键帧开始、
	// 顺序确定，且不受实时队列容量限制。
	v.seedBacklog(backfill)
	v.start()
	return v
}

// remove 主动断开一个观看端并释放其队列。
func (h *hub) remove(id string) {
	h.mu.Lock()
	v := h.viewers[id]
	delete(h.viewers, id)
	h.mu.Unlock()
	if v != nil {
		v.close()
		h.metrics.viewersClosed.Add(1)
	}
}

// get 返回观看端（用于主动重同步）。
func (h *hub) get(id string) (*Viewer, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	v, ok := h.viewers[id]
	return v, ok
}

// broadcast 把一个包非阻塞扇出给所有观看端，并清理已结束的观看端。
func (h *hub) broadcast(p EncodedPacket) {
	h.mu.Lock()
	viewers := make([]*Viewer, 0, len(h.viewers))
	for _, v := range h.viewers {
		viewers = append(viewers, v)
	}
	h.mu.Unlock()
	for _, v := range viewers {
		if !v.deliver(p) {
			h.remove(v.id)
		}
	}
}

// requestPLI 请求编码器尽快产出一个关键帧；并发/重复请求会被合并，
// 且永不阻塞调用方。
func (h *hub) requestPLI() {
	h.metrics.pliRequests.Add(1)
	select {
	case h.pli <- struct{}{}:
	default:
	}
}

func (h *hub) pliRequests() <-chan struct{} { return h.pli }

func (h *hub) viewerCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.viewers)
}

// closeAll 关闭全部观看端。
func (h *hub) closeAll() {
	h.mu.Lock()
	viewers := h.viewers
	h.viewers = make(map[string]*Viewer)
	h.mu.Unlock()
	for _, v := range viewers {
		v.close()
		h.metrics.viewersClosed.Add(1)
	}
}
