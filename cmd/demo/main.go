// demo 用内存通道模拟采集源，演示管线的一快一慢两个观看端。
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/huchangjun118/pixel-stream-pipeline-go/pipeline"
)

func main() {
	cfg := pipeline.DefaultConfig()
	cfg.KeyframeInterval = 10
	cfg.SubscriberQueueSize = 4

	p, err := pipeline.New(cfg)
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fast := p.Subscribe()
	slow := p.Subscribe()

	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	// 采集源：以不稳定节奏产出帧。
	go func() {
		for i := uint64(1); i <= 200; i++ {
			f := pipeline.Frame{
				Seq:        i,
				CapturedAt: time.Now(),
				Width:      8, Height: 8,
				Pixels: make([]byte, 8*8*4),
			}
			for j := range f.Pixels {
				f.Pixels[j] = byte(i) + byte(j%7)
			}
			p.Input() <- f
			time.Sleep(time.Duration(i%5) * 5 * time.Millisecond) // 不稳定节奏
		}
		close(p.Input())
	}()

	// 快端：立即消费。
	go func() {
		n := 0
		for range fast.C {
			n++
		}
		fmt.Println("fast subscriber done, received:", n)
	}()
	// 慢端：每帧睡一会，触发回压与关键帧重同步。
	go func() {
		n := 0
		for f := range slow.C {
			_ = f
			n++
			time.Sleep(30 * time.Millisecond)
		}
		fmt.Println("slow subscriber done, received:", n)
	}()

	if err := <-done; err != nil {
		fmt.Println("pipeline exit:", err)
	}
	time.Sleep(100 * time.Millisecond) // 等两个消费 goroutine 打印
	m := p.Metrics()
	fmt.Printf("metrics: in=%d sequenced=%d encoded=%d broadcast=%d encodeErr=%d drops=%v\n",
		m.FramesIn, m.FramesSequenced, m.FramesEncoded, m.FramesBroadcast, m.EncodeErrors, m.Dropped)
}
