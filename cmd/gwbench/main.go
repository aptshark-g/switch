// gwbench — 网关压测客户端（2026-08-13）: 缓存命中路径,
// 测量网关真实上限（Go 连接池 + 并发, 无 Python 客户端瓶颈）。
//
// 用法: go run ./cmd/gwbench -url http://127.0.0.1:8080 -n 20000 -c 64
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var body = []byte(`{"provider":"deepseek","model":"deepseek-v4-flash",
	"thinking":{"type":"disabled"},
	"messages":[{"role":"user","content":"压测 ping"}],
	"max_tokens":16,"temperature":0.0}`)

func main() {
	url := flag.String("url", "http://127.0.0.1:8080/v1/chat/completions",
		"gateway URL")
	n := flag.Int("n", 20000, "total requests")
	c := flag.Int("c", 64, "concurrency")
	flag.Parse()

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: *c,
			MaxConnsPerHost:     *c,
		},
		Timeout: 30 * time.Second,
	}
	var wg sync.WaitGroup
	var ok, fail atomic.Int64
	lats := make([]int64, *n)
	sem := make(chan struct{}, *c)
	t0 := time.Now()
	for i := 0; i < *n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			req, _ := http.NewRequest("POST", *url, bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer dm-client")
			s := time.Now()
			resp, err := client.Do(req)
			lat := time.Since(s).Microseconds()
			lats[i] = lat
			if err != nil {
				fail.Add(1)
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 {
				ok.Add(1)
			} else {
				fail.Add(1)
			}
		}(i)
	}
	wg.Wait()
	dt := time.Since(t0).Seconds()

	sort.Slice(lats, func(a, b int) bool { return lats[a] < lats[b] })
	p50 := lats[len(lats)/2]
	p99 := lats[int(float64(len(lats))*0.99)]
	p999 := lats[int(float64(len(lats))*0.999)]
	fmt.Printf("并发=%d 总量=%d 耗时=%.2fs 吞吐=%.0f req/s\n",
		*c, *n, dt, float64(*n)/dt)
	fmt.Printf("ok=%d fail=%d\n", ok.Load(), fail.Load())
	fmt.Printf("p50=%.3fms p99=%.3fms p99.9=%.3fms\n",
		float64(p50)/1000, float64(p99)/1000, float64(p999)/1000)
}
