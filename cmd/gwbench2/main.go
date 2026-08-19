// gwbench2 — 网关扩展压测（2026-08-20）:
//   -mode miss    未命中路径（每请求唯一 body, 全部走真实上游, mock 上游测网关开销）
//   -mode stream  长上下文 SSE 流式并发
//   -mode stab    高并发稳定性（固定时长, 采样延迟分布）
//   -mode err     上游错误注入（429）, 观察错误分类/熔断
// 用法: go run ./cmd/gwbench2 -url http://127.0.0.1:8080/v1/chat/completions -mode miss -n 2000 -c 32
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var usage = map[string]any{
	"prompt_tokens": 8000, "completion_tokens": 512, "total_tokens": 8512,
}

func makeBody(mode string, i int) []byte {
	var content string
	if mode == "miss" {
		content = fmt.Sprintf("bench request %d (unique)", i)
	} else {
		content = "bench request"
	}
	b, _ := json.Marshal(map[string]any{
		"provider": "mock", "model": "mock-model",
		"messages":   []map[string]any{{"role": "user", "content": content}},
		"max_tokens": 512, "temperature": 0.0,
		"stream": mode == "stream",
	})
	return b
}

func main() {
	url := flag.String("url", "http://127.0.0.1:8080/v1/chat/completions", "gateway URL")
	mode := flag.String("mode", "miss", "miss | stream | stab | err")
	n := flag.Int("n", 2000, "total requests (miss/stream/err)")
	c := flag.Int("c", 32, "concurrency")
	dur := flag.Int("seconds", 10, "duration seconds (stab mode)")
	flag.Parse()

	client := &http.Client{
		Transport: &http.Transport{MaxIdleConnsPerHost: *c, MaxConnsPerHost: *c},
		Timeout:   60 * time.Second,
	}
	var wg sync.WaitGroup
	var ok, fail, streamDone atomic.Int64
	sem := make(chan struct{}, *c)
	t0 := time.Now()

	run := func(total int) {
		lats := make([]int64, total)
		for i := 0; i < total; i++ {
			wg.Add(1)
			sem <- struct{}{}
			go func(i int) {
				defer wg.Done()
				defer func() { <-sem }()
				body := makeBody(*mode, i)
				req, _ := http.NewRequest("POST", *url, bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", "Bearer dm-client")
				s := time.Now()
				resp, err := client.Do(req)
				lat := time.Since(s).Microseconds()
				if err != nil {
					fail.Add(1)
					lats[i] = lat
					return
				}
				defer resp.Body.Close()
				if *mode == "stream" {
					sc := bufio.NewScanner(resp.Body)
					for sc.Scan() {
						line := sc.Text()
						if strings.HasPrefix(line, "data: ") && strings.Contains(line, "finish_reason") {
							streamDone.Add(1)
						}
					}
					if resp.StatusCode != 200 {
						fail.Add(1)
					} else {
						ok.Add(1)
					}
				} else {
					_, _ = io.Copy(io.Discard, resp.Body)
					if resp.StatusCode == 200 {
						ok.Add(1)
					} else {
						fail.Add(1)
					}
				}
				lats[i] = lat
			}(i)
		}
		wg.Wait()
		el := time.Since(t0).Seconds()
		all := ok.Load() + fail.Load()
		fmt.Printf("mode=%s 并发=%d 总量=%d 耗时=%.2fs 吞吐=%d req/s\n",
			*mode, *c, all, el, int(float64(all)/el))
		fmt.Printf("ok=%d fail=%d", ok.Load(), fail.Load())
		if *mode == "stream" {
			fmt.Printf(" stream_done=%d", streamDone.Load())
		}
		fmt.Println()
		// 延迟分布
		sorted := make([]int64, 0, len(lats))
		for _, v := range lats {
			if v > 0 {
				sorted = append(sorted, v)
			}
		}
		if len(sorted) > 0 {
			sortInt64(sorted)
			p := func(q float64) int64 { return sorted[int(float64(len(sorted)-1)*q)] }
			fmt.Printf("p50=%dms p90=%dms p99=%dms p99.9=%dms\n",
				p(0.5)/1000, p(0.9)/1000, p(0.99)/1000, p(0.999)/1000)
		}
	}

	if *mode == "stab" {
		// 常驻 worker 池持续打流（修正 v1 轮次屏障伪影）
		deadline := time.Now().Add(time.Duration(*dur) * time.Second)
		var total atomic.Int64
		var totOK, totFail atomic.Int64
		var sumLat atomic.Int64
		for w := 0; w < *c; w++ {
			wg.Add(1)
			go func(wid int) {
				defer wg.Done()
				i := wid
				for time.Now().Before(deadline) {
					body := makeBody("miss", i)
					i += *c
					req, _ := http.NewRequest("POST", *url, bytes.NewReader(body))
					req.Header.Set("Content-Type", "application/json")
					req.Header.Set("Authorization", "Bearer dm-client")
					s := time.Now()
					resp, err := client.Do(req)
					lat := time.Since(s).Microseconds()
					total.Add(1)
					sumLat.Add(lat)
					if err != nil {
						totFail.Add(1)
						continue
					}
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
					if resp.StatusCode == 200 {
						totOK.Add(1)
					} else {
						totFail.Add(1)
					}
				}
			}(w)
		}
		wg.Wait()
		el := time.Since(t0).Seconds()
		all := total.Load()
		avg := 0.0
		if all > 0 {
			avg = float64(sumLat.Load()) / float64(all) / 1000.0
		}
		fmt.Printf("stab(常驻): 并发=%d 时长=%.1fs 总量=%d 吞吐=%d req/s\n",
			*c, el, all, int(float64(all)/el))
		fmt.Printf("ok=%d fail=%d 平均延迟=%.1fms\n",
			totOK.Load(), totFail.Load(), avg)
		return
	}
	run(*n)
}

func sortInt64(a []int64) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j] < a[j-1]; j-- {
			a[j], a[j-1] = a[j-1], a[j]
		}
	}
}
