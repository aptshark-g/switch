package cache

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCoalescerSingleExecution(t *testing.T) {
	c := NewCoalescer()
	var execs atomic.Int64
	fn := func() (any, error) {
		execs.Add(1)
		time.Sleep(20 * time.Millisecond)
		return "ok", nil
	}
	var wg sync.WaitGroup
	results := make([]any, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx], _ = c.Do("same-key", fn, time.Second)
		}(i)
	}
	wg.Wait()
	if execs.Load() != 1 {
		t.Fatalf("fn executed %d times, want 1 (coalesced)", execs.Load())
	}
	for i, r := range results {
		if r != "ok" {
			t.Fatalf("result[%d] = %v, want ok", i, r)
		}
	}
	if c.Pending() != 0 {
		t.Fatalf("pending = %d, want 0 after done", c.Pending())
	}
}

func TestCoalescerDistinctKeys(t *testing.T) {
	c := NewCoalescer()
	var execs atomic.Int64
	fn := func() (any, error) {
		execs.Add(1)
		time.Sleep(20 * time.Millisecond) // 保证同 key 并发调用重叠
		return "ok", nil
	}
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = c.Do("key", fn, time.Second)
			_, _ = c.Do("other", fn, time.Second)
		}()
	}
	wg.Wait()
	// 两个不同 key → 各执行一次; 同 key 合并
	if execs.Load() != 2 {
		t.Fatalf("fn executed %d times, want 2 (two distinct keys)", execs.Load())
	}
}

func TestCoalescerErrorPropagates(t *testing.T) {
	c := NewCoalescer()
	boom := errors.New("boom")
	_, err := c.Do("k", func() (any, error) { return nil, boom }, time.Second)
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
}

func TestCoalescerWaiterTimeout(t *testing.T) {
	c := NewCoalescer()
	var release = make(chan struct{})
	go func() {
		_, _ = c.Do("k", func() (any, error) {
			<-release // 模拟慢上游
			return "late", nil
		}, 5*time.Second)
	}()
	time.Sleep(50 * time.Millisecond) // 确保第一个调用已在执行
	_, err := c.Do("k", func() (any, error) { return nil, nil }, 10*time.Millisecond)
	if err == nil {
		t.Fatal("waiter should time out")
	}
	close(release)
}
