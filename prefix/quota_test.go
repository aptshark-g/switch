package prefix

import "testing"

func TestQuotaRealPriority(t *testing.T) {
	q := NewQuotaCoordinator(10, 0.30) // 桶 10, 预热只用剩余 >30%
	// 消耗 7 个真实 token → 剩余 3
	for i := 0; i < 7; i++ {
		q.RealRequest("p", "fp")
	}
	// 剩余 3 = 30% of 10 → 预热不允许（>30% 才允许）
	if q.WarmAllowed("p", "fp") {
		t.Fatal("warm should be blocked when only 30% remains (not > 30%)")
	}
	// 再消耗 1 → 剩余 2 < 3, 更不允许
	q.RealRequest("p", "fp")
	if q.WarmAllowed("p", "fp") {
		t.Fatal("warm should be blocked below reserve")
	}
	// 新桶: 消耗 5 → 剩余 5 > 3 → 预热允许
	q2 := NewQuotaCoordinator(10, 0.30)
	for i := 0; i < 5; i++ {
		q2.RealRequest("p", "fp")
	}
	if !q2.WarmAllowed("p", "fp") {
		t.Fatal("warm should be allowed with 50% remaining")
	}
	// 预热消耗后剩余 4, 仍 > 3 → 可再预热
	if !q2.WarmAllowed("p", "fp") {
		t.Fatal("second warm should be allowed")
	}
	// 预热把桶打到 reserve 以下 → 阻塞
	for i := 0; i < 5; i++ {
		q2.WarmAllowed("p", "fp")
	}
	if q2.WarmAllowed("p", "fp") {
		t.Fatal("warm should be blocked below reserve")
	}
}

func TestQuotaNilSafe(t *testing.T) {
	var q *QuotaCoordinator
	q.RealRequest("p", "fp") // 不 panic
	if !q.WarmAllowed("p", "fp") {
		t.Fatal("nil quota should allow warm")
	}
}
