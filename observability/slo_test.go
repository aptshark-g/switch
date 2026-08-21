package observability

import "testing"

func TestSLOMonitorNoAlertOnSuccess(t *testing.T) {
	sm := NewSLOMonitor(DefaultSLOConfig())
	alerts := 0
	sm.OnAlert(func(SLOAlert) { alerts++ })
	for i := 0; i < 200; i++ {
		sm.RecordSuccess()
	}
	if alerts != 0 {
		t.Fatalf("alerts = %d, want 0 on all-success", alerts)
	}
	snap := sm.Snapshot()
	if snap.BurnRate != 0 {
		t.Fatalf("burn_rate = %.2f, want 0", snap.BurnRate)
	}
}

func TestSLOMonitorAlertOnFailures(t *testing.T) {
	sm := NewSLOMonitor(DefaultSLOConfig())
	var fired SLOAlert
	sm.OnAlert(func(a SLOAlert) { fired = a })
	// SLO 99.5%: budgeted error rate = 0.5%。全失败 → burn rate ≈ 200 > 14.4 → page。
	for i := 0; i < 100; i++ {
		sm.RecordFailure()
	}
	if fired.Level != SLOAlertPage {
		t.Fatalf("alert level = %v, want page (all failures)", fired.Level)
	}
	if fired.BurnRate < 1 {
		t.Fatalf("burn_rate = %.2f, want > 1", fired.BurnRate)
	}
}

func TestSLOMonitorThresholdSensitivity(t *testing.T) {
	sm := NewSLOMonitor(DefaultSLOConfig())
	paged := false
	sm.OnAlert(func(a SLOAlert) {
		if a.Level == SLOAlertPage {
			paged = true
		}
	})
	// 1% 失败率 → 稳态 burn ≈ 2 < ticket(6) → 绝不 page。
	// 注: 前 30 条里 1 次失败会瞬时算 3.3% → 可能触发一次 ticket
	// （小样本噪声, 真实 1h 窗口会平滑掉）; 断言只要求不 page。
	for i := 0; i < 3000; i++ {
		if i%100 == 0 {
			sm.RecordFailure()
		} else {
			sm.RecordSuccess()
		}
	}
	if paged {
		t.Fatal("1% failures should never page")
	}
	if snap := sm.Snapshot(); snap.BurnRate >= 6 {
		t.Fatalf("steady burn_rate = %.2f, want < 6 (ticket threshold)", snap.BurnRate)
	}
}
