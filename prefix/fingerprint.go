// Package prefix 实现前缀指纹与命中观测（基线 B-1）。
//
// 分层口径（2026-08-21）: P0..P4 精确分层是 DialogMesh 编译器的契约
// （B-3 固化前缀）; 网关 B-1 先按角色近似三层, 用于纯观测:
//
//	Seg0 = system 消息 + 工具定义（稳定头, 对应 P0）
//	Seg1 = 历史消息（除最后一条, 对应 P1+P2+P3 近似）
//	Seg2 = 最后一条消息（本轮输入, 对应 P4）
//
// 编译器契约落地后, 段边界可替换为真实 P0..P3 映射。
package prefix

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/aptshark/gateway/provider"
)

const (
	SegSystem  = 0
	SegHistory = 1
	SegLast    = 2
	SegCount   = 3
)

// FingerprintTree 一个请求的分层指纹树。
type FingerprintTree struct {
	Segments [SegCount]string // 每层 sha256 hex
	TokenLen [SegCount]int    // 每层估算 token 长度（len/4, 命中归属推断用）
	FP       string           // sha256(seg0||seg1||seg2), 主亲和键
}

// FingerprintRequest 计算请求的分层指纹（消息 + 工具定义）。
func FingerprintRequest(req *provider.GenerateRequest) *FingerprintTree {
	t := &FingerprintTree{}
	msgs := req.Messages
	if len(msgs) == 0 {
		t.FP = hashOf("")
		return t
	}

	// Seg0: system 消息 + 工具定义（canonical: 工具按 name 排序）。
	var sb strings.Builder
	for _, m := range msgs {
		if m.Role == "system" {
			sb.WriteString(m.Content)
			sb.WriteString("\x00")
		}
	}
	if len(req.Tools) > 0 {
		tools := make([]string, 0, len(req.Tools))
		for _, td := range req.Tools {
			b, _ := json.Marshal(td)
			tools = append(tools, string(b))
		}
		sortStrings(tools)
		for _, s := range tools {
			sb.WriteString(s)
			sb.WriteString("\x00")
		}
	}
	seg0 := sb.String()
	t.Segments[SegSystem] = hashOf(seg0)
	t.TokenLen[SegSystem] = len(seg0) / 4

	// Seg1: 历史（除最后一条）; Seg2: 最后一条。
	var hist strings.Builder
	for _, m := range msgs[:len(msgs)-1] {
		hist.WriteString(m.Role)
		hist.WriteString(":")
		hist.WriteString(m.Content)
		hist.WriteString("\x00")
	}
	seg1 := hist.String()
	t.Segments[SegHistory] = hashOf(seg1)
	t.TokenLen[SegHistory] = len(seg1) / 4

	last := msgs[len(msgs)-1]
	t.Segments[SegLast] = hashOf(last.Content)
	t.TokenLen[SegLast] = len(last.Content) / 4

	t.FP = hashOf(t.Segments[0] + t.Segments[1] + t.Segments[2])
	return t
}

func hashOf(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
