
package provider

import "testing"

func TestMergeToolCallsByIndex(t *testing.T) {
	// 模拟 deepseek 流式: 首片 id+name, 后续 3 片 arguments 增量
	acc := []ToolCall{}
	acc = mergeToolCalls(acc, []ToolCall{{
		Index: 0, ID: "call_1", Type: "function",
		Function: FunctionCall{Name: "file_write"},
	}})
	acc = mergeToolCalls(acc, []ToolCall{{
		Index: 0, Function: FunctionCall{Arguments: `{"path":`},
	}})
	acc = mergeToolCalls(acc, []ToolCall{{
		Index: 0, Function: FunctionCall{Arguments: `"a.py",`},
	}})
	acc = mergeToolCalls(acc, []ToolCall{{
		Index: 0, Function: FunctionCall{Arguments: `"content":"hi"}`},
	}})
	if len(acc) != 1 {
		t.Fatalf("期望 1 条合并结果, 得到 %d 条（碎片未合并）", len(acc))
	}
	c := acc[0]
	if c.ID != "call_1" || c.Function.Name != "file_write" {
		t.Fatalf("id/name 未保留: %+v", c)
	}
	want := `{"path":"a.py","content":"hi"}`
	if c.Function.Arguments != want {
		t.Fatalf("arguments 合并错误: got=%q want=%q", c.Function.Arguments, want)
	}
}

func TestMergeToolCallsMultiIndex(t *testing.T) {
	acc := mergeToolCalls(nil, []ToolCall{
		{Index: 0, ID: "a", Function: FunctionCall{Name: "f1"}},
		{Index: 1, ID: "b", Function: FunctionCall{Name: "f2"}},
	})
	acc = mergeToolCalls(acc, []ToolCall{
		{Index: 0, Function: FunctionCall{Arguments: "x"}},
		{Index: 1, Function: FunctionCall{Arguments: "y"}},
	})
	if len(acc) != 2 || acc[0].Function.Arguments != "x" ||
		acc[1].Function.Arguments != "y" {
		t.Fatalf("多 index 合并错误: %+v", acc)
	}
}

func TestMergeToolCallsAppendWhenNoIndex(t *testing.T) {
	// 无 index 分片（非标准流）→ 直接追加, 不合并（防误并）
	acc := mergeToolCalls(nil, []ToolCall{{
		ID: "z", Function: FunctionCall{Name: "f3", Arguments: "{}"},
	}})
	if len(acc) != 1 || acc[0].ID != "z" {
		t.Fatalf("无 index 追加错误: %+v", acc)
	}
}
