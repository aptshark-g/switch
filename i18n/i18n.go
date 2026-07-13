package i18n

import (
	"os"
	"strings"
	"sync"
)

var (
	mu      sync.RWMutex
	active  = "en"
	locales = map[string]map[string]string{}
)

func Register(lang string, m map[string]string) {
	mu.Lock(); defer mu.Unlock()
	locales[lang] = m
}

func SetLang(lang string) {
	mu.Lock(); defer mu.Unlock()
	if _, ok := locales[lang]; ok { active = lang }
}

func Lang() string { mu.RLock(); defer mu.RUnlock(); return active }
func List() []string { mu.RLock(); defer mu.RUnlock(); out := make([]string, 0, len(locales)); for k := range locales { out = append(out, k) }; return out }

func T(key string) string {
	mu.RLock(); defer mu.RUnlock()
	if m, ok := locales[active]; ok { if v, ok := m[key]; ok { return v } }
	if m, ok := locales["en"]; ok { if v, ok := m[key]; ok { return v } }
	return key
}

func DetectLang() string {
	lang := os.Getenv("LANG")
	if lang == "" { lang = os.Getenv("LC_ALL") }
	if strings.HasPrefix(lang, "zh") { return "zh" }
	return "en"
}

func init() {
	Register("en", map[string]string{
		"title":           "Gateway Runtime Console",
		"chat":            "Chat",
		"switch_provider": "Switch Provider",
		"switch_model":    "Switch Model",
		"view_stats":      "View Live Stats",
		"list_providers": "List Providers",
		"diagnostics": "Diagnostics",
		"provider_manager": "Provider Manager",
		"activate": "Activate",
		"add_key": "Add Key",
		"edit": "Edit",
		"delete": "Delete",
		"add_new": "Add New",
		"select": "Select",
		"test": "Test Connection",
		"reload_config":   "Reload Config",
		"switch_lang":     "Switch Language",
		"exit":            "Exit",
		"provider":        "Provider",
		"model":           "Model",
		"uptime":          "Uptime",
		"reqs":            "Reqs",
		"tokens":          "Tokens",
		"bye":             "bye",
		"invalid":         "invalid choice",
		"no_providers":    "No providers. Check gateway.",
		"no_models":       "No models.",
		"press_enter":     "Press enter...",
		"reloaded":        "Reloaded",
		"select_lang":     "Select language",
		"lang_switched": "Language switched",
		"back_to_menu": "Back to Menu",
	})

	Register("zh", map[string]string{
		"title":           "Gateway 运行时控制台",
		"chat":            "对话",
		"switch_provider": "切换供应商",
		"switch_model":    "切换模型",
		"view_stats":      "查看实时统计",
		"list_providers": "供应商列表",
		"diagnostics": "诊断信息",
		"provider_manager": "供应商管理",
		"activate": "激活",
		"add_key": "填入Key",
		"edit": "编辑",
		"delete": "删除",
		"add_new": "新增供应商",
		"select": "选择使用",
		"test": "测试连接",
		"reload_config":   "重载配置",
		"switch_lang":     "切换语言",
		"exit":            "退出",
		"provider":        "供应商",
		"model":           "模型",
		"uptime":          "运行",
		"reqs":            "请求",
		"tokens":          "令牌",
		"bye":             "再见",
		"invalid":         "无效选项",
		"no_providers":    "没有供应商。检查网关状态。",
		"no_models":       "没有模型。",
		"press_enter":     "按回车继续...",
		"reloaded":        "已重载",
		"select_lang":     "选择语言",
		"lang_switched": "语言已切换",
		"back_to_menu": "返回主菜单",
	})
}



"back_to_menu": "Back to Menu",
"provider_manager": "Provider Manager",
"activate": "Activate",
"add_key": "Add Key",
"edit": "Edit",
"delete": "Delete",
"add_new": "Add New",
"select": "Select",
"test": "Test Connection",
"need_key_first": "API key required - enter key below",
"no": "no",
"active_yes": "ACTIVE",
"key_yes": "set",
"name_col": "Name",
"active_col": "Active",
"key_col": "Key",
"models_col": "Models",
"back_to_menu": "返回主菜单",
"provider_manager": "供应商管理",
"activate": "激活",
"add_key": "填入Key",
"edit": "编辑",
"delete": "删除",
"add_new": "新增供应商",
"select": "选择使用",
"test": "测试连接",
"need_key_first": "需要API密钥，请在下方输入",
"no": "否",
"active_yes": "已激活",
"key_yes": "已配置",
"name_col": "名称",
"active_col": "激活",
"key_col": "密钥",
"models_col": "模型",
"test_failed": "Test Failed - provider unreachable",
"testing": "Testing connectivity...",
"test_failed": "测试失败 - 供应商不可达",
"testing": "正在测试连通性...",
