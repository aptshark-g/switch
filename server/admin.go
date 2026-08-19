package server

import (
	"net/http"
	"os"
)

// handleErrorCatalog serves the error catalog YAML (查表, 可扩展)。
// 路径: env DM_ERROR_CATALOG 或 cwd/error_catalog.yaml。
func (s *Server) handleErrorCatalog(w http.ResponseWriter, r *http.Request) {
	path := os.Getenv("DM_ERROR_CATALOG")
	if path == "" {
		path = "error_catalog.yaml"
	}
	data, err := os.ReadFile(path)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{
			"error": "error catalog not found", "code": "NOT_FOUND"})
		return
	}
	w.Header().Set("Content-Type", "text/yaml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// handleAdminPage serves a zero-dependency admin dashboard: providers /
// health(cached) / stats(tokens+cache) / usage(cost) / error catalog。
func (s *Server) handleAdminPage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(adminPageHTML))
}

const adminPageHTML = `<!DOCTYPE html>
<html lang="zh"><head><meta charset="utf-8">
<title>DialogMesh Gateway Admin</title>
<style>
body{font-family:system-ui,sans-serif;margin:24px;background:#0f1115;color:#e6e6e6}
h1{font-size:20px} h2{font-size:15px;margin-top:24px;color:#8ab4f8}
table{border-collapse:collapse;width:100%;margin-top:8px;font-size:13px}
th,td{border:1px solid #2a2f3a;padding:5px 8px;text-align:left}
th{background:#1a1e27} td{background:#141821}
.ok{color:#7ee787}.bad{color:#ff7b72}.muted{color:#8b949e}
pre{background:#141821;padding:10px;overflow:auto;font-size:12px}
</style></head><body>
<h1>DialogMesh Gateway Admin</h1>
<div id="err" class="bad"></div>
<h2>Providers</h2><div id="providers"></div>
<h2>Health (cached)</h2><div id="health"></div>
<h2>Stats (tokens / cache)</h2><div id="stats"></div>
<h2>Usage + Cost</h2><div id="usage"></div>
<h2>Error Catalog</h2><div id="catalog"></div>
<script>
async function j(u){const r=await fetch(u);if(!r.ok)throw new Error(u+" "+r.status);return r.json()}
function tbl(headers,rows){return "<table><tr>"+headers.map(h=>"<th>"+h+"</th>").join("")+
  "</tr>"+rows.map(r=>"<tr>"+r.map(c=>"<td>"+c+"</td>").join("")+"</tr>").join("")+"</table>"}
async function main(){
  try{
    const p=await j("/v1/providers");
    document.getElementById("providers").innerHTML=tbl(
      ["name","kind","key","healthy","models","circuit"],
      (p.providers||[]).map(x=>[x.name,x.kind,x.has_key?"✓":"✗",
        x.healthy?"<span class=ok>✓</span>":"<span class=bad>✗</span>",
        (x.models||[]).length, x.circuit||""]));
    const h=await j("/v1/health");
    document.getElementById("health").innerHTML="<span class='"+
      (h.status==="ok"?"ok":"bad")+"'>"+h.status+"</span> "+
      h.providers_healthy+"/"+h.providers_total+" (cached="+h.cached+")";
    const s=await j("/v1/stats");
    document.getElementById("stats").innerHTML=tbl(
      ["tokens_prompt","tokens_completion","cache_hits","cache_misses",
       "cache_hit_rate","prompt_cache_hit_rate",
       "requests","active_connections"],
      [[s.tokens_prompt||0,s.tokens_completion||0,s.cache_hits||0,
        s.cache_misses||0,Math.round((s.cache_hit_rate||0)*100)+"%",
        Math.round((s.prompt_cache_hit_rate||0)*100)+"%",
        s.total_requests||s.requests||0,
        s.active_connections||0]]);
    const u=await j("/v1/usage");
    document.getElementById("usage").innerHTML=tbl(
      ["total_requests","total_tokens","cost_usd"],
      [[u.total_requests,u.total_tokens,
        u.cost&&u.cost.total?u.cost.total.cost_usd.toFixed(6):"-"]]);
  }catch(e){document.getElementById("err").textContent="加载失败: "+e.message}
  try{
    const c=await fetch("/v1/error-catalog");
    document.getElementById("catalog").innerHTML="<pre>"+
      (await c.text()).replace(/&/g,"&amp;").replace(/</g,"&lt;")+"</pre>";
  }catch(e){}
}
main();
setInterval(main,15000);
</script></body></html>`
