# !/bin/python
from __future__ import annotations

import json
from html import escape

from shared.lineage.schedule_table_lineage import (
    DOWNSTREAM,
    UPSTREAM,
    build_table_job_map,
    find_table_candidates,
    load_job_index,
    normalize_table_name,
    trace_table_lineage,
)


def put_html(*args, **kwargs):
    from pywebio.output import put_html as render_html

    return render_html(*args, **kwargs)


def put_table(*args, **kwargs):
    from pywebio.output import put_table as render_table

    return render_table(*args, **kwargs)


def put_black_text(*args, **kwargs):
    from shared.ui.pywebio_helper import put_black_text as render_text

    return render_text(*args, **kwargs)


def put_red_text(*args, **kwargs):
    from shared.ui.pywebio_helper import put_red_text as render_text

    return render_text(*args, **kwargs)


def safe_put_error(*args, **kwargs):
    from shared.ui.pywebio_helper import safe_put_error as render_error

    return render_error(*args, **kwargs)


def start_pywebio_app(*args, **kwargs):
    from shared.ui.pywebio_helper import start_pywebio_app as start_app

    return start_app(*args, **kwargs)


DEFAULT_MAX_DEPTH = 8
DIRECTION_LABELS = {UPSTREAM: "上游检查", DOWNSTREAM: "下游检查"}
TYPE_LABELS = {
    UPSTREAM: {"root_table": "根表", "table": "中间结果表", "dwf_table": "DWF 截止表"},
    DOWNSTREAM: {
        "root_table": "根表",
        "table": "中间结果表",
        "terminal_table": "末端表",
    },
}

ROAMER_STYLE = r"""
<style>
.table-roamer{--line:#d8dee9;--text:#172033;--muted:#64748b;--accent:#2563eb;width:min(96vw,1760px);
margin:16px calc(50% - min(48vw,880px)) 32px;border:1px solid var(--line);border-radius:10px;
background:#f6f8fb;color:var(--text);overflow:hidden;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Microsoft YaHei",sans-serif}
.table-roamer *{box-sizing:border-box}.table-roamer .bar{display:flex;justify-content:space-between;gap:14px;flex-wrap:wrap;
padding:14px 18px;border-bottom:1px solid var(--line);background:#fff}.table-roamer .cap{font-weight:700}.table-roamer .sub,.table-roamer .meta{color:var(--muted);font-size:12px}
.table-roamer .warn{width:100%;padding:8px 11px;border:1px solid #fed7aa;border-radius:7px;background:#fff7ed;color:#9a3412;font-size:13px}
.table-roamer .workspace{display:grid;grid-template-columns:minmax(0,1fr) 340px;gap:14px;padding:14px}
.table-roamer .graph,.table-roamer .detail{border:1px solid var(--line);border-radius:8px;background:#fff}.table-roamer .graph{overflow:auto;max-height:72vh;padding:22px}
.table-roamer svg{display:block;max-width:none}.table-roamer .edge{stroke:#94a3b8;stroke-width:1.3;fill:none}.table-roamer .node{outline:none;cursor:pointer}
.table-roamer .node rect{fill:#fff;stroke:#94a3b8;stroke-width:1.2}.table-roamer .node text{fill:#334155;font:12px Consolas,monospace;pointer-events:none}
.table-roamer .node.root_table rect{fill:#dcfce7;stroke:#16a34a}.table-roamer .node.dwf_table rect{fill:#fef3c7;stroke:#d97706}
.table-roamer .node.terminal_table rect{fill:#fce7f3;stroke:#db2777}
.table-roamer svg.dim .node,.table-roamer svg.dim .edge{opacity:.22}.table-roamer svg.dim .node.hot,.table-roamer svg.dim .node.lit,.table-roamer svg.dim .edge.lit{opacity:1}
.table-roamer .node.hot rect{fill:#dbeafe;stroke:var(--accent);stroke-width:2}.table-roamer .edge.lit{stroke:var(--accent);stroke-width:2}
.table-roamer .detail{padding:16px;align-self:start;max-height:72vh;overflow:auto}.table-roamer .empty{color:var(--muted);font-size:13px;line-height:1.7}
.table-roamer .detail h3{margin:0 0 12px;font:700 14px Consolas,monospace;word-break:break-all}.table-roamer dl{display:grid;grid-template-columns:95px 1fr;gap:9px;margin:0;font-size:13px}
.table-roamer dt{color:var(--muted)}.table-roamer dd{margin:0;word-break:break-all}.table-roamer .legend{display:flex;gap:18px;padding:0 18px 16px;color:#475569;font-size:12px}
.table-roamer .dot{width:9px;height:9px;border-radius:50%;display:inline-block;margin-right:5px}.table-roamer .dot.root_table{background:#16a34a}.table-roamer .dot.table{background:#2563eb}.table-roamer .dot.dwf_table{background:#d97706}.table-roamer .dot.terminal_table{background:#db2777}
@media(max-width:900px){.table-roamer{width:100%;margin:16px 0 32px}.table-roamer .workspace{grid-template-columns:1fr}}
</style>
"""

ROAMER_SCRIPT = r"""
<script>
(function(){
const graph=__GRAPH__,root=document.getElementById("__ROOT_ID__"),svg=root.querySelector("svg"),detail=root.querySelector(".detail");
const typeLabel=__TYPE_LABEL__,nodeW=260,nodeH=36,colGap=330,rowGap=58,pad=28,byId=new Map(graph.nodes.map(n=>[n.id,n]));
const cols={};for(const n of graph.nodes)(cols[n.col]||(cols[n.col]=[])).push(n);
const keys=Object.keys(cols).map(Number).sort((a,b)=>a-b),maxRows=Math.max(1,...keys.map(k=>cols[k].length));
const width=pad*2+(keys.length-1)*colGap+nodeW,height=pad*2+(maxRows-1)*rowGap+nodeH;
for(const k of keys){cols[k].sort((a,b)=>a.label.localeCompare(b.label));const offset=(maxRows-cols[k].length)*rowGap/2;
cols[k].forEach((n,i)=>{n.x=pad+keys.indexOf(k)*colGap;n.y=pad+offset+i*rowGap})}
svg.setAttribute("width",width);svg.setAttribute("height",height);svg.setAttribute("viewBox",`0 0 ${width} ${height}`);
const defs=document.createElementNS("http://www.w3.org/2000/svg","defs"),marker=document.createElementNS("http://www.w3.org/2000/svg","marker");
marker.setAttribute("id","__MARKER_ID__");marker.setAttribute("markerWidth","8");marker.setAttribute("markerHeight","8");marker.setAttribute("refX","7");marker.setAttribute("refY","4");marker.setAttribute("orient","auto");
const arrow=document.createElementNS("http://www.w3.org/2000/svg","path");arrow.setAttribute("d","M0,0 L8,4 L0,8 Z");arrow.setAttribute("fill","#94a3b8");marker.appendChild(arrow);defs.appendChild(marker);svg.appendChild(defs);
const adj={},radj={};for(const [a,b] of graph.edges){(adj[a]||(adj[a]=[])).push(b);(radj[b]||(radj[b]=[])).push(a)}
function reach(id,map){const seen=new Set(),stack=[...(map[id]||[])];while(stack.length){const x=stack.pop();if(seen.has(x))continue;seen.add(x);stack.push(...(map[x]||[]))}return seen}
const edgeEls=[];for(const [a,b] of graph.edges){const x=byId.get(a),y=byId.get(b);if(!x||!y)continue;const p=document.createElementNS("http://www.w3.org/2000/svg","path");
const mid=(x.x+nodeW+y.x)/2;p.setAttribute("d",`M${x.x+nodeW} ${x.y+nodeH/2} C${mid} ${x.y+nodeH/2} ${mid} ${y.y+nodeH/2} ${y.x} ${y.y+nodeH/2}`);
p.setAttribute("class","edge");p.setAttribute("marker-end","url(#__MARKER_ID__)");p.dataset.a=a;p.dataset.b=b;svg.appendChild(p);edgeEls.push(p)}
const nodeEls=new Map();for(const n of graph.nodes){const g=document.createElementNS("http://www.w3.org/2000/svg","g");g.setAttribute("class",`node ${n.type}`);g.setAttribute("tabindex","0");
const r=document.createElementNS("http://www.w3.org/2000/svg","rect");r.setAttribute("x",n.x);r.setAttribute("y",n.y);r.setAttribute("width",nodeW);r.setAttribute("height",nodeH);r.setAttribute("rx","6");
const t=document.createElementNS("http://www.w3.org/2000/svg","text");t.setAttribute("x",n.x+12);t.setAttribute("y",n.y+23);t.textContent=n.label.length>34?n.label.slice(0,31)+"...":n.label;
g.append(r,t);svg.appendChild(g);nodeEls.set(n.id,g);g.onclick=()=>select(n.id);g.onkeydown=e=>{if(e.key==="Enter"||e.key===" "){e.preventDefault();select(n.id)}}}
function esc(s){return String(s==null?"":s).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]))}
function select(id){const up=reach(id,radj),down=reach(id,adj);svg.classList.add("dim");for(const [nid,el] of nodeEls){el.classList.toggle("hot",nid===id);el.classList.toggle("lit",up.has(nid)||down.has(nid))}
for(const e of edgeEls)e.classList.toggle("lit",(up.has(e.dataset.a)&&(up.has(e.dataset.b)||e.dataset.b===id))||(down.has(e.dataset.b)&&(down.has(e.dataset.a)||e.dataset.a===id)));
const n=byId.get(id),rows=Object.entries(n.detail||{}).map(([k,v])=>`<dt>${esc(k)}</dt><dd>${esc(v)}</dd>`).join("");
detail.innerHTML=`<h3>${esc(n.label)}</h3><dl><dt>节点类型</dt><dd>${esc(typeLabel[n.type]||n.type)}</dd><dt>全部上游</dt><dd>${up.size}</dd><dt>全部下游</dt><dd>${down.size}</dd>${rows}</dl>`}
svg.onclick=e=>{if(e.target===svg){svg.classList.remove("dim");for(const el of nodeEls.values())el.classList.remove("hot","lit");for(const el of edgeEls)el.classList.remove("lit");detail.innerHTML='<div class="empty">点击表节点查看完整表名，并高亮它的全部上下游。</div>'}}
})();
</script>
"""


def build_graph_html(graph: dict, index: int) -> str:
    direction = graph.get("direction", UPSTREAM)
    direction_label = DIRECTION_LABELS.get(direction, DIRECTION_LABELS[UPSTREAM])
    type_labels = TYPE_LABELS.get(direction, TYPE_LABELS[UPSTREAM])
    root_id = f"table-roamer-{index}"
    marker_id = f"table-arrow-{index}"
    warnings = "".join(
        f"<div>{escape(item)}</div>" for item in graph.get("warnings", [])
    )
    warning_html = f'<div class="warn">{warnings}</div>' if warnings else ""
    legend = "".join(
        f'<span><i class="dot {escape(node_type)}"></i>{escape(label)}</span>'
        for node_type, label in type_labels.items()
    )
    direction_note = (
        "根表在右侧，上游表向左展开，DWF 表停止继续追踪"
        if direction == UPSTREAM
        else "根表在左侧，下游表向右展开，无下游依赖的表标记为末端表"
    )
    html = f"""
{ROAMER_STYLE}
<div class="table-roamer" id="{root_id}">
  <div class="bar">
    <div><div class="cap">表血缘双向漫游 · {direction_label}</div>
    <div class="sub">{direction_note}；箭头始终表示来源表 → 目标表</div></div>
    <div class="meta">nodes={len(graph.get("nodes", []))} · edges={len(graph.get("edges", []))} · max_depth={graph.get("max_depth")}</div>
    {warning_html}
  </div>
  <div class="workspace"><div class="graph"><svg role="img" aria-label="表血缘图 - {direction_label}"></svg></div>
  <aside class="detail"><div class="empty">点击表节点查看完整表名，并高亮它的全部上下游。</div></aside></div>
  <div class="legend">{legend}</div>
</div>
"""
    script = ROAMER_SCRIPT.replace("__GRAPH__", json.dumps(graph, ensure_ascii=False))
    script = script.replace("__ROOT_ID__", root_id)
    script = script.replace("__MARKER_ID__", marker_id)
    script = script.replace(
        "__TYPE_LABEL__", json.dumps(type_labels, ensure_ascii=False)
    )
    return html + script


def put_graph(graph: dict, index: int):
    put_html(build_graph_html(graph, index))


def analyze_one(
    input_name: str,
    job_index: dict,
    table_job_map: dict,
    max_depth: int,
    index: int,
    direction: str = UPSTREAM,
):
    candidates = find_table_candidates(input_name, table_job_map)
    if not candidates:
        put_red_text(f"{normalize_table_name(input_name)} 未找到对应结果表")
        return
    if len(candidates) > 1:
        put_red_text(
            f"{normalize_table_name(input_name)} 匹配到多个表，请输入包含 schema 的完整表名"
        )
        put_table([["候选表名"]] + [[item] for item in candidates[:100]])
        return
    trace = trace_table_lineage(
        candidates[0],
        job_index,
        table_job_map,
        max_depth=max_depth,
        direction=direction,
    )
    put_black_text(f"检查方向: {DIRECTION_LABELS[direction]}；根表: {trace.root_table}")
    put_graph(trace.to_graph_dict(), index)


def main():
    from pywebio.input import NUMBER, TEXT, input, input_group, radio, textarea

    info = input_group(
        "表血缘双向漫游",
        [
            radio(
                "检查方向",
                name="direction",
                options=[("上游检查", UPSTREAM), ("下游检查", DOWNSTREAM)],
                value=UPSTREAM,
                required=True,
            ),
            textarea(
                "请输入表名，每行一个，例如 DWM.TABLE_NAME", name="targets", type=TEXT
            ),
            input(
                "最大作业递归深度，默认 8",
                name="max_depth",
                type=NUMBER,
                value=DEFAULT_MAX_DEPTH,
            ),
        ],
    )
    direction = (
        info.get("direction") if info.get("direction") in DIRECTION_LABELS else UPSTREAM
    )
    try:
        max_depth = max(1, min(int(info.get("max_depth") or DEFAULT_MAX_DEPTH), 30))
    except (TypeError, ValueError):
        max_depth = DEFAULT_MAX_DEPTH
    targets = [
        line.strip()
        for line in str(info.get("targets") or "").splitlines()
        if line.strip()
    ]
    if not targets:
        put_red_text("请输入至少一个表名")
        return
    job_index = load_job_index()
    table_job_map = build_table_job_map(job_index)
    for index, target in enumerate(targets, start=1):
        try:
            analyze_one(target, job_index, table_job_map, max_depth, index, direction)
        except Exception as exc:
            safe_put_error(exc)


if __name__ == "__main__":
    start_pywebio_app("表血缘双向漫游", main)
