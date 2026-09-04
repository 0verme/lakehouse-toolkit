from __future__ import annotations

import json
import time
from html import escape
from importlib import import_module

from shared.lineage.lineage_builder import (
    ProcessInfo,
    build_lineage_graph_with_targeted_schedule_times,
    load_process_infos,
    load_schedule_map,
    process_target_name,
    process_task_name,
)
from shared.ui.pywebio_helper import (
    put_black_text,
    put_red_text,
    safe_put_error,
    start_pywebio_app,
)

_pywebio_output = import_module("pywebio.output")
put_html = _pywebio_output.put_html
put_table = _pywebio_output.put_table


DEFAULT_MAX_DEPTH = 5
LARGE_GRAPH_NODE_THRESHOLD = 150

TYPE_LABEL = {
    "root_table": "根目标表",
    "process": "pipeline任务",
    "table": "中间表",
    "source_table": "源头表",
}


ROAMER_CSS = r"""
<style>
.lineage-roamer {
  --bg: #f6f8fb;
  --surface: #ffffff;
  --surface-2: #ffffff;
  --line: #e5e7eb;
  --line-2: #cbd5e1;
  --text: #111827;
  --text-2: #374151;
  --text-3: #6b7280;
  --accent: #2563eb;
  --signal: #0ea5e9;
  --signal-soft: #e0f2fe;
  --t-root_table: #059669;
  --t-process: #d97706;
  --t-table: #2563eb;
  --t-source_table: #64748b;
  width: min(96vw, 1760px);
  margin: 16px calc(50% - min(48vw, 880px)) 32px;
  border: 1px solid var(--line);
  border-radius: 10px;
  background: var(--bg);
  color: var(--text);
  overflow: hidden;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", Arial, sans-serif;
}
.lineage-roamer * { box-sizing: border-box; }
.lineage-roamer .bar {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
  flex-wrap: wrap;
  padding: 14px 18px;
  border-bottom: 1px solid var(--line);
  background: var(--surface);
}
.lineage-roamer .cap {
  display: block;
  color: var(--text);
  font-size: 15px;
  font-weight: 700;
}
.lineage-roamer .subcap {
  display: block;
  margin-top: 3px;
  color: var(--text-3);
  font-size: 12px;
}
.lineage-roamer .meta {
  color: var(--text-2);
  font-family: Consolas, "Liberation Mono", monospace;
  font-size: 12px;
  white-space: nowrap;
}
.lineage-roamer .warn {
  width: 100%;
  margin-top: 10px;
  padding: 9px 12px;
  border: 1px solid #bae6fd;
  border-radius: 8px;
  background: #f0f9ff;
  color: #075985;
  font-size: 13px;
  line-height: 1.6;
}
.lineage-roamer .workspace {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 340px;
  gap: 14px;
  padding: 14px;
}
.lineage-roamer .graph-panel,
.lineage-roamer .detail-panel {
  border: 1px solid var(--line);
  border-radius: 8px;
  background: var(--surface);
}
.lineage-roamer .graph-panel {
  min-width: 0;
  overflow: auto;
  max-height: 70vh;
}
.lineage-roamer .graph-scroll {
  padding: 22px;
}
.lineage-roamer svg {
  display: block;
  width: auto;
  height: auto;
  max-width: none;
  cursor: crosshair;
}
.lineage-roamer .node { outline: none; }
.lineage-roamer .node rect {
  fill: var(--surface-2);
  stroke: var(--line-2);
  stroke-width: 1.2;
  transition: .18s ease;
}
.lineage-roamer .node.alert-red rect {
  fill: #fee2e2;
  stroke: #dc2626;
}
.lineage-roamer .node.alert-orange rect {
  fill: #ffedd5;
  stroke: #ea580c;
}
.lineage-roamer .node.alert-amber rect {
  fill: #fef3c7;
  stroke: #d97706;
}
.lineage-roamer .node text {
  fill: var(--text-2);
  font-family: Consolas, "Liberation Mono", monospace;
  font-size: 12px;
  pointer-events: none;
  transition: .18s ease;
}
.lineage-roamer .node .dot { transition: .18s ease; }
.lineage-roamer .edge {
  stroke: var(--line-2);
  stroke-width: 1.25;
  fill: none;
  transition: .18s ease;
}
.lineage-roamer .dot.type-root_table { fill: var(--t-root_table); }
.lineage-roamer .dot.type-process { fill: var(--t-process); }
.lineage-roamer .dot.type-table { fill: var(--t-table); }
.lineage-roamer .dot.type-source_table { fill: var(--t-source_table); }
.lineage-roamer svg.dim .node rect,
.lineage-roamer svg.dim .node text,
.lineage-roamer svg.dim .node .dot { opacity: .42; }
.lineage-roamer svg.dim .edge { opacity: .22; }
.lineage-roamer .node.hot rect {
  fill: var(--signal-soft);
  stroke: var(--accent);
  stroke-width: 2;
  opacity: 1 !important;
}
.lineage-roamer .node.hot text {
  fill: var(--text);
  font-weight: 700;
  opacity: 1 !important;
}
.lineage-roamer .node.hot .dot { opacity: 1 !important; }
.lineage-roamer .node.lit-down rect,
.lineage-roamer .node.lit-up rect {
  opacity: 1 !important;
}
.lineage-roamer .node.lit-down rect { stroke: var(--signal); }
.lineage-roamer .node.lit-up rect { stroke: var(--accent); }
.lineage-roamer .node.lit-down text,
.lineage-roamer .node.lit-up text,
.lineage-roamer .node.lit-down .dot,
.lineage-roamer .node.lit-up .dot {
  opacity: 1 !important;
}
.lineage-roamer .node.lit-down text,
.lineage-roamer .node.lit-up text { fill: var(--text); }
.lineage-roamer .edge.lit-down {
  stroke: var(--signal);
  stroke-width: 1.8;
  opacity: 1 !important;
  stroke-dasharray: 4 6;
  animation: lineage-flow .7s linear infinite;
}
.lineage-roamer .edge.lit-up {
  stroke: var(--accent);
  stroke-width: 1.8;
  opacity: 1 !important;
}
@keyframes lineage-flow { to { stroke-dashoffset: -10; } }
.lineage-roamer .detail-panel {
  min-width: 0;
  padding: 16px;
  box-shadow: 0 10px 30px rgba(15, 23, 42, .07);
  align-self: start;
  max-height: 70vh;
  overflow: auto;
}
.lineage-roamer .card-empty {
  color: var(--text-3);
  font-size: 13px;
  line-height: 1.7;
}
.lineage-roamer .card-title-row {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 8px;
}
.lineage-roamer .card-title {
  display: block;
  color: var(--text);
  font-family: Consolas, "Liberation Mono", monospace;
  font-size: 14px;
  font-weight: 700;
  line-height: 1.45;
  word-break: break-all;
}
.lineage-roamer .card-type {
  display: inline-block;
  margin-top: 10px;
  padding: 2px 8px;
  border-radius: 999px;
  border: 1px solid var(--line);
  background: #f8fafc;
  color: var(--text-2);
  font-size: 12px;
}
.lineage-roamer .card dl {
  display: grid;
  grid-template-columns: 88px minmax(0, 1fr);
  gap: 9px 10px;
  margin: 14px 0 0;
  font-size: 13px;
}
.lineage-roamer .card dt {
  color: var(--text-3);
  white-space: nowrap;
}
.lineage-roamer .card dd {
  margin: 0;
  color: var(--text-2);
  word-break: break-all;
  line-height: 1.55;
}
.lineage-roamer .card-close {
  flex: 0 0 auto;
  width: 26px;
  height: 26px;
  border: 1px solid var(--line);
  border-radius: 6px;
  background: var(--surface);
  color: var(--text-3);
  cursor: pointer;
  font-size: 18px;
  line-height: 1;
}
.lineage-roamer .card-close:hover {
  border-color: var(--line-2);
  color: var(--text);
  background: #f8fafc;
}
.lineage-roamer .legend {
  display: flex;
  flex-wrap: wrap;
  gap: 16px;
  padding: 0 18px 16px;
  color: var(--text-2);
  font-family: Consolas, "Liberation Mono", monospace;
  font-size: 12px;
}
.lineage-roamer .legend span {
  display: inline-flex;
  align-items: center;
  gap: 7px;
}
.lineage-roamer .legend i {
  display: inline-block;
  width: 8px;
  height: 8px;
  border-radius: 50%;
}
@media (max-width: 900px) {
  .lineage-roamer {
    width: 100%;
    margin: 16px 0 32px;
  }
  .lineage-roamer .workspace {
    grid-template-columns: 1fr;
  }
  .lineage-roamer .detail-panel {
    max-height: none;
  }
}
@media (prefers-reduced-motion: reduce) {
  .lineage-roamer * {
    animation-duration: .001ms !important;
    transition-duration: .001ms !important;
  }
  .lineage-roamer .edge.lit-down {
    stroke-dasharray: none;
  }
}
</style>
"""


ROAMER_JS = r"""
<script>
(function(){
  const rawGraph = __GRAPH__;
  const root = document.getElementById("__ROOT_ID__");
  if (!root || !rawGraph || !rawGraph.nodes) return;
  const svg = root.querySelector("svg");
  const detailPanel = root.querySelector(".detail-panel");
  const metaEl = root.querySelector(".js-meta");
  const typeLabel = __TYPE_LABEL__;
  const layout = {padX:24, padY:28, colGap:330, nodeW:260, nodeH:34, minRowGap:24};

  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function(c) {
      return {"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c];
    });
  }
  function buildAdj(edges, reverse) {
    const out = {};
    for (const edge of edges) {
      const from = reverse ? edge[1] : edge[0];
      const to = reverse ? edge[0] : edge[1];
      (out[from] || (out[from] = [])).push(to);
    }
    return out;
  }
  function reach(start, adj) {
    const seen = new Set();
    const stack = [...(adj[start] || [])];
    while (stack.length) {
      const id = stack.pop();
      if (seen.has(id)) continue;
      seen.add(id);
      for (const next of adj[id] || []) {
        if (!seen.has(next)) stack.push(next);
      }
    }
    return seen;
  }
  function shortLabel(label) {
    const text = String(label || "");
    const last = text.includes(".") ? text.split(".").pop() : text;
    return last.length > 30 ? last.slice(0, 27) + "..." : last;
  }
  function renderNodeLabel(node) {
    const base = shortLabel(node && node.label);
    if (!node) return base;
    if (node.type === "process") return `任务 ${base}`;
    if (node.type === "root_table") return `目标 ${base}`;
    return base;
  }
  function compareNodeSort(a, b) {
    const aHasTime = a && a.sort_has_time ? 0 : 1;
    const bHasTime = b && b.sort_has_time ? 0 : 1;
    if (aHasTime !== bHasTime) return aHasTime - bHasTime;
    const aTime = Number.isFinite(a && a.sort_time_seconds) ? a.sort_time_seconds : 999999999;
    const bTime = Number.isFinite(b && b.sort_time_seconds) ? b.sort_time_seconds : 999999999;
    if (aTime !== bTime) return aTime - bTime;
    const aType = Number.isFinite(a && a.type_order) ? a.type_order : 99;
    const bType = Number.isFinite(b && b.type_order) ? b.type_order : 99;
    if (aType !== bType) return aType - bType;
    const aLabel = String((a && a.label) || "");
    const bLabel = String((b && b.label) || "");
    if (aLabel !== bLabel) return aLabel.localeCompare(bLabel);
    return String((a && a.id) || "").localeCompare(String((b && b.id) || ""));
  }
  function uniqueSorted(values) {
    return [...new Set(values)].sort();
  }
  function buildDisplayGraph(source) {
    const nodes = (source.nodes || []).filter(node => node.type !== "process").map(node => ({...node}));
    const nodeIds = new Set(nodes.map(node => node.id));
    const forward = buildAdj(source.edges || [], false);
    const reverse = buildAdj(source.edges || [], true);
    const edges = [];
    const seen = new Set();
    for (const node of source.nodes || []) {
      if (node.type !== "process") continue;
      const upstreamIds = uniqueSorted((reverse[node.id] || []).filter(id => nodeIds.has(id)));
      const downstreamIds = uniqueSorted((forward[node.id] || []).filter(id => nodeIds.has(id)));
      for (const fromId of upstreamIds) {
        for (const toId of downstreamIds) {
          if (fromId === toId) continue;
          const edgeKey = `${fromId}=>${toId}`;
          if (seen.has(edgeKey)) continue;
          seen.add(edgeKey);
          edges.push([fromId, toId]);
        }
      }
    }
    for (const edge of source.edges || []) {
      if (!nodeIds.has(edge[0]) || !nodeIds.has(edge[1]) || edge[0] === edge[1]) continue;
      const edgeKey = `${edge[0]}=>${edge[1]}`;
      if (seen.has(edgeKey)) continue;
      seen.add(edgeKey);
      edges.push([edge[0], edge[1]]);
    }
    return {nodes, edges};
  }
  const graph = buildDisplayGraph(rawGraph);
  if (metaEl) {
    metaEl.textContent = `nodes=${graph.nodes.length} · edges=${graph.edges.length} · max_depth=${rawGraph.max_depth}`;
  }
  function renderEmptyDetail() {
    detailPanel.innerHTML = '<div class="card-empty">悬停节点可临时高亮上下游；点击节点后在这里查看完整名称、上下游数量和 pipeline 任务明细。按 Esc 或点击图空白处可取消锁定。</div>';
  }
  function applyLayout() {
    const cols = new Map();
    for (const node of graph.nodes) {
      if (!cols.has(node.col)) cols.set(node.col, []);
      cols.get(node.col).push(node);
    }
    const colIndexes = [...cols.keys()].sort((a, b) => a - b);
    const maxRows = Math.max(1, ...[...cols.values()].map(nodes => nodes.length));
    const contentH = maxRows * layout.nodeH + (maxRows - 1) * layout.minRowGap;
    const width = layout.padX * 2 + (colIndexes.length - 1) * layout.colGap + layout.nodeW;
    const height = contentH + layout.padY * 2;
    for (const colIndex of colIndexes) {
      const nodes = cols.get(colIndex).slice().sort(compareNodeSort);
      const blockH = nodes.length * layout.nodeH + (nodes.length - 1) * layout.minRowGap;
      const startY = layout.padY + Math.max(0, (contentH - blockH) / 2);
      const x = layout.padX + colIndexes.indexOf(colIndex) * layout.colGap;
      nodes.forEach((node, row) => {
        node.x = x;
        node.y = startY + row * (layout.nodeH + layout.minRowGap);
        node.w = layout.nodeW;
      });
    }
    svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
    svg.setAttribute("width", String(width));
    svg.setAttribute("height", String(height));
  }

  const byId = new Map(graph.nodes.map(node => [node.id, node]));
  const adj = buildAdj(graph.edges, false);
  const radj = buildAdj(graph.edges, true);
  const nodeEls = new Map();
  const edgeEls = [];
  let hovered = null;
  let locked = null;

  renderEmptyDetail();
  applyLayout();
  for (const edge of graph.edges) {
    const a = byId.get(edge[0]);
    const b = byId.get(edge[1]);
    if (!a || !b) continue;
    const x1 = a.x + a.w;
    const y1 = a.y + layout.nodeH / 2;
    const x2 = b.x;
    const y2 = b.y + layout.nodeH / 2;
    const mid = (x1 + x2) / 2;
    const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
    path.setAttribute("d", `M${x1} ${y1} C${mid} ${y1} ${mid} ${y2} ${x2} ${y2}`);
    path.setAttribute("class", "edge");
    path.dataset.from = edge[0];
    path.dataset.to = edge[1];
    svg.appendChild(path);
    edgeEls.push(path);
  }
  for (const node of graph.nodes) {
    const g = document.createElementNS("http://www.w3.org/2000/svg", "g");
    const alertClass = node.alert_level ? ` alert-${node.alert_level}` : "";
    g.setAttribute("class", `node${alertClass}`);
    g.setAttribute("tabindex", "0");
    g.setAttribute("role", "button");
    g.dataset.id = node.id;

    const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    rect.setAttribute("x", node.x);
    rect.setAttribute("y", node.y);
    rect.setAttribute("width", node.w);
    rect.setAttribute("height", layout.nodeH);
    rect.setAttribute("rx", "6");

    const dot = document.createElementNS("http://www.w3.org/2000/svg", "circle");
    dot.setAttribute("class", `dot type-${node.type}`);
    dot.setAttribute("cx", node.x + 14);
    dot.setAttribute("cy", node.y + layout.nodeH / 2);
    dot.setAttribute("r", "4");

    const text = document.createElementNS("http://www.w3.org/2000/svg", "text");
    text.setAttribute("x", node.x + 28);
    text.setAttribute("y", node.y + layout.nodeH / 2 + 4);
    text.textContent = renderNodeLabel(node);

    const title = document.createElementNS("http://www.w3.org/2000/svg", "title");
    title.textContent = `${typeLabel[node.type] || node.type}\n${node.label}`;

    g.append(title, rect, dot, text);
    svg.appendChild(g);
    nodeEls.set(node.id, g);
    g.addEventListener("mouseenter", () => onHover(node.id));
    g.addEventListener("focus", () => onHover(node.id));
    g.addEventListener("click", (event) => {
      event.stopPropagation();
      toggleLock(node.id);
    });
    g.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        toggleLock(node.id);
      }
    });
  }

  function onHover(id) {
    hovered = id;
    if (!locked) highlight(id);
  }
  function toggleLock(id) {
    if (locked === id) {
      unlock();
      return;
    }
    locked = id;
    highlight(id);
    showDetail(id);
  }
  function unlock() {
    if (!locked) return;
    locked = null;
    renderEmptyDetail();
    if (hovered) highlight(hovered);
    else clear();
  }
  function clear() {
    svg.classList.remove("dim", "locked");
    for (const el of nodeEls.values()) el.classList.remove("hot", "lit-down", "lit-up");
    for (const edge of edgeEls) edge.classList.remove("lit-down", "lit-up");
  }
  function highlight(id) {
    const down = reach(id, adj);
    const up = reach(id, radj);
    svg.classList.add("dim");
    svg.classList.toggle("locked", locked !== null);
    for (const [nodeId, el] of nodeEls) {
      el.classList.remove("hot", "lit-down", "lit-up");
      if (nodeId === id) el.classList.add("hot");
      else if (down.has(nodeId)) el.classList.add("lit-down");
      else if (up.has(nodeId)) el.classList.add("lit-up");
    }
    for (const edge of edgeEls) {
      edge.classList.remove("lit-down", "lit-up");
      const from = edge.dataset.from;
      const to = edge.dataset.to;
      if ((from === id || down.has(from)) && down.has(to)) edge.classList.add("lit-down");
      else if ((to === id || up.has(to)) && up.has(from)) edge.classList.add("lit-up");
    }
  }
  function showDetail(id) {
    const node = byId.get(id);
    const directDown = (adj[id] || []).slice().sort((leftId, rightId) => compareNodeSort(byId.get(leftId), byId.get(rightId)));
    const allDown = reach(id, adj);
    const allUp = reach(id, radj);
    const detail = node.detail || {};
    const scheduleInfo = node.schedule_time || detail["调度时间"] || "";
    let scheduleMiss = "";
    if (!scheduleInfo || scheduleInfo === "-") {
      if (!("调度时间" in detail)) scheduleMiss = "detail key 不一致";
      else if (node.type === "source_table") scheduleMiss = "source_table fallback 未命中";
      else scheduleMiss = "SQL 无返回或 alias 不匹配";
    }
    console.info("[lineage_lineage] selected_node", {
      label: node.label,
      type: node.type,
      detail: detail,
      schedule_info: scheduleInfo || null,
      miss_reason: scheduleMiss || null,
    });
    const directText = directDown.map(nid => (byId.get(nid) || {}).label || nid).join("、") || "-";
    const detailRows = Object.keys(detail).map(key => `<dt>${esc(key)}</dt><dd>${esc(detail[key])}</dd>`).join("");
    detailPanel.innerHTML = `
      <div class="card">
        <div class="card-title-row">
          <span class="card-title">${esc(node.label)}</span>
          <button class="card-close" type="button" aria-label="关闭">×</button>
        </div>
        <span class="card-type">${esc(typeLabel[node.type] || node.type)}</span>
        <dl>
          <dt>节点类型</dt><dd>${esc(typeLabel[node.type] || node.type)}</dd>
          <dt>全部上游</dt><dd>${allUp.size}</dd>
          <dt>全部下游</dt><dd>${allDown.size}</dd>
          <dt>直接下游</dt><dd>${esc(directText)}</dd>
          ${detailRows}
        </dl>
      </div>
    `;
    detailPanel.querySelector(".card-close").addEventListener("click", (event) => {
      event.stopPropagation();
      unlock();
    });
  }
  svg.addEventListener("mouseleave", () => {
    hovered = null;
    if (!locked) clear();
  });
  svg.addEventListener("click", unlock);
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") unlock();
  });
})();
</script>
"""


def put_candidates(input_name: str, candidates: list[ProcessInfo]):
    put_red_text(f"{input_name} 匹配到多个 pipeline 任务，请输入更精确的 PROCESS_NAME")
    rows = [["来源表", "PROCESS_NAME", "任务名", "目标表"]]
    for item in candidates[:80]:
        rows.append(
            [
                item.source_table,
                item.process_name,
                process_task_name(item.process_name),
                process_target_name(item.process_name),
            ]
        )
    put_table(rows)


def put_graph(graph: dict, container_index: int):
    root_id = f"lineage-roamer-{container_index}"
    node_count = len(graph.get("nodes", []))
    edge_count = len(graph.get("edges", []))
    legend = "".join(
        f'<span><i class="dot type-{escape(node_type)}"></i>{escape(label)}</span>'
        for node_type, label in TYPE_LABEL.items()
        if node_type != "process"
    )

    warning_items = list(graph.get("warnings", []))
    if node_count > LARGE_GRAPH_NODE_THRESHOLD:
        warning_items.append(
            "当前节点较多，建议降低递归深度，或点击节点聚焦查看相关上下游。"
        )
    if graph.get("truncated"):
        warning_items.append("递归达到最大深度，图中链路已截断。")
    if graph.get("cycles"):
        warning_items.append(
            f"检测到环路 {len(graph.get('cycles', []))} 条，已停止对应分支继续展开。"
        )
    warnings = "".join(f"<div>{escape(item)}</div>" for item in warning_items)
    warning_html = f'<div class="warn">{warnings}</div>' if warnings else ""
    html = f"""
{ROAMER_CSS}
<div class="lineage-roamer" id="{root_id}">
  <div class="bar">
    <div>
      <span class="cap">pipeline 上游血缘漫游</span>
      <span class="subcap">主链路基于 SCRIPT_CODE 的 FROM / JOIN / USING 实际引用关系</span>
    </div>
    <span class="meta js-meta">nodes={node_count} · edges={edge_count} · max_depth={escape(str(graph.get("max_depth")))}</span>
    {warning_html}
  </div>
  <div class="workspace">
    <div class="graph-panel">
      <div class="graph-scroll">
        <svg role="img" aria-label="pipeline 上游血缘图"></svg>
      </div>
    </div>
    <aside class="detail-panel" aria-label="节点详情"></aside>
  </div>
  <div class="legend">{legend}</div>
</div>
"""
    js = ROAMER_JS.replace("__GRAPH__", json.dumps(graph, ensure_ascii=False))
    js = js.replace("__ROOT_ID__", root_id)
    js = js.replace("__TYPE_LABEL__", json.dumps(TYPE_LABEL, ensure_ascii=False))
    put_html(html + js)


def analyze_one(
    input_name: str,
    process_infos: list[ProcessInfo],
    schedule_map: dict[str, set[str]],
    max_depth: int,
    index: int,
    bootstrap_logs: list[str] | None = None,
):
    graph, candidates = build_lineage_graph_with_targeted_schedule_times(
        input_name,
        process_infos,
        schedule_map=schedule_map,
        max_depth=max_depth,
    )
    if candidates:
        put_candidates(input_name, candidates)
        return
    if not graph:
        put_red_text(f"{input_name} 未定位到 pipeline 任务或目标表")
        return

    put_black_text("输入: " + graph.root_input)
    put_black_text("根目标表: " + graph.root_target)
    if graph.root_process:
        put_black_text("根任务: " + graph.root_process)
    put_graph(graph.to_dict(), index)


def main():
    pywebio_input = import_module("pywebio.input")
    input_group = pywebio_input.input_group
    textarea = pywebio_input.textarea
    input_number = pywebio_input.input
    info = input_group(
        "pipeline 上游血缘漫游",
        [
            textarea(
                "请输入 pipeline 任务名或目标表名，每行一个",
                name="targets",
                type=pywebio_input.TEXT,
            ),
            input_number(
                "最大递归深度，默认 5",
                name="max_depth",
                type=pywebio_input.NUMBER,
                value=DEFAULT_MAX_DEPTH,
            ),
        ],
    )

    try:
        max_depth = int(info.get("max_depth") or DEFAULT_MAX_DEPTH)
    except (TypeError, ValueError):
        max_depth = DEFAULT_MAX_DEPTH
    max_depth = max(1, min(max_depth, 30))

    started_at = time.perf_counter()
    process_infos = load_process_infos()
    print(
        f"[lineage_lineage_perf] stage=load_process_infos elapsed={(time.perf_counter() - started_at) * 1000:.1f}ms rows={len(process_infos)}"
    )

    started_at = time.perf_counter()
    schedule_map = load_schedule_map()
    print(
        f"[lineage_lineage_perf] stage=load_schedule_map elapsed={(time.perf_counter() - started_at) * 1000:.1f}ms targets={len(schedule_map)}"
    )
    targets = [
        line.strip()
        for line in str(info.get("targets") or "").splitlines()
        if line.strip()
    ]
    if not targets:
        put_red_text("请输入至少一个 pipeline 任务名或目标表名")
        return

    for index, item in enumerate(targets, start=1):
        try:
            analyze_one(item, process_infos, schedule_map, max_depth, index)
        except Exception as exc:
            safe_put_error(exc)


if __name__ == "__main__":
    start_pywebio_app("pipeline 上游血缘漫游", main)
