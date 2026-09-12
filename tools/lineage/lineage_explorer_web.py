"""PyWebIO adapter for bounded DWS-backed Lineage Explorer queries.

The page exposes only environment, root, direction, view and safe traversal
limits.  All scope/profile resolution remains in the shared domain layer; this
module only opens the request connection, calls Query Service, and renders its
Domain Graph projection.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from html import escape
from time import perf_counter
from typing import Any, Callable

from shared.lineage.dws_query import (
    DWSActiveSnapshotNotFoundError,
    DWSLineageEdgeReader,
    DWSLineageReaderError,
    LINEAGE_ACTIVE_SNAPSHOT_INVALID,
    LINEAGE_ACTIVE_SNAPSHOT_NOT_FOUND,
)
from shared.lineage.environment_scope import (
    DISABLED_LINEAGE_ENVIRONMENT,
    LINEAGE_SCOPE_CONFIG_INVALID,
    LINEAGE_SCOPE_CONFIG_NOT_FOUND,
    UNKNOWN_LINEAGE_ENVIRONMENT,
    LineageEnvironmentScope,
    LineageEnvironmentScopeError,
    LineageEnvironmentScopeResolver,
    load_lineage_environment_scope_resolver,
)
from shared.lineage.query import (
    LineageDirection,
    LineageQueryResult,
    LineageQueryService,
    LineageQueryTiming,
    LineageView,
)
from shared.ui.pywebio_helper import put_red_text
from tools.lineage.reconcile_sql_schedule_web import build_environment_options

DEFAULT_EXPLORER_DEPTH = 1
DEFAULT_EXPLORER_MAX_NODES = 100
MAX_EXPLORER_DEPTH = 20
MAX_EXPLORER_NODES = 5000

_ROOT_INVALID = "LINEAGE_ROOT_INVALID"
_LINEAGE_ROOT_NOT_FOUND = "LINEAGE_ROOT_NOT_FOUND"
_DWS_CONNECTION_FAILED = "DWS_CONNECTION_FAILED"
_LINEAGE_QUERY_FAILED = "LINEAGE_QUERY_FAILED"

_ERROR_MESSAGES = {
    UNKNOWN_LINEAGE_ENVIRONMENT: "未找到所选环境的 lineage scope 配置。",
    DISABLED_LINEAGE_ENVIRONMENT: "所选环境已停用，不能执行查询。",
    LINEAGE_SCOPE_CONFIG_NOT_FOUND: "未找到 lineage scope 配置文件。",
    LINEAGE_SCOPE_CONFIG_INVALID: "lineage scope 配置无效。",
    LINEAGE_ACTIVE_SNAPSHOT_NOT_FOUND: "当前 DWS 没有可用的 active lineage snapshot。",
    LINEAGE_ACTIVE_SNAPSHOT_INVALID: "DWS active lineage snapshot 状态无效。",
    _ROOT_INVALID: "root 必须是合法的 qualified schema.table。",
    _LINEAGE_ROOT_NOT_FOUND: "root 不存在于当前 active lineage projection。",
    _DWS_CONNECTION_FAILED: "DWS 连接失败，请检查已配置的 environment scope。",
    _LINEAGE_QUERY_FAILED: "DWS lineage 查询失败。",
}


@dataclass(frozen=True, slots=True)
class LineageExplorerRequest:
    """Validated, UI-facing query parameters without deployment profiles."""

    environment: str
    root: str
    direction: LineageDirection
    view: LineageView
    depth: int = DEFAULT_EXPLORER_DEPTH
    max_nodes: int = DEFAULT_EXPLORER_MAX_NODES


@dataclass(frozen=True, slots=True)
class LineageExplorerErrorView:
    code: str
    message: str

    def as_text(self) -> str:
        return f"{self.code}: {self.message}"


ReaderFactory = Callable[..., DWSLineageEdgeReader]
ConnectionFactory = Callable[[str], Any]


class LineageExplorerDomainError(ValueError):
    """An expected Explorer-domain error suitable for user-facing mapping."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class LineageExplorerConnectionError(RuntimeError):
    """A connection failure kept separate from DWS query failures."""

    code = _DWS_CONNECTION_FAILED


def build_explorer_request(
    resolver: LineageEnvironmentScopeResolver,
    *,
    environment: object,
    root: object,
    direction: object,
    view: object,
    depth: object = DEFAULT_EXPLORER_DEPTH,
    max_nodes: object = DEFAULT_EXPLORER_MAX_NODES,
) -> tuple[LineageEnvironmentScope, LineageExplorerRequest]:
    """Resolve environment and validate all user-controlled query boundaries."""

    if not isinstance(resolver, LineageEnvironmentScopeResolver):
        raise TypeError("resolver must be a LineageEnvironmentScopeResolver")
    scope = resolver.resolve(str(environment or ""))
    normalized_root = _normalize_root(root)
    resolved_direction = _resolve_direction(direction)
    resolved_view = _resolve_view(view)
    resolved_depth = _validate_limit(depth, "depth", minimum=0, maximum=MAX_EXPLORER_DEPTH)
    resolved_max_nodes = _validate_limit(
        max_nodes,
        "max_nodes",
        minimum=1,
        maximum=MAX_EXPLORER_NODES,
    )
    return scope, LineageExplorerRequest(
        environment=scope.environment,
        root=normalized_root,
        direction=resolved_direction,
        view=resolved_view,
        depth=resolved_depth,
        max_nodes=resolved_max_nodes,
    )


def execute_explorer_query(
    scope: LineageEnvironmentScope,
    request: LineageExplorerRequest,
    *,
    connection: Any | None = None,
    connection_factory: ConnectionFactory | None = None,
    reader_factory: ReaderFactory = DWSLineageEdgeReader,
) -> LineageQueryResult:
    """Execute one bounded query with one request-scoped DWS connection."""

    if request.environment != scope.environment:
        raise ValueError("request environment does not match resolved scope")
    timing = LineageQueryTiming()
    owned_connection = connection is None
    request_connection = connection
    query_started = perf_counter()
    try:
        if request_connection is None:
            connect = connection_factory or _connect_with_profile
            connect_started = perf_counter()
            try:
                request_connection = connect(scope.dws_profile)
                if request_connection is None:
                    raise RuntimeError("DWS connection factory returned no connection")
            except Exception as error:
                raise LineageExplorerConnectionError() from error
            finally:
                timing.connection_ms = int((perf_counter() - connect_started) * 1000)
        reader = reader_factory(connection=request_connection)
        result = LineageQueryService(reader).query(
            request.root,
            request.environment,
            request.direction,
            depth=request.depth,
            max_nodes=request.max_nodes,
            source_profile=scope.sql_source_profile,
            view=request.view,
            timing=timing,
        )
        timing.total_ms = int((perf_counter() - query_started) * 1000)
        if result.root_found is False:
            raise LineageExplorerDomainError(_LINEAGE_ROOT_NOT_FOUND)
        return result
    finally:
        if owned_connection:
            _close_quietly(request_connection)


def map_explorer_error(error: Exception) -> LineageExplorerErrorView:
    """Map coded scope/DWS/root failures without exposing credentials."""

    if isinstance(error, LineageEnvironmentScopeError):
        return LineageExplorerErrorView(
            error.code,
            _ERROR_MESSAGES.get(error.code, "lineage scope 配置错误。"),
        )
    if isinstance(error, (LineageExplorerDomainError, LineageExplorerConnectionError)):
        return LineageExplorerErrorView(
            error.code,
            _ERROR_MESSAGES.get(error.code, "lineage root 或 DWS 连接不可用。"),
        )
    if isinstance(error, DWSActiveSnapshotNotFoundError):
        return LineageExplorerErrorView(
            LINEAGE_ACTIVE_SNAPSHOT_NOT_FOUND,
            _ERROR_MESSAGES[LINEAGE_ACTIVE_SNAPSHOT_NOT_FOUND],
        )
    if isinstance(error, DWSLineageReaderError):
        return LineageExplorerErrorView(
            error.code,
            _ERROR_MESSAGES.get(error.code, "DWS lineage snapshot 错误。"),
        )
    if isinstance(error, (ConnectionError, TimeoutError)):
        return LineageExplorerErrorView(
            _DWS_CONNECTION_FAILED,
            _ERROR_MESSAGES[_DWS_CONNECTION_FAILED],
        )
    if isinstance(error, ValueError) and "qualified schema.table" in str(error):
        return LineageExplorerErrorView(_ROOT_INVALID, _ERROR_MESSAGES[_ROOT_INVALID])
    return LineageExplorerErrorView(_LINEAGE_QUERY_FAILED, _ERROR_MESSAGES[_LINEAGE_QUERY_FAILED])


def build_graph_payload(result: LineageQueryResult) -> dict[str, object]:
    """Convert Domain Graph to renderer input without changing core lineage types."""

    root = result.root
    nodes = [
        {
            "id": node.id,
            "label": node.table,
            "depth": node.depth,
            "root": node.id == root,
        }
        for node in result.nodes
    ]
    return {
        "root": root,
        "direction": None if result.direction is None else result.direction.value,
        "nodes": nodes,
        "edges": [edge.to_dict() for edge in result.edges],
        "truncated": result.truncated,
    }


def build_summary(result: LineageQueryResult) -> str:
    """Build the compact business-facing result summary."""

    return (
        f"environment = `{result.environment or '-'}`；"
        f"root = `{result.root or '-'}`；"
        f"direction = `{result.direction.value if result.direction else '-'}`；"
        f"view = `{result.view.value}`；"
        f"nodes = `{len(result.nodes)}`；"
        f"edges = `{len(result.edges)}`；"
        f"max_depth = `{result.max_depth}`；"
        f"truncated = `{str(result.truncated).lower()}`；"
        f"elapsed = `{result.timing.total_ms if result.timing else 0} ms`"
    )


def build_graph_html(graph: dict[str, object], index: int = 1) -> str:
    """Render a small dependency-free SVG graph with zoom, pan and fit."""

    graph_json = json.dumps(graph, ensure_ascii=False, separators=(",", ":"))
    root_id = f"lineage-explorer-{index}"
    svg_id = f"{root_id}-svg"
    detail_id = f"{root_id}-detail"
    marker_id = f"{root_id}-arrow"
    warning = (
        '<div class="lineage-explorer-warning">结果已达到最大深度或节点限制，当前图未完全展开。</div>'
        if graph.get("truncated")
        else ""
    )
    return f"""
<style>
#lineage-explorer-{index}{{--line:#d8dee9;--text:#172033;--muted:#64748b;--accent:#2563eb;
width:min(96vw,1760px);margin:14px auto 28px;border:1px solid var(--line);border-radius:10px;
background:#f6f8fb;color:var(--text);overflow:hidden;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Microsoft YaHei",sans-serif}}
#lineage-explorer-{index} *{{box-sizing:border-box}}
#lineage-explorer-{index} .lineage-explorer-toolbar{{display:flex;align-items:center;gap:8px;flex-wrap:wrap;padding:12px 16px;background:#fff;border-bottom:1px solid var(--line)}}
#lineage-explorer-{index} button{{border:1px solid #cbd5e1;border-radius:6px;background:#fff;color:#334155;padding:5px 10px;cursor:pointer}}
#lineage-explorer-{index} button:hover{{border-color:#2563eb;color:#2563eb}}
#lineage-explorer-{index} .lineage-explorer-meta{{margin-left:auto;color:var(--muted);font-size:12px}}
#lineage-explorer-{index} .lineage-explorer-warning{{margin:12px 16px 0;padding:8px 10px;border:1px solid #fdba74;border-radius:7px;background:#fff7ed;color:#9a3412;font-size:13px}}
#lineage-explorer-{index} .lineage-explorer-workspace{{display:grid;grid-template-columns:minmax(0,1fr) 320px;gap:12px;padding:14px}}
#lineage-explorer-{index} .lineage-explorer-canvas,#lineage-explorer-{index} .lineage-explorer-detail{{border:1px solid var(--line);border-radius:8px;background:#fff}}
#lineage-explorer-{index} .lineage-explorer-canvas{{height:min(70vh,720px);overflow:hidden;touch-action:none}}
#lineage-explorer-{index} svg{{display:block;width:100%;height:100%;background:linear-gradient(90deg,#fff,#f8fafc)}}
#lineage-explorer-{index} .lineage-edge{{stroke:#94a3b8;stroke-width:1.4;fill:none}}
#lineage-explorer-{index} .lineage-node{{cursor:pointer;outline:none}}
#lineage-explorer-{index} .lineage-node rect{{fill:#fff;stroke:#94a3b8;stroke-width:1.2}}
#lineage-explorer-{index} .lineage-node text{{fill:#334155;font:12px Consolas,"Microsoft YaHei",monospace;pointer-events:none}}
#lineage-explorer-{index} .lineage-node.root rect{{fill:#dcfce7;stroke:#16a34a;stroke-width:2}}
#lineage-explorer-{index} .lineage-node.selected rect{{fill:#dbeafe;stroke:#2563eb;stroke-width:2}}
#lineage-explorer-{index} .lineage-explorer-detail{{padding:15px;align-self:start;min-height:180px;max-height:70vh;overflow:auto}}
#lineage-explorer-{index} .lineage-explorer-detail h3{{margin:0 0 12px;font:700 14px Consolas,"Microsoft YaHei",monospace;word-break:break-all}}
#lineage-explorer-{index} .lineage-explorer-detail dl{{display:grid;grid-template-columns:90px 1fr;gap:8px;margin:0;font-size:13px}}
#lineage-explorer-{index} .lineage-explorer-detail dt{{color:var(--muted)}}
#lineage-explorer-{index} .lineage-explorer-detail dd{{margin:0;word-break:break-all}}
#lineage-explorer-{index} .lineage-explorer-empty{{color:var(--muted);font-size:13px;line-height:1.7}}
@media(max-width:900px){{#lineage-explorer-{index}{{width:100%;margin:12px 0 24px}}#lineage-explorer-{index} .lineage-explorer-workspace{{grid-template-columns:1fr}}}}
</style>
<div id="{root_id}">
  <div class="lineage-explorer-toolbar">
    <strong>血缘图</strong>
    <button type="button" data-action="zoom-in">放大</button>
    <button type="button" data-action="zoom-out">缩小</button>
    <button type="button" data-action="fit">适应</button>
    <button type="button" data-action="reset">重置高亮</button>
    <span class="lineage-explorer-meta">点击节点查看完整表名；滚轮缩放，拖拽平移</span>
  </div>
  {warning}
  <div class="lineage-explorer-workspace">
    <div class="lineage-explorer-canvas"><svg id="{svg_id}" role="img" aria-label="血缘图"></svg></div>
    <aside id="{detail_id}" class="lineage-explorer-detail"><div class="lineage-explorer-empty">点击节点查看完整表名和深度。</div></aside>
  </div>
</div>
<script>
(function(){{
const graph={graph_json},root=document.getElementById("{root_id}"),svg=document.getElementById("{svg_id}"),detail=document.getElementById("{detail_id}");
const nodeW=280,nodeH=42,colGap=360,rowGap=64,pad=34,byId=new Map((graph.nodes||[]).map(n=>[n.id,n]));
const depths=[...(new Set((graph.nodes||[]).map(n=>n.depth)))].sort((a,b)=>a-b),cols={{}};
for(const n of graph.nodes||[])((cols[n.depth]||(cols[n.depth]=[])).push(n));
const maxRows=Math.max(1,...depths.map(d=>(cols[d]||[]).length));
const width=pad*2+Math.max(0,depths.length-1)*colGap+nodeW,height=pad*2+Math.max(0,maxRows-1)*rowGap+nodeH;
for(const d of depths){{const values=cols[d],offset=(maxRows-values.length)*rowGap/2;values.sort((a,b)=>a.label.localeCompare(b.label)).forEach((n,i)=>{{n.x=pad+depths.indexOf(d)*colGap;n.y=pad+offset+i*rowGap}})}}
svg.setAttribute("viewBox",`0 0 ${{width}} ${{height}}`);
const ns="http://www.w3.org/2000/svg",defs=document.createElementNS(ns,"defs"),marker=document.createElementNS(ns,"marker");marker.id="{marker_id}";marker.setAttribute("markerWidth","8");marker.setAttribute("markerHeight","8");marker.setAttribute("refX","7");marker.setAttribute("refY","4");marker.setAttribute("orient","auto");const arrow=document.createElementNS(ns,"path");arrow.setAttribute("d","M0,0 L8,4 L0,8 Z");arrow.setAttribute("fill","#94a3b8");marker.appendChild(arrow);defs.appendChild(marker);svg.appendChild(defs);
const world=document.createElementNS(ns,"g");svg.appendChild(world);const edgeEls=[];
for(const edge of graph.edges||[]){{const a=byId.get(edge.source),b=byId.get(edge.target);if(!a||!b)continue;const p=document.createElementNS(ns,"path"),mid=(a.x+nodeW+b.x)/2;p.setAttribute("d",`M${{a.x+nodeW}} ${{a.y+nodeH/2}} C${{mid}} ${{a.y+nodeH/2}} ${{mid}} ${{b.y+nodeH/2}} ${{b.x}} ${{b.y+nodeH/2}}`);p.setAttribute("class","lineage-edge");p.setAttribute("marker-end","url(#{marker_id})");p.dataset.source=edge.source;p.dataset.target=edge.target;world.appendChild(p);edgeEls.push(p)}}
const nodeEls=new Map();for(const n of graph.nodes||[]){{const g=document.createElementNS(ns,"g");g.setAttribute("class",`lineage-node${{n.root?" root":""}}`);g.setAttribute("tabindex","0");const r=document.createElementNS(ns,"rect");r.setAttribute("x",n.x);r.setAttribute("y",n.y);r.setAttribute("width",nodeW);r.setAttribute("height",nodeH);r.setAttribute("rx","7");const t=document.createElementNS(ns,"text");t.setAttribute("x",n.x+12);t.setAttribute("y",n.y+26);t.textContent=n.label.length>36?n.label.slice(0,33)+"...":n.label;const title=document.createElementNS(ns,"title");title.textContent=n.label;g.append(r,t,title);world.appendChild(g);nodeEls.set(n.id,g);g.onclick=()=>select(n.id);g.onkeydown=e=>{{if(e.key==="Enter"||e.key===" "){{e.preventDefault();select(n.id)}}}}}}
function esc(s){{return String(s==null?"":s).replace(/[&<>"']/g,c=>({{"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}}[c]))}}
function select(id){{for(const [nid,el] of nodeEls)el.classList.toggle("selected",nid===id);const n=byId.get(id);if(!n)return;detail.innerHTML=`<h3>${{esc(n.label)}}</h3><dl><dt>完整表名</dt><dd>${{esc(n.label)}}</dd><dt>深度</dt><dd>${{n.depth}}</dd><dt>节点身份</dt><dd>${{n.root?"root":"lineage asset"}}</dd></dl>`}}
function clearSelection(){{for(const el of nodeEls.values())el.classList.remove("selected");detail.innerHTML='<div class="lineage-explorer-empty">点击节点查看完整表名和深度。</div>'}}
let scale=1,tx=0,ty=0,drag=null;function draw(){{world.setAttribute("transform",`translate(${{tx}} ${{ty}}) scale(${{scale}})`);}}function fit(){{const box=svg.getBoundingClientRect(),sx=box.width/width,sy=box.height/height;scale=Math.max(.25,Math.min(1,sx,sy)*.9);tx=(box.width/scale-width)/2;ty=(box.height/scale-height)/2;draw()}}function zoom(f){{scale=Math.max(.2,Math.min(4,scale*f));draw()}}
root.querySelector('[data-action="zoom-in"]').onclick=()=>zoom(1.2);root.querySelector('[data-action="zoom-out"]').onclick=()=>zoom(.83);root.querySelector('[data-action="fit"]').onclick=fit;root.querySelector('[data-action="reset"]').onclick=clearSelection;svg.addEventListener("wheel",e=>{{e.preventDefault();zoom(e.deltaY<0?1.1:.9)}},{{passive:false}});svg.addEventListener("pointerdown",e=>{{drag={{x:e.clientX,y:e.clientY,tx,ty}};svg.setPointerCapture(e.pointerId)}});svg.addEventListener("pointermove",e=>{{if(!drag)return;tx=drag.tx+(e.clientX-drag.x)/scale;ty=drag.ty+(e.clientY-drag.y)/scale;draw()}});svg.addEventListener("pointerup",()=>drag=null);fit();
}})();
</script>
"""


def render_result(result: LineageQueryResult, *, index: int = 1) -> None:
    """Render result summary, graph, and timing details into the current scope."""

    from pywebio.output import put_collapse, put_html, put_markdown, put_text

    render_started = perf_counter()
    graph = build_graph_payload(result)
    graph_html = build_graph_html(graph, index=index)
    if result.timing is not None:
        result.timing.render_preparation_ms = int(
            (perf_counter() - render_started) * 1000
        )
        result.timing.total_ms += result.timing.render_preparation_ms
    put_markdown("### 查询结果")
    put_text(build_summary(result))
    if result.root_found is True and len(result.nodes) == 1 and not result.edges:
        put_text("当前方向没有邻居；root 在 active lineage projection 中存在。")
    if result.truncated:
        put_text("结果已达到最大深度或节点限制，当前图未完全展开。")
    put_html(graph_html)
    timing = result.timing.as_dict() if result.timing is not None else {}
    put_collapse(
        "详情 / 调试信息",
        content=[
            put_text(f"batch_id = {result.batch_id or '-'}"),
            put_text(json.dumps(timing, ensure_ascii=False)),
        ],
    )


def main(*, resolver: LineageEnvironmentScopeResolver | None = None) -> None:
    """Run the formal PyWebIO Lineage Explorer page."""

    from pywebio.input import NUMBER, TEXT, actions, input, input_group, radio, select
    from pywebio.output import put_markdown, put_scope, use_scope

    try:
        resolved_resolver = resolver or load_lineage_environment_scope_resolver()
    except Exception as error:
        put_red_text(f"环境配置加载失败；{map_explorer_error(error).message}")
        return

    options = build_environment_options(resolved_resolver.enabled_scopes())
    if not options:
        put_red_text("没有启用的 lineage environment scope，请检查本地配置。")
        return
    put_markdown("# 血缘探索")
    put_scope("lineage-explorer-results")
    default_environment = options[0].value
    while True:
        form = input_group(
            "查询条件",
            [
                select(
                    "环境",
                    name="environment",
                    options=[option.as_pywebio_option() for option in options],
                    value=default_environment,
                ),
                input(
                    "表名",
                    name="root",
                    type=TEXT,
                    value="DWF.F_EVT_LONJ_TRANS_LIST",
                    placeholder="例如：DWF.F_EVT_LONJ_TRANS_LIST",
                ),
                radio(
                    "方向",
                    name="direction",
                    options=[
                        ("上游", LineageDirection.UPSTREAM.value),
                        ("下游", LineageDirection.DOWNSTREAM.value),
                        ("双向", LineageDirection.BOTH.value),
                    ],
                    value=LineageDirection.DOWNSTREAM.value,
                ),
                radio(
                    "视图",
                    name="view",
                    options=[
                        ("业务血缘", LineageView.BUSINESS.value),
                        ("物理血缘", LineageView.PHYSICAL.value),
                    ],
                    value=LineageView.BUSINESS.value,
                ),
                input(
                    "深度",
                    name="depth",
                    type=NUMBER,
                    value=DEFAULT_EXPLORER_DEPTH,
                ),
                input(
                    "最大节点",
                    name="max_nodes",
                    type=NUMBER,
                    value=DEFAULT_EXPLORER_MAX_NODES,
                ),
                actions(
                    "操作",
                    name="action",
                    buttons=[
                        {"label": "查询", "value": "query"},
                        {"label": "清空结果", "value": "reset"},
                    ],
                ),
            ],
        )
        with use_scope("lineage-explorer-results", clear=True):
            if form.get("action") == "reset":
                default_environment = str(form.get("environment") or default_environment)
                continue
            try:
                scope, request = build_explorer_request(
                    resolved_resolver,
                    environment=form.get("environment"),
                    root=form.get("root"),
                    direction=form.get("direction"),
                    view=form.get("view"),
                    depth=form.get("depth"),
                    max_nodes=form.get("max_nodes"),
                )
                result = execute_explorer_query(scope, request)
                render_result(result)
                default_environment = scope.environment
            except Exception as error:
                failure = map_explorer_error(error)
                put_red_text(escape(failure.as_text()))


def _connect_with_profile(profile: str) -> Any:
    from shared.db.gaussdb import connect_with_profile

    return connect_with_profile(profile)


def _normalize_root(value: object) -> str:
    from shared.lineage.domain import canonicalize_dataset_name

    normalized = canonicalize_dataset_name(str(value or ""))
    if normalized is None:
        raise ValueError("root must be a qualified schema.table")
    return normalized


def _resolve_direction(value: object) -> LineageDirection:
    try:
        return LineageDirection(value)
    except (TypeError, ValueError) as exc:
        valid = ", ".join(item.value for item in LineageDirection)
        raise ValueError(f"direction must be one of: {valid}") from exc


def _resolve_view(value: object) -> LineageView:
    try:
        return LineageView(value)
    except (TypeError, ValueError) as exc:
        valid = ", ".join(item.value for item in LineageView)
        raise ValueError(f"view must be one of: {valid}") from exc


def _validate_limit(value: object, field_name: str, *, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} 必须是整数。")
    if value < minimum or value > maximum:
        raise ValueError(f"{field_name} 必须在 {minimum} 到 {maximum} 之间。")
    return value


def _close_quietly(resource: Any) -> None:
    close = getattr(resource, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            pass


if __name__ == "__main__":
    from shared.ui.pywebio_helper import start_pywebio_app

    start_pywebio_app("血缘探索", main)
