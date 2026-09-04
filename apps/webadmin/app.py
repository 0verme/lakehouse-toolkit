from __future__ import annotations

import socket
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import psutil
import streamlit as st
import yaml

st.set_page_config(
    page_title="工具管理台",
    page_icon="🛠",
    layout="wide",
)

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parents[1]
CONFIG_PATH = ROOT_DIR / "configs" / "tools.yaml"
LOCAL_CONFIG_PATH = ROOT_DIR / "configs" / "tools.local.yaml"
MANAGER_PATH = BASE_DIR / "manager" / "tool_manager.py"
PID_DIR = ROOT_DIR / "runtime" / "pids"
LOG_ROOT = ROOT_DIR / "logs"

CONFIG_CACHE: dict = {}


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        st.error(f"未找到配置文件: {CONFIG_PATH}")
        st.stop()

    with open(CONFIG_PATH, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if LOCAL_CONFIG_PATH.exists():
        with open(LOCAL_CONFIG_PATH, encoding="utf-8") as f:
            local_data = yaml.safe_load(f) or {}
        local_tools = {
            str(tool.get("name")): tool
            for tool in local_data.get("tools", [])
            if isinstance(tool, dict) and tool.get("name")
        }
        data = dict(data)
        data.update({key: value for key, value in local_data.items() if key != "tools"})
        data["tools"] = [
            {**tool, **local_tools.get(str(tool.get("name")), {})}
            for tool in data.get("tools", [])
        ]
    return data


def load_tools() -> list[dict]:
    data = CONFIG_CACHE or load_config()
    return data.get("tools", [])


def normalize_probe_host(host: str) -> str:
    host = (host or "127.0.0.1").strip()
    if host in {socket.INADDR_ANY, "::", ""}:
        return "127.0.0.1"
    return host


def _parse_port(value) -> int:
    try:
        port = int(value or 0)
    except (TypeError, ValueError):
        return 0
    return port if port > 0 else 0


def is_port_in_use(host: str, port: int) -> bool:
    port = _parse_port(port)
    if not port:
        return False
    host = normalize_probe_host(host)
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.15)
            return s.connect_ex((host, port)) == 0
    except OSError:
        return False


def normalize_path_str(p: str) -> str:
    return str(Path(p)).replace("\\", "/").lower()


def pid_file(name: str) -> Path:
    return PID_DIR / f"{name}.pid"


def read_pid(name: str) -> int | None:
    p = pid_file(name)
    if not p.exists():
        return None
    try:
        return int(p.read_text(encoding="utf-8").strip())
    except Exception:
        return None


def format_seconds(seconds: float) -> str:
    seconds = int(max(seconds, 0))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h > 0:
        return f"{h}小时{m}分{s}秒"
    if m > 0:
        return f"{m}分{s}秒"
    return f"{s}秒"


def get_request_host() -> str | None:
    context = getattr(st, "context", None)
    headers = getattr(context, "headers", None)
    if not headers:
        return None

    forwarded_host = headers.get("X-Forwarded-Host") or headers.get("x-forwarded-host")
    host = forwarded_host or headers.get("Host") or headers.get("host")
    if not host:
        return None
    return str(host).split(",")[0].strip()


def get_request_scheme() -> str:
    context = getattr(st, "context", None)
    headers = getattr(context, "headers", None)
    if not headers:
        return "http"

    forwarded_proto = headers.get("X-Forwarded-Proto") or headers.get(
        "x-forwarded-proto"
    )
    if forwarded_proto:
        return str(forwarded_proto).split(",")[0].strip()
    return "http"


def build_tool_target(tool: dict) -> dict | None:
    port = _parse_port(tool.get("port", 0))
    if port <= 0:
        return None

    public_url = str(tool.get("public_url", "") or "").strip()
    if public_url:
        href = public_url
        label = public_url
    else:
        public_base_url = str(CONFIG_CACHE.get("public_base_url", "") or "").strip()
        if public_base_url:
            href = f"{public_base_url.rstrip('/')}:{port}"
            label = href
        else:
            request_host = get_request_host()
            request_scheme = get_request_scheme()
            if request_host:
                hostname = request_host.split(":")[0]
                href = f"{request_scheme}://{hostname}:{port}"
                label = f"{hostname}:{port}"
            else:
                href = f"http://localhost:{port}"
                label = f"localhost:{port}"

    return {
        "label": label,
        "href": href,
    }


def safe_read_log(path: str, max_lines: int = 100) -> str:
    try:
        log_root = LOG_ROOT.resolve()
        log_path = Path(path).expanduser().resolve()
        if log_path == log_root or log_root not in log_path.parents:
            return "日志路径不受支持"
        if not log_path.exists():
            return "日志文件不存在"
        with open(log_path, encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
        return "".join(lines[-max_lines:])
    except (OSError, ValueError) as exc:
        return f"读取日志失败: {exc}"


def run_manager(action: str, name: str) -> str:
    if action not in {"start", "stop", "restart", "status"}:
        return f"不支持的动作: {action}"
    try:
        # The manager path and action set are fixed; shell execution is disabled.
        result = subprocess.run(  # noqa: S603
            [sys.executable, str(MANAGER_PATH), action, name],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
        )
        output = (result.stdout or "") + (result.stderr or "")
        return output if output.strip() else f"{action} {name} 已执行，但没有返回输出"
    except Exception as e:
        return f"执行失败: {e}"


@st.cache_data(ttl=2, show_spinner=False)
def scan_all_processes() -> list[dict]:
    procs = []
    for proc in psutil.process_iter(
        ["pid", "name", "cmdline", "create_time", "memory_info"]
    ):
        try:
            cmdline_list = proc.info.get("cmdline") or []
            cmdline = " ".join(cmdline_list).replace("\\", "/").lower()
            mem = proc.info.get("memory_info")
            rss_mb = round(mem.rss / 1024 / 1024, 1) if mem else None
            procs.append(
                {
                    "pid": proc.info.get("pid"),
                    "cmdline": cmdline,
                    "create_time": proc.info.get("create_time", 0),
                    "rss_mb": rss_mb,
                }
            )
        except psutil.Error:
            continue
    return procs


def process_by_pid(pid: int, all_procs: list[dict]) -> dict | None:
    for proc in all_procs:
        if proc.get("pid") == pid:
            return proc
    return None


def script_match_score(proc_cmdline: str, tool: dict) -> int:
    score = 0
    script = str(tool.get("script", "")).lower()
    workdir = normalize_path_str(tool.get("workdir", ""))
    name = str(tool.get("name", "")).lower()
    port = str(tool.get("port", ""))

    if script and script in proc_cmdline:
        score += 5
    if workdir and workdir in proc_cmdline:
        score += 3
    if name and name in proc_cmdline:
        score += 1
    if port and port != "0" and (
        f"--server.port {port}" in proc_cmdline
        or f"--port {port}" in proc_cmdline
        or f" {port}" in proc_cmdline
    ):
        score += 1
    return score


def find_best_process_from_cache(tool: dict, all_procs: list[dict]) -> dict | None:
    candidates = []
    for proc in all_procs:
        score = script_match_score(proc["cmdline"], tool)
        if score > 0:
            candidates.append((score, proc))

    if not candidates:
        return None

    candidates.sort(key=lambda x: (x[0], x[1].get("create_time", 0)), reverse=True)
    return candidates[0][1]


def build_tool_status(tool: dict, all_procs: list[dict]) -> dict:
    host = tool.get("host", "127.0.0.1")
    port = _parse_port(tool.get("port", 0))

    port_running = is_port_in_use(host, port) if port > 0 else False
    pid_from_file = read_pid(tool.get("name", ""))
    proc = process_by_pid(pid_from_file, all_procs) if pid_from_file else None
    if proc is None:
        proc = find_best_process_from_cache(tool, all_procs)
    proc_running = proc is not None

    if port > 0:
        if port_running and proc_running:
            state, state_icon, state_text = "running", "🟢", "运行中"
        elif port_running or proc_running:
            state, state_icon, state_text = "warning", "🟡", "疑似异常"
        else:
            state, state_icon, state_text = "stopped", "🔴", "未运行"
    else:
        if proc_running:
            state, state_icon, state_text = "running", "🟢", "运行中"
        else:
            state, state_icon, state_text = "stopped", "🔴", "未运行"

    pid = "-"
    uptime = "-"
    cpu_percent = "-"
    memory_mb = "-"
    start_time_str = "-"

    if proc_running:
        try:
            pid = proc.get("pid") or "-"
            create_time = proc.get("create_time", 0) or 0
            if create_time:
                uptime = format_seconds(time.time() - create_time)
                start_time_str = datetime.fromtimestamp(create_time).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            rss_mb = proc.get("rss_mb")
            if rss_mb is not None:
                memory_mb = f"{rss_mb:.1f} MB"
        except (AttributeError, TypeError, ValueError):
            pid = "-"
            uptime = "-"
            memory_mb = "-"
            start_time_str = "-"

    return {
        "state": state,
        "state_icon": state_icon,
        "state_text": state_text,
        "pid": pid,
        "uptime": uptime,
        "cpu_percent": cpu_percent,
        "memory_mb": memory_mb,
        "start_time_str": start_time_str,
    }


st.markdown(
    """
<style>
.block-container {
    padding-top: 1.1rem;
    padding-bottom: 1.5rem;
    max-width: 1600px;
}
.tool-card-link {
    display: block;
    text-decoration: none;
    color: inherit;
    margin-bottom: 0.75rem;
}
.tool-card {
    border: 1px solid #e8e8e8;
    border-radius: 16px;
    background: linear-gradient(180deg, #ffffff 0%, #fbfbfb 100%);
    padding: 16px;
    transition: all 0.18s ease;
    box-shadow: 0 1px 4px rgba(0, 0, 0, 0.04);
}
.tool-card-link:hover .tool-card {
    border-color: #9ec5fe;
    box-shadow: 0 8px 24px rgba(31, 111, 235, 0.12);
    transform: translateY(-1px);
}
.tool-card-disabled {
    border: 1px solid #ececec;
    border-radius: 16px;
    background: #fafafa;
    padding: 16px;
    margin-bottom: 0.75rem;
}
.tool-card-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    margin-bottom: 10px;
}
.tool-card-title {
    font-size: 20px;
    font-weight: 700;
    line-height: 1.2;
}
.tool-card-subtitle {
    color: #666;
    font-size: 13px;
}
.tool-card-badge {
    white-space: nowrap;
    padding: 4px 10px;
    border-radius: 999px;
    background: #eef5ff;
    color: #1d4ed8;
    font-size: 12px;
    font-weight: 600;
}
.tool-card-grid {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 8px 14px;
    font-size: 13px;
}
.tool-card-row {
    color: #333;
}
.tool-card-row strong {
    color: #111;
}
div.stButton > button {
    width: 100%;
    height: 38px;
    border-radius: 10px;
    border: 1px solid #d8d8d8;
    background: #ffffff;
    font-size: 15px;
}
div.stButton > button:hover {
    border-color: #aaaaaa;
    background: #fafafa;
}
[data-testid="stMetric"] {
    background: #fafafa;
    border: 1px solid #ededed;
    border-radius: 14px;
    padding: 10px 14px;
}
</style>
""",
    unsafe_allow_html=True,
)

st.title("🛠 工具管理台")

if "selected_log" not in st.session_state:
    st.session_state.selected_log = None
if "last_output" not in st.session_state:
    st.session_state.last_output = ""
if "last_action_time" not in st.session_state:
    st.session_state.last_action_time = ""

CONFIG_CACHE = load_config()
tools = load_tools()

top1, top2, top3, top4 = st.columns([1.1, 1, 1, 1.4])
with top1:
    auto_refresh = st.toggle("自动刷新", value=False)
with top2:
    refresh_seconds = st.selectbox("刷新间隔", [3, 5, 10, 15, 30], index=1)
with top3:
    only_enabled = st.toggle("仅显示启用", value=True)
with top4:
    keyword = st.text_input(
        "搜索工具", value="", placeholder="输入 name / title / script / group"
    )

if auto_refresh:
    time.sleep(refresh_seconds)
    st.rerun()

filtered_tools = []
for tool in tools:
    if only_enabled and not tool.get("enable", True):
        continue

    kw = keyword.strip().lower()
    if kw:
        text = " ".join(
            [
                str(tool.get("name", "")),
                str(tool.get("title", "")),
                str(tool.get("script", "")),
                str(tool.get("group", "")),
                str(tool.get("type", "")),
            ]
        ).lower()
        if kw not in text:
            continue

    filtered_tools.append(tool)

with st.spinner("正在加载工具状态..."):
    all_procs = scan_all_processes()
    statuses = [build_tool_status(tool, all_procs) for tool in filtered_tools]

running_count = sum(1 for s in statuses if s["state"] == "running")
warning_count = sum(1 for s in statuses if s["state"] == "warning")
stopped_count = sum(1 for s in statuses if s["state"] == "stopped")

m1, m2, m3, m4 = st.columns(4)
m1.metric("工具总数", len(filtered_tools))
m2.metric("运行中", running_count)
m3.metric("疑似异常", warning_count)
m4.metric("未运行", stopped_count)

st.divider()

grouped = defaultdict(list)
for tool, status in zip(filtered_tools, statuses, strict=False):
    grouped[tool.get("group", "default")].append((tool, status))

for group_name in sorted(grouped.keys()):
    items = grouped[group_name]
    group_running = sum(1 for _, s in items if s["state"] == "running")
    group_warning = sum(1 for _, s in items if s["state"] == "warning")
    group_stopped = sum(1 for _, s in items if s["state"] == "stopped")

    with st.expander(
        f"{group_name}（共 {len(items)} 个，运行中 {group_running} / 异常 {group_warning} / 未运行 {group_stopped}）",
        expanded=True,
    ):
        cols = st.columns(3)
        for idx, (tool, status) in enumerate(items):
            with cols[idx % 3]:
                target = build_tool_target(tool)
                port_text = (
                    tool.get("port", 0) if int(tool.get("port", 0) or 0) > 0 else "N/A"
                )
                card_html = f"""
<div class="tool-card-header">
  <div>
    <div class="tool-card-title">{status["state_icon"]} {tool.get("name", "")}</div>
    <div class="tool-card-subtitle">{tool.get("title", "")}</div>
  </div>
  <div class="tool-card-badge">{target["label"] if target else "No URL"}</div>
</div>
<div class="tool-card-grid">
  <div class="tool-card-row"><strong>Status:</strong> {status["state_text"]}</div>
  <div class="tool-card-row"><strong>Port:</strong> {port_text}</div>
  <div class="tool-card-row"><strong>Type:</strong> {tool.get("type", "python")}</div>
  <div class="tool-card-row"><strong>PID:</strong> {status["pid"]}</div>
  <div class="tool-card-row"><strong>Uptime:</strong> {status["uptime"]}</div>
  <div class="tool-card-row"><strong>CPU:</strong> {status["cpu_percent"]}</div>
  <div class="tool-card-row"><strong>Memory:</strong> {status["memory_mb"]}</div>
  <div class="tool-card-row"><strong>Started:</strong> {status["start_time_str"]}</div>
  <div class="tool-card-row"><strong>Script:</strong> {tool.get("script", "")}</div>
  <div class="tool-card-row"><strong>Log:</strong> {tool.get("log", "")}</div>
</div>
"""
                if target:
                    st.markdown(
                        f'<a class="tool-card-link" href="{target["href"]}" target="_blank"><div class="tool-card">{card_html}</div></a>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown(
                        f'<div class="tool-card-disabled">{card_html}</div>',
                        unsafe_allow_html=True,
                    )

                b1, b2, b3, b4 = st.columns(4)
                if b1.button("启动", key=f"start_{tool['name']}"):
                    st.session_state.last_output = run_manager("start", tool["name"])
                    st.session_state.last_action_time = datetime.now().strftime(
                        "%H:%M:%S"
                    )
                    scan_all_processes.clear()
                    st.rerun()

                if b2.button("停止", key=f"stop_{tool['name']}"):
                    st.session_state.last_output = run_manager("stop", tool["name"])
                    st.session_state.last_action_time = datetime.now().strftime(
                        "%H:%M:%S"
                    )
                    scan_all_processes.clear()
                    st.rerun()

                if b3.button("重启", key=f"restart_{tool['name']}"):
                    st.session_state.last_output = run_manager("restart", tool["name"])
                    st.session_state.last_action_time = datetime.now().strftime(
                        "%H:%M:%S"
                    )
                    scan_all_processes.clear()
                    st.rerun()

                if b4.button("日志", key=f"log_{tool['name']}"):
                    st.session_state.selected_log = tool.get("log", "")
                    st.rerun()

                st.markdown("---")

st.divider()
left, right = st.columns([1, 1])

with left:
    st.subheader("执行结果")
    if st.session_state.last_action_time:
        st.caption(f"最近一次操作时间：{st.session_state.last_action_time}")
    st.code(st.session_state.last_output or "暂无执行结果")

with right:
    st.subheader("日志预览")
    if st.session_state.selected_log:
        st.caption(st.session_state.selected_log)
        st.code(safe_read_log(st.session_state.selected_log, max_lines=100))
    else:
        st.code("尚未选择日志")

st.divider()
st.subheader("工具清单")

table_rows = []
for tool, status in zip(filtered_tools, statuses, strict=False):
    table_rows.append(
        {
            "group": tool.get("group", "default"),
            "name": tool.get("name", ""),
            "title": tool.get("title", ""),
            "type": tool.get("type", "python"),
            "state": f"{status['state_icon']} {status['state_text']}",
            "port": str(tool.get("port", 0)),
            "pid": str(status["pid"]),
            "uptime": status["uptime"],
            "cpu": status["cpu_percent"],
            "memory": status["memory_mb"],
            "script": tool.get("script", ""),
            "workdir": tool.get("workdir", ""),
            "log": tool.get("log", ""),
        }
    )

st.dataframe(table_rows, use_container_width=True, hide_index=True)
