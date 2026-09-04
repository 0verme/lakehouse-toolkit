# !/bin/python
from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from functools import lru_cache, wraps
from html import escape
from pathlib import Path

import pywebio
import yaml
from pywebio import config
from pywebio.input import TEXT, textarea
from pywebio.output import put_html, put_markdown, put_text

ROOT_DIR = Path(__file__).resolve().parents[2]
TOOLS_CONFIG_PATH = ROOT_DIR / "configs" / "tools.yaml"
LOCAL_TOOLS_CONFIG_PATH = ROOT_DIR / "configs" / "tools.local.yaml"
CCS_PATH = ROOT_DIR / "resources" / "static" / "pywebio-premium.css"


@lru_cache(maxsize=1)
def load_premium_css() -> str:
    try:
        return CCS_PATH.read_text(encoding="utf-8")
    except OSError:
        return ""


def normalize_rel_path(path_str: str) -> str:
    return str(Path(path_str)).replace("\\", "/").strip("/").lower()


@lru_cache(maxsize=1)
def load_tools_config() -> list[dict]:
    if not TOOLS_CONFIG_PATH.exists():
        return []
    with open(TOOLS_CONFIG_PATH, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    local_tools = {}
    if LOCAL_TOOLS_CONFIG_PATH.exists():
        with open(LOCAL_TOOLS_CONFIG_PATH, encoding="utf-8") as f:
            local_data = yaml.safe_load(f) or {}
        local_tools = {
            str(tool.get("name")): tool
            for tool in local_data.get("tools", [])
            if isinstance(tool, dict) and tool.get("name")
        }
    return [
        {**tool, **local_tools.get(str(tool.get("name")), {})}
        for tool in data.get("tools", [])
    ]


def resolve_current_tool_config() -> dict | None:
    main_module = sys.modules.get("__main__")
    main_file = getattr(main_module, "__file__", None)
    if not main_file:
        return None

    try:
        rel_main = normalize_rel_path(Path(main_file).resolve().relative_to(ROOT_DIR))
    except Exception:
        rel_main = normalize_rel_path(main_file)

    for tool in load_tools_config():
        workdir = normalize_rel_path(tool.get("workdir", ""))
        script = normalize_rel_path(tool.get("script", ""))
        candidate = "/".join(part for part in [workdir, script] if part)
        if candidate and candidate == rel_main:
            return tool
    return None


def resolve_tool_title(default_title: str) -> str:
    tool = resolve_current_tool_config()
    if tool:
        return str(tool.get("title") or default_title)
    return default_title


def resolve_registered_port(default_port: int | None = None) -> int | None:
    tool = resolve_current_tool_config()
    if tool and tool.get("port") is not None:
        try:
            port = int(tool.get("port"))
        except (TypeError, ValueError):
            port = None
        if port and port > 0:
            return port
    return default_port


def put_black_text(text: str):
    put_markdown(f"**{text}**")


def put_red_text(text: str):
    put_markdown(f'<p style="color:red;">{text}</p>')


def put_table_plus(table_data):
    headers = table_data[0]
    rows = table_data[1:]
    thead = "<tr>" + "".join(f"<th>{escape(str(h))}</th>" for h in headers) + "</tr>"
    tbody = ""
    for row in rows:
        tbody += (
            "<tr>"
            + "".join(
                f"<td>{cell if isinstance(cell, str) and '<a ' in cell else escape(str(cell))}</td>"
                for cell in row
            )
            + "</tr>"
        )

    html_content = f"""
        <style>
        .custom-table-container {{
            width: 100%;
            overflow-x: auto;
            margin: 10px 0;
        }}
        .custom-table {{
            width: 100%;
            border-collapse: collapse;
            table-layout: fixed;
            font-family: Arial, sans-serif;
            font-size: 14px;
        }}
        .custom-table th,
        .custom-table td {{
            border: 1px solid #ccc;
            padding: 10px;
            text-align: left;
            vertical-align: top;
            word-break: break-all;
            word-wrap: break-word;
            white-space: normal;
        }}
        .custom-table th {{
            background-color: #f0f0f0;
            font-weight: bold;
        }}
        </style>

        <div class="custom-table-container">
            <table class="custom-table">
                <thead>{thead}</thead>
                <tbody>{tbody}</tbody>
            </table>
        </div>
        """
    put_html(html_content)


def put_section_title(title: str):
    put_black_text(title)


def put_separator(char: str = "=", width: int = 66):
    put_text(char * width)


def iter_nonempty_lines(text: str) -> list[str]:
    return [
        line.strip().replace("\t", "")
        for line in text.splitlines()
        if line.strip().replace("\t", "")
    ]


def multiline_textarea(label: str) -> str:
    return textarea(label, type=TEXT)


def multiline_entries(label: str) -> list[str]:
    return iter_nonempty_lines(multiline_textarea(label))


def safe_put_error(exc: Exception):
    put_red_text(str(exc))


def run_for_multiline_input(label: str, handler: Callable[[str], None]):
    for item in multiline_entries(label):
        try:
            handler(item)
        except Exception as exc:
            safe_put_error(exc)


def resolve_server_args(
    default_host: str = "127.0.0.1", default_port: int | None = None
) -> tuple[str, int | None]:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--host", default=default_host)
    parser.add_argument("--port", type=int, default=default_port)
    args, _ = parser.parse_known_args()
    return args.host, args.port


_THEME_TOGGLE_JS = r"""
(function(){
  if (window.__pwioThemeToggle) return;
  window.__pwioThemeToggle = true;
  var root = document.documentElement;
  function systemDark(){
    return !!(window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches);
  }
  function current(){
    return localStorage.getItem('pwio-theme') || (systemDark() ? 'dark' : 'light');
  }
  var btn = document.createElement('button');
  btn.style.cssText = 'position:fixed;top:14px;right:14px;z-index:9999;padding:6px 12px;' +
    'border-radius:8px;border:1px solid rgba(127,127,127,.3);background:rgba(127,127,127,.12);' +
    'cursor:pointer;font-size:13px;font-weight:600;color:var(--text);backdrop-filter:blur(6px);';
  function apply(mode){
    root.classList.remove('pwio-dark', 'pwio-light');
    root.classList.add(mode === 'dark' ? 'pwio-dark' : 'pwio-light');
    btn.textContent = mode === 'dark' ? '☀️ 浅色' : '🌙 深色';
  }
  document.body.appendChild(btn);
  apply(current());
  btn.onclick = function(){
    var next = current() === 'dark' ? 'light' : 'dark';
    localStorage.setItem('pwio-theme', next);
    apply(next);
  };
})();
"""


def _with_theme_ui(app: Callable) -> Callable:
    @wraps(app)
    def wrapper(*args, **kwargs):
        from pywebio.session import run_js

        run_js(_THEME_TOGGLE_JS)
        return app(*args, **kwargs)

    return wrapper


def start_pywebio_app(
    title: str, app: Callable, port: int | None = None, host: str = "127.0.0.1"
):
    registered_port = resolve_registered_port(default_port=port)
    host, port = resolve_server_args(default_host=host, default_port=registered_port)
    if not port or int(port) <= 0:
        raise ValueError(
            "Missing valid port. Pass --port from the launcher, set a default in start_pywebio_app(), or register the tool port in configs/tools.yaml."
        )
    resolved_title = resolve_tool_title(title)
    css = load_premium_css()
    app = _with_theme_ui(app)
    if css:
        app = config(css_style=css)(app)
    pywebio.config(title=resolved_title)
    pywebio.start_server(app, port=port, host=host, cdn=False)
