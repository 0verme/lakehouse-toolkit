from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import yaml

BASE_DIR = Path(__file__).resolve().parent.parent
ROOT_DIR = BASE_DIR.parents[1]
CONFIG_PATH = ROOT_DIR / "configs" / "tools.yaml"
LOCAL_CONFIG_PATH = ROOT_DIR / "configs" / "tools.local.yaml"
PID_DIR = ROOT_DIR / "runtime" / "pids"
LOG_ROOT = ROOT_DIR / "logs"
PID_DIR.mkdir(parents=True, exist_ok=True)


def _load_yaml(path: Path) -> dict:
    try:
        if not path.exists():
            return {}
        with open(path, encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except (OSError, TypeError, ValueError, yaml.YAMLError):
        return {}


def load_tools():
    data = _load_yaml(CONFIG_PATH)
    local_data = _load_yaml(LOCAL_CONFIG_PATH)
    local_tools = {
        str(tool.get("name")): tool
        for tool in local_data.get("tools", [])
        if isinstance(tool, dict) and tool.get("name")
    }
    tools = []
    for base_tool in data.get("tools", []):
        tool = dict(base_tool)
        tool.update(local_tools.get(str(tool.get("name")), {}))
        tools.append(tool)
    return tools


def get_tool(name: str):
    for tool in load_tools():
        if tool["name"] == name:
            return tool
    raise ValueError(f"工具不存在: {name}")


def resolve_path(path_str: str) -> Path:
    path = Path(path_str)
    return path if path.is_absolute() else ROOT_DIR / path


def resolve_workdir(tool: dict) -> Path:
    return resolve_path(tool["workdir"])


def resolve_log_path(tool: dict) -> Path:
    log_root = LOG_ROOT.resolve()
    log_path = resolve_path(str(tool["log"])).expanduser().resolve()
    if log_path == log_root or log_root not in log_path.parents:
        raise ValueError(f"日志路径必须位于 {log_root} 下")
    return log_path


def pid_file(name: str) -> Path:
    return PID_DIR / f"{name}.pid"


def read_pid(name: str):
    p = pid_file(name)
    if not p.exists():
        return None
    try:
        return int(p.read_text(encoding="utf-8").strip())
    except Exception:
        return None


def is_pid_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


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
            s.settimeout(0.5)
            return s.connect_ex((host, port)) == 0
    except OSError:
        return False


def read_log_tail(path: Path, max_lines: int = 40) -> str:
    try:
        if not path.exists():
            return "[log] 日志文件不存在"
        with open(path, encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
        return "".join(lines[-max_lines:]).strip() or "[log] 日志为空"
    except Exception as exc:
        return f"[log] 读取日志失败: {exc}"


def console_safe(text: object) -> str:
    value = str(text)
    encoding = sys.stdout.encoding or "utf-8"
    return value.encode(encoding, errors="replace").decode(encoding, errors="replace")


def resolve_python_bin(tool: dict) -> str:
    python_cfg = tool.get("python")
    if isinstance(python_cfg, dict):
        if sys.platform.startswith("win"):
            candidate = python_cfg.get("windows") or python_cfg.get("win")
        else:
            candidate = python_cfg.get("linux")
        python_bin = candidate or python_cfg.get("default") or sys.executable
    else:
        python_bin = python_cfg or sys.executable

    if str(python_bin).lower() == "python":
        return sys.executable
    return str(python_bin)


def build_command(tool: dict) -> list[str]:
    tool_type = tool.get("type", "python").lower()
    workdir = resolve_workdir(tool)
    script_path = str(workdir / tool["script"])
    host = tool.get("host", "127.0.0.1")
    port = _parse_port(tool.get("port", 0))
    python_bin = resolve_python_bin(tool)

    if tool_type == "streamlit":
        cmd = [
            python_bin,
            "-m",
            "streamlit",
            "run",
            script_path,
            "--server.address",
            host,
        ]
        if port > 0:
            cmd.extend(["--server.port", str(port)])
        return cmd

    cmd = [python_bin, script_path]
    if port > 0:
        cmd.extend(["--host", host, "--port", str(port)])
    return cmd


def start_tool(name: str):
    tool = get_tool(name)
    host = tool.get("host", "127.0.0.1")
    port = _parse_port(tool.get("port", 0))
    workdir = resolve_workdir(tool)
    log_path = resolve_log_path(tool)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    old_pid = read_pid(name)
    if old_pid and is_pid_running(old_pid):
        print(f"[SKIP] {name} 已在运行, pid={old_pid}")
        return

    if port > 0 and is_port_in_use(host, port):
        print(console_safe(f"[ERROR] 端口已被占用: {host}:{port}"))
        return

    cmd = build_command(tool)
    env = os.environ.copy()
    root_path = str(ROOT_DIR)
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        root_path
        if not existing_pythonpath
        else f"{root_path}{os.pathsep}{existing_pythonpath}"
    )
    print(console_safe(f"[INFO] tool={name}"))
    print(console_safe(f"[INFO] python={cmd[0]}"))
    print(console_safe(f"[INFO] cwd={workdir}"))
    print(console_safe(f"[INFO] log={log_path}"))
    print(console_safe(f"[INFO] PYTHONPATH={env['PYTHONPATH']}"))
    print(console_safe(f"[INFO] cmd={' '.join(str(x) for x in cmd)}"))

    try:
        with open(log_path, "a", encoding="utf-8") as stdout_f:
            # Commands are assembled from the validated local tool registry; no shell is used.
            process = subprocess.Popen(  # noqa: S603
                cmd,
                cwd=str(workdir),
                env=env,
                stdout=stdout_f,
                stderr=stdout_f,
                start_new_session=True,
                shell=False,
            )
    except (OSError, ValueError) as exc:
        print(console_safe(f"[ERROR] 启动失败: {exc}"))
        return

    pid_file(name).write_text(str(process.pid), encoding="utf-8")
    print(console_safe(f"[OK] 启动成功: {name}, pid={process.pid}"))
    time.sleep(1.0)

    return_code = process.poll()
    if return_code is not None:
        print(console_safe(f"[ERROR] 进程启动后立即退出: code={return_code}"))
        print(console_safe(read_log_tail(log_path)))
        pid_file(name).unlink(missing_ok=True)
        return

    if port > 0:
        if is_port_in_use(host, port):
            print(console_safe(f"[OK] 端口监听成功: {host}:{port}"))
        else:
            print(console_safe(f"[WARN] 进程仍在运行，但端口暂未监听: {host}:{port}"))


def stop_tool(name: str):
    pid = read_pid(name)
    if not pid:
        print(f"[SKIP] {name} 没有 pid 记录")
        return

    if not is_pid_running(pid):
        print(f"[SKIP] {name} 进程不存在, pid={pid}")
        pid_file(name).unlink(missing_ok=True)
        return

    try:
        os.kill(pid, signal.SIGTERM)
        print(f"[OK] 已停止: {name}, pid={pid}")
    except Exception as e:
        print(f"[ERROR] 停止失败: {name}, {e}")
        return

    pid_file(name).unlink(missing_ok=True)


def restart_tool(name: str):
    stop_tool(name)
    start_tool(name)


def status_tool(name: str):
    tool = get_tool(name)
    pid = read_pid(name)
    running = bool(pid and is_pid_running(pid))
    port = _parse_port(tool.get("port", 0))
    port_used = (
        is_port_in_use(tool.get("host", "127.0.0.1"), port) if port > 0 else False
    )

    print(
        {
            "name": tool["name"],
            "type": tool.get("type", "python"),
            "group": tool.get("group", "default"),
            "workdir": str(resolve_workdir(tool)),
            "pid": pid,
            "running": running,
            "port": port,
            "port_in_use": port_used,
            "log": str(resolve_log_path(tool)),
        }
    )


def run_action(action: str, target: str):
    if action == "start":
        start_tool(target)
    elif action == "stop":
        stop_tool(target)
    elif action == "restart":
        restart_tool(target)
    elif action == "status":
        status_tool(target)
    else:
        raise ValueError(f"不支持的动作: {action}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("用法: python tool_manager.py [start|stop|restart|status] [tool_name]")
        sys.exit(1)

    action = sys.argv[1]
    target = sys.argv[2]
    run_action(action, target)
