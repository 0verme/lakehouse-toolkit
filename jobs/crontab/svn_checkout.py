"""共享的 SVN checkout 启动辅助。"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from shared.config.env import required_env


def checkout(
    *,
    url_env: str,
    directory_env: str,
    default_url: str,
    default_directory: str,
    username_env: str,
    password_env: str,
) -> None:
    url = os.getenv(url_env, default_url).strip()
    directory = Path(os.getenv(directory_env, default_directory)).expanduser()
    if not url:
        raise RuntimeError(f"Missing required environment variable {url_env}")
    username = required_env(username_env)
    password = required_env(password_env)
    directory.mkdir(parents=True, exist_ok=True)
    command = [
        os.getenv("PYTOOLS_SVN_BIN", "svn"),
        "checkout",
        url,
        str(directory),
        "--username",
        username,
        "--password",
        password,
        "--non-interactive",
        "--trust-server-cert-failures",
        "unknown-ca,cn-mismatch,expired,not-yet-valid,other",
    ]
    # SVN arguments are passed as a list; shell execution is disabled.
    subprocess.run(command, check=True, shell=False)  # noqa: S603
