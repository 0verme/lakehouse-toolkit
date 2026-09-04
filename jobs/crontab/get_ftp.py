"""从显式配置的 FTPS 下载运行时输入文件。"""

from __future__ import annotations

import os
from ftplib import FTP_TLS
from pathlib import Path

from shared.config.env import required_env

FTP_HOST = os.getenv("PYTOOLS_FTP_HOST", "ftp.example.invalid")


def get_ftp_port() -> int:
    try:
        port = int(os.getenv("PYTOOLS_FTP_PORT", "21"))
    except (TypeError, ValueError) as exc:
        raise ValueError("PYTOOLS_FTP_PORT must be an integer") from exc
    if not 1 <= port <= 65535:
        raise ValueError("PYTOOLS_FTP_PORT must be between 1 and 65535")
    return port


REMOTE_FILES = {
    "workspace_catalog": os.getenv(
        "PYTOOLS_FTP_WORKSPACE_CATALOG", "/demo/workspace_catalog.xlsx"
    ),
    "job_bundle": os.getenv("PYTOOLS_FTP_JOB_BUNDLE", "/demo/job_bundle.zip"),
}
LOCAL_FILES = {
    "workspace_catalog": Path(
        os.getenv(
            "PYTOOLS_FTP_WORKSPACE_CATALOG_OUTPUT",
            "runtime/input/workspace_catalog.xlsx",
        )
    ),
    "job_bundle": Path(
        os.getenv("PYTOOLS_FTP_JOB_BUNDLE_OUTPUT", "runtime/input/job_bundle.zip")
    ),
}


def download_files() -> None:
    username = required_env("PYTOOLS_FTP_USER")
    password = required_env("PYTOOLS_FTP_PASSWORD")
    for output in LOCAL_FILES.values():
        output.parent.mkdir(parents=True, exist_ok=True)

    # FTP_TLS encrypts both authentication and the data channel.
    with FTP_TLS() as ftp:  # noqa: S321
        ftp.connect(FTP_HOST, get_ftp_port(), timeout=30)
        ftp.login(username, password)
        ftp.prot_p()
        for name, remote_path in REMOTE_FILES.items():
            with LOCAL_FILES[name].open("wb") as local_file:
                ftp.retrbinary(f"RETR {remote_path}", local_file.write)


if __name__ == "__main__":
    download_files()
    print("FTP files downloaded")
