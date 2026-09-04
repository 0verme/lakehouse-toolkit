# !/bin/python
import hashlib

from pywebio.input import TEXT, input
from pywebio.output import put_code

from shared.ui.pywebio_helper import put_black_text, start_pywebio_app


def build_sha256():
    path = input("请输入文件路径或 URL", type=TEXT).strip()
    parts = [part for part in path.split("/") if part]
    if len(parts) >= 2:
        source = parts[-2] + parts[-1]
    elif parts:
        source = parts[-1]
    else:
        source = ""

    sha256_value = hashlib.sha256(source.encode("utf-8")).hexdigest()

    put_black_text("SHA-256 结果")
    put_code(f"""输入: {path}
拼接字符串: {source}
SHA-256: {sha256_value}
""")


if __name__ == "__main__":
    start_pywebio_app("Lakehouse Toolkit", build_sha256)
