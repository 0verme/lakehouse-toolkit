"""双重 Base64 解码演示。

Base64 不是加密；该工具仅用于处理使用者在运行时输入的演示字符串。
"""

import base64

from pywebio.input import TEXT, input
from pywebio.output import put_text

from shared.ui.pywebio_helper import put_black_text, safe_put_error, start_pywebio_app


def decode_secret():
    encoded = input("请输入需要解码的内容", type=TEXT).strip()
    if not encoded:
        put_text("输入不能为空。")
        return

    try:
        first_pass = base64.b64decode(encoded.encode("utf-8"), validate=True)
        decoded = base64.b64decode(first_pass, validate=True).decode("utf-8")
        put_black_text("解码结果")
        put_text(f"输入: {encoded}")
        put_text(f"结果: {decoded}")
    except Exception as exc:
        safe_put_error(exc)


if __name__ == "__main__":
    start_pywebio_app("双重 Base64 解码演示", decode_secret)
