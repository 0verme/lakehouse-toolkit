# !/bin/python
import base64

from pywebio.input import TEXT, input
from pywebio.output import put_text

from shared.ui.pywebio_helper import put_black_text, start_pywebio_app


def encode_secret():
    secret = input("请输入需要加密的内容", type=TEXT).strip()
    encoded = base64.b64encode(base64.b64encode(secret.encode("utf-8"))).decode("utf-8")

    put_black_text("加密结果")
    put_text(f"原文: {secret}")
    put_text(f"密文: {encoded}")


if __name__ == "__main__":
    start_pywebio_app("Lakehouse Toolkit", encode_secret)
