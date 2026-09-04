# !/bin/python
from __future__ import annotations

import codecs


def detect_encoding(file_path, encodings=("utf-8", "gbk")):
    for encoding in encodings:
        try:
            with codecs.open(file_path, "r", encoding=encoding) as f:
                f.read()
            return encoding
        except UnicodeDecodeError:
            continue
    return "utf-8"
