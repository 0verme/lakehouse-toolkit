from __future__ import annotations

import logging
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_LOG_DIR = _ROOT / "logs"
_FMT = "%(asctime)s [%(levelname)-5s] [%(name)s] %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"

_file_handlers: dict[str, TimedRotatingFileHandler] = {}


def _get_file_handler(log_file: str) -> TimedRotatingFileHandler:
    if log_file not in _file_handlers:
        _LOG_DIR.mkdir(parents=True, exist_ok=True)
        h = TimedRotatingFileHandler(
            _LOG_DIR / log_file,
            when="midnight",
            backupCount=30,
            encoding="utf-8",
        )
        h.suffix = "%Y-%m-%d"
        h.setFormatter(logging.Formatter(_FMT, datefmt=_DATE_FMT))
        _file_handlers[log_file] = h
    return _file_handlers[log_file]


def get_logger(name: str, log_file: str = "app.log") -> logging.Logger:
    """返回一个已配置好的 logger。

    同一 name 多次调用返回同一实例（Python logging 标准行为）。
    log_file 决定落盘的文件名，默认写到 logs/app.log，按天滚动保留 30 天。

    用法::

        from shared.log import get_logger
        logger = get_logger(__name__)
        logger.info("任务开始")
        logger.error("出错了", exc_info=True)
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False

    logger.addHandler(_get_file_handler(log_file))

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(logging.Formatter(_FMT, datefmt=_DATE_FMT))
    logger.addHandler(stdout_handler)

    return logger
