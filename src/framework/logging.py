from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Any

from rich.console import Console
from rich.logging import RichHandler
from rich.text import Text

HANDLER_NAMES = ["jsonl_handler", "pretty_handler"]
JSONL_KEYS = (
    "ts",
    "level",
    "name",
    "subsys",
    "guild_id",
    "user_id",
    "msg_id",
    "event",
    "detail",
    "message",
)
ICONS = {
    logging.DEBUG: "ℹ",
    logging.INFO: "✔",
    logging.WARNING: "⚠",
    logging.ERROR: "✖",
    logging.CRITICAL: "✖",
}
MAX_FIELD_CHARS = 4_000


class JsonlFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(record.created))
            + f".{int(record.msecs):03d}",
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
        }
        for key in JSONL_KEYS:
            if hasattr(record, key):
                payload[key] = _bounded(getattr(record, key))
        return json.dumps({key: payload.get(key) for key in JSONL_KEYS}, ensure_ascii=False)


class IconFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.icon = ICONS.get(record.levelno, "•")
        return True


def enforce_dual_sinks(logger: logging.Logger) -> None:
    names = sorted(handler.get_name() for handler in logger.handlers)
    if len(logger.handlers) != 2 or names != HANDLER_NAMES:
        raise RuntimeError(f"Logging misconfigured: expected exactly {HANDLER_NAMES}, got {names}.")


def setup_logging(jsonl_path: str = "logs/app.jsonl", level: int = logging.INFO) -> None:
    root = logging.getLogger()
    if getattr(root, "_iexscoper_logging_configured", False):
        enforce_dual_sinks(root)
        return
    root.setLevel(level)
    root.handlers = [_pretty_handler(level), _jsonl_handler(jsonl_path, level)]
    root._iexscoper_logging_configured = True  # type: ignore[attr-defined]
    enforce_dual_sinks(root)


def _pretty_handler(level: int) -> RichHandler:
    pretty = RichHandler(
        console=Console(stderr=True, soft_wrap=False),
        rich_tracebacks=True,
        tracebacks_show_locals=level <= logging.DEBUG,
        show_time=True,
        show_path=False,
        markup=True,
        log_time_format=_rich_time,
    )
    pretty.set_name("pretty_handler")
    pretty.setLevel(level)
    pretty.addFilter(IconFilter())
    pretty.setFormatter(logging.Formatter("%(icon)s %(message)s"))
    return pretty


def _jsonl_handler(jsonl_path: str, level: int) -> RotatingFileHandler:
    path = jsonl_path or "logs/app.jsonl"
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    jsonl = RotatingFileHandler(
        path,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    jsonl.set_name("jsonl_handler")
    jsonl.setLevel(level)
    jsonl.setFormatter(JsonlFormatter())
    return jsonl


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def _bounded(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _bounded(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_bounded(item) for item in value]
    if not isinstance(value, str) or len(value) <= MAX_FIELD_CHARS:
        return value
    removed = len(value) - MAX_FIELD_CHARS
    return f"{value[:MAX_FIELD_CHARS]}…(+{removed})"


def _rich_time(current: datetime) -> Text:
    return Text(current.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])
