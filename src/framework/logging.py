import json
import logging
import os
import time
from logging.handlers import RotatingFileHandler
from rich.logging import RichHandler

class JsonlFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(record.created)) + f".{int(record.msecs):03d}"
        payload = {
            "ts": ts,
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
        }
        for key in ("subsys", "guild_id", "user_id", "msg_id", "event", "detail", "year", "day", "symbol"):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        return json.dumps(payload, ensure_ascii=False)

def enforce_dual_sinks(logger: logging.Logger) -> None:
    names = [h.get_name() for h in logger.handlers]
    if len(logger.handlers) != 2 or sorted(names) != ["jsonl_handler", "pretty_handler"]:
        raise RuntimeError("Logging misconfigured: require exactly pretty_handler and jsonl_handler.")

def setup_logging(jsonl_path: str) -> None:
    root = logging.getLogger()
    if getattr(root, "_iexscoper_logging_configured", False):
        enforce_dual_sinks(root)
        return
    root.setLevel(logging.INFO)
    pretty = RichHandler(rich_tracebacks=True, show_time=True, show_path=False, markup=True)
    pretty.set_name("pretty_handler")
    pretty.setLevel(logging.INFO)
    path = jsonl_path or "logs/app.jsonl"
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    file_handler = RotatingFileHandler(path, maxBytes=10_000_000, backupCount=5, encoding="utf-8")
    file_handler.set_name("jsonl_handler")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(JsonlFormatter())
    root.handlers = [pretty, file_handler]
    root._iexscoper_logging_configured = True
    enforce_dual_sinks(root)

def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
