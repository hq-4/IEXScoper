from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class EdgarFullTextConfig:
    template_path: Path
    output_root: Path
    endpoint: str
    symbols: tuple[str, ...]
    user_agent: str
    forms: tuple[str, ...]
    use_form_filter: bool
    event_terms: tuple[str, ...]
    size: int
    max_symbols: int | None
    timeout_seconds: float
    sleep_seconds: float
    retries: int
