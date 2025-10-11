from pathlib import Path
from typing import Iterable

def update_year_manifest(path: str, files: Iterable[dict]) -> str:
    Path(Path(path).parent).mkdir(parents=True, exist_ok=True)
    return path
