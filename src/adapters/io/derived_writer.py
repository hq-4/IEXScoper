from pathlib import Path

def write_staging_parts(root: str, year: int) -> int:
    Path(root).mkdir(parents=True, exist_ok=True)
    return 0

def write_master_file(root: str, year: int, tmp_path: str) -> str:
    Path(root).mkdir(parents=True, exist_ok=True)
    return str(Path(root) / f"{year}_IEX_TOPS1.6_trades_persec.parquet")
