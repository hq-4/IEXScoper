import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Settings:
    iex_csv_root: str
    iex_parquet_root: str
    iex_work_root: str
    iex_report_root: str
    display_tz: str
    log_jsonl_path: str
    database_url: str | None

def get_settings() -> Settings:
    return Settings(
        iex_csv_root=os.getenv("IEX_CSV_ROOT", ""),
        iex_parquet_root=os.getenv("IEX_PARQUET_ROOT", ""),
        iex_work_root=os.getenv("IEX_WORK_ROOT", "data/iex"),
        iex_report_root=os.getenv("IEX_REPORT_ROOT", "reports"),
        display_tz=os.getenv("DISPLAY_TZ", "America/New_York"),
        log_jsonl_path=os.getenv("LOG_JSONL_PATH", "logs/app.jsonl"),
        database_url=os.getenv("DATABASE_URL"),
    )
