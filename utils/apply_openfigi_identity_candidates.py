from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging

DEFAULT_STAGE_ROOT = Path("data/resolution/staged")
DEFAULT_FACT_ROOT = Path("data/resolution")
DEFAULT_REPORT = Path("reports/openfigi-identity/apply_summary.json")
FACT_FILES = ("identity_facts.jsonl", "event_facts.jsonl")


def main() -> int:
    args = parse_args()
    stage_dir = Path(args.stage) if args.stage else _latest_stage(Path(args.stage_root))
    setup_logging("logs/app.jsonl")
    logger = get_logger(__name__)
    summary = apply_stage(stage_dir, Path(args.fact_root), apply=args.apply)
    summary["stage_dir"] = str(stage_dir)
    summary["applied"] = args.apply
    Path(args.report).write_text(json.dumps(summary, indent=2, sort_keys=True))
    logger.info("OpenFIGI apply pass", extra={"event": "openfigi_apply", "detail": summary})
    return 0


def apply_stage(stage_dir: Path, fact_root: Path, apply: bool) -> dict[str, Any]:
    existing_ids, existing_eras = _load_canonical(fact_root)
    summary: dict[str, Any] = {"by_file": {}}
    for name in FACT_FILES:
        stage_facts = _read_jsonl(stage_dir / name)
        new_facts = [
            fact
            for fact in stage_facts
            if fact["fact_id"] not in existing_ids and fact["symbol_era_id"] not in existing_eras
        ]
        states = Counter(str(fact.get("verification_state")) for fact in new_facts)
        summary["by_file"][name] = {
            "staged": len(stage_facts),
            "new": len(new_facts),
            "skipped_duplicate": len(stage_facts) - len(new_facts),
            "by_verification_state": dict(sorted(states.items())),
        }
        if apply and new_facts:
            with (fact_root / name).open("a", encoding="utf-8") as handle:
                for fact in new_facts:
                    handle.write(json.dumps(fact, sort_keys=True) + "\n")
    return summary


def _load_canonical(fact_root: Path) -> tuple[set[str], set[str]]:
    fact_ids: set[str] = set()
    verified_eras: set[str] = set()
    for name in FACT_FILES:
        for fact in _read_jsonl(fact_root / name):
            fact_ids.add(fact["fact_id"])
            if fact.get("verification_state") == "verified":
                verified_eras.add(fact["symbol_era_id"])
    return fact_ids, verified_eras


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _latest_stage(stage_root: Path) -> Path:
    stages = sorted(
        (path for path in stage_root.iterdir() if (path / "stage_manifest.json").exists()),
        key=lambda path: (path / "stage_manifest.json").stat().st_mtime,
    )
    binding = [
        p
        for p in stages
        if "openfigi_era_binding"
        in json.loads((p / "stage_manifest.json").read_text()).get("stopping_reason", "")
    ]
    if not binding:
        raise ValueError("no openfigi era-binding stage found")
    return binding[-1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply staged OpenFIGI facts into the canonical store."
    )
    parser.add_argument("--stage", help="stage dir; default: latest openfigi binding stage")
    parser.add_argument("--stage-root", default=str(DEFAULT_STAGE_ROOT))
    parser.add_argument("--fact-root", default=str(DEFAULT_FACT_ROOT))
    parser.add_argument("--report", default=str(DEFAULT_REPORT))
    parser.add_argument(
        "--apply", action="store_true", help="write to canonical store (default: dry run)"
    )
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
