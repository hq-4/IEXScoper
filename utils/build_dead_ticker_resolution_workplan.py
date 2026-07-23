from __future__ import annotations

import argparse
from pathlib import Path

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.framework.logging import get_logger, setup_logging
from utils.dead_ticker_resolution_workplan import (
    DEFAULT_LANES_PATH,
    DEFAULT_AUTOMATION_PATH,
    DEFAULT_SEC_EVIDENCE_PATH,
    DEFAULT_WORKPLAN_ROOT,
    ResolutionWorkplanConfig,
    build_resolution_workplan,
)
from utils.sec_document_graph_scoring import DEFAULT_SEC_DOC_TOP_N


def main() -> int:
    args = parse_args()
    evidence_path = Path(args.sec_evidence_path) if args.sec_evidence_path else None
    config = ResolutionWorkplanConfig(
        lanes_path=Path(args.lanes_path),
        output_root=Path(args.output_root),
        sec_evidence_path=evidence_path,
        sec_doc_top_n=args.sec_doc_top_n,
        automation_path=Path(args.automation_path) if args.automation_path else None,
    )
    setup_logging(str(config.output_root / "resolution_workplan.jsonl"))
    result = build_resolution_workplan(config)
    get_logger(__name__).info(
        "dead ticker resolution workplan complete",
        extra={"event": "dead_ticker_resolution_workplan_complete", "detail": result["summary"]},
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lanes-path", default=str(DEFAULT_LANES_PATH))
    parser.add_argument("--output-root", default=str(DEFAULT_WORKPLAN_ROOT))
    parser.add_argument("--sec-evidence-path", default=str(DEFAULT_SEC_EVIDENCE_PATH))
    parser.add_argument("--sec-doc-top-n", type=int, default=DEFAULT_SEC_DOC_TOP_N)
    parser.add_argument("--automation-path", default=str(DEFAULT_AUTOMATION_PATH))
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
