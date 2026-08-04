from __future__ import annotations

import json

from utils.apply_openfigi_identity_candidates import apply_stage


def _write_jsonl(path, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(r, sort_keys=True) + "\n" for r in rows))


def _fact(fid: str, era: str, state: str) -> dict:
    return {
        "fact_id": fid,
        "symbol_era_id": era,
        "symbol": era.split("#")[0],
        "verification_state": state,
    }


def test_apply_stage_dry_run_and_apply(tmp_path) -> None:
    stage = tmp_path / "staged" / "s1"
    _write_jsonl(
        stage / "identity_facts.jsonl",
        [
            _fact("identity:a", "AAA#001", "corroborated"),
            _fact("identity:b", "BBB#001", "openfigi_asserted"),
        ],
    )
    _write_jsonl(stage / "event_facts.jsonl", [_fact("event:a", "AAA#001", "event_candidate")])
    root = tmp_path / "canonical"
    _write_jsonl(root / "identity_facts.jsonl", [_fact("identity:x", "CCC#001", "verified")])
    _write_jsonl(root / "event_facts.jsonl", [])

    dry = apply_stage(stage, root, apply=False)
    assert dry["by_file"]["identity_facts.jsonl"]["new"] == 2
    assert len((root / "identity_facts.jsonl").read_text().splitlines()) == 1  # untouched

    applied = apply_stage(stage, root, apply=True)
    assert applied["by_file"]["identity_facts.jsonl"]["by_verification_state"] == {
        "corroborated": 1,
        "openfigi_asserted": 1,
    }
    assert len((root / "identity_facts.jsonl").read_text().splitlines()) == 3

    rerun = apply_stage(stage, root, apply=True)  # idempotent
    assert rerun["by_file"]["identity_facts.jsonl"]["new"] == 0
    assert len((root / "identity_facts.jsonl").read_text().splitlines()) == 3


def test_apply_stage_skips_eras_with_verified_identity(tmp_path) -> None:
    stage = tmp_path / "staged" / "s1"
    _write_jsonl(stage / "identity_facts.jsonl", [_fact("identity:a", "CCC#001", "corroborated")])
    _write_jsonl(stage / "event_facts.jsonl", [])
    root = tmp_path / "canonical"
    _write_jsonl(root / "identity_facts.jsonl", [_fact("identity:x", "CCC#001", "verified")])
    _write_jsonl(root / "event_facts.jsonl", [])
    result = apply_stage(stage, root, apply=True)
    assert result["by_file"]["identity_facts.jsonl"]["new"] == 0
