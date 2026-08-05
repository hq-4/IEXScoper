from __future__ import annotations

import json
from pathlib import Path

from utils.canonical_identity_join import (
    load_best_identity_facts,
    load_canonical_facts_for_review,
)

IDENTITY_FACTS = [
    {
        "symbol_era_id": "AAA#001",
        "verification_state": "verified",
        "issuer": "Alpha Corp",
        "entity_id": "123456",
        "evidence_method": "sec_date_scoped_display_names",
        "instrument": "probable_operating_company",
        "flags": [],
        "source": "https://www.sec.gov/Archives/edgar/data/123456/0001/filing.htm",
    },
    {
        "symbol_era_id": "BBB#001",
        "verification_state": "verified",
        "issuer": "Beta Legacy Co",
        "entity_id": "",
        "evidence_method": "legacy_historical_override",
        "instrument": "probable_operating_company",
        "flags": ["migrated_without_entity_id"],
        "source": "https://www.sec.gov/Archives/edgar/data/999000/0002/filing.htm",
    },
]


def _write_facts(fact_root: Path) -> None:
    fact_root.mkdir(parents=True, exist_ok=True)
    (fact_root / "identity_facts.jsonl").write_text(
        "".join(json.dumps(f) + "\n" for f in IDENTITY_FACTS)
    )
    (fact_root / "event_facts.jsonl").write_text("")


def test_load_best_identity_facts_carries_source_url(tmp_path: Path) -> None:
    fact_root = tmp_path / "facts"
    _write_facts(fact_root)

    identities = load_best_identity_facts(fact_root)
    rows = {row["symbol_era_id"]: row for row in identities.iter_rows(named=True)}

    assert rows["AAA#001"]["identity_source_url"].endswith("/data/123456/0001/filing.htm")
    assert rows["AAA#001"]["identity_entity_id"] == "123456"
    # BBB's entity_id was migrated empty, but the source URL still carries a real CIK
    # (recoverable via utils.sec_identity_evidence.parse_cik_from_archive_url) even
    # though this module itself doesn't parse it — that's the sector-reconciliation
    # layer's job, not the identity join's.
    assert rows["BBB#001"]["identity_entity_id"] == ""
    assert rows["BBB#001"]["identity_source_url"].endswith("/data/999000/0002/filing.htm")


def test_review_projection_drops_source_url(tmp_path: Path) -> None:
    fact_root = tmp_path / "facts"
    _write_facts(fact_root)

    review = load_canonical_facts_for_review(fact_root)

    assert "identity_source_url" not in review.columns
    assert "canonical_identity_source_url" not in review.columns
    assert "canonical_identity_tier" in review.columns
