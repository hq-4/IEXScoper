from __future__ import annotations

import re
from typing import Any

US_EXCH_CODES = {
    "US",
    "UN",
    "UQ",
    "UA",
    "UP",
    "UW",
    "UV",
    "UO",
    "UB",
    "UJ",
    "UT",
    "OTC",
    "PQ",
    "PS",
    "PN",
}
NAME_STOPWORDS = {
    "INC",
    "CORP",
    "CORPORATION",
    "CO",
    "COMPANY",
    "LTD",
    "LLC",
    "LP",
    "THE",
    "AND",
    "OF",
    "CLASS",
    "COM",
    "NEW",
    "HOLDINGS",
    "HLDGS",
    "GROUP",
    "A",
    "B",
    "C",
}


def split_us_matches(response: dict[str, Any], variant: str) -> tuple[list[dict], list[dict]]:
    data = (response or {}).get("data") or []
    if variant in {"no_market_sector"}:
        return list(data), []
    us = [m for m in data if str(m.get("exchCode") or "").upper() in US_EXCH_CODES]
    noise = [m for m in data if m not in us]
    return us, noise


def name_tokens(name: str) -> set[str]:
    tokens = set(re.split(r"[^A-Z0-9]+", name.upper()))
    return {t for t in tokens if len(t) >= 3 and t not in NAME_STOPWORDS}


def name_plausible(returned_name: Any, issuer: str) -> bool:
    returned = name_tokens(str(returned_name or ""))
    return bool(returned & name_tokens(issuer))


def variant_report(
    sample: list[str],
    variant: str,
    cache: dict[str, dict[str, Any]],
    ground_truth: dict[str, str],
) -> dict[str, Any]:
    recalled, noise_only, exch_counts = [], [], {}
    gt_matched = gt_plausible = gt_total = 0
    for symbol in sample:
        response = cache.get(f"{symbol}|{variant}")
        us, noise = split_us_matches(response, variant)
        for match in us + noise:
            code = str(match.get("exchCode") or "")
            exch_counts[code] = exch_counts.get(code, 0) + 1
        if us:
            recalled.append(symbol)
        elif noise:
            noise_only.append(symbol)
        if symbol in ground_truth:
            gt_total += 1
            if us:
                gt_matched += 1
                if any(name_plausible(m.get("name"), ground_truth[symbol]) for m in us):
                    gt_plausible += 1
    total = len(sample)
    return {
        "sample_size": total,
        "recalled_symbols": len(recalled),
        "recall_rate": round(len(recalled) / total, 4) if total else 0.0,
        "noise_only_symbols": len(noise_only),
        "exch_code_distribution": dict(
            sorted(exch_counts.items(), key=lambda kv: kv[1], reverse=True)
        ),
        "ground_truth": {
            "subset_size": gt_total,
            "matched": gt_matched,
            "name_plausible": gt_plausible,
            "name_plausible_rate": round(gt_plausible / gt_matched, 4) if gt_matched else None,
        },
    }


def build_report(
    sample: list[str],
    cache: dict[str, dict[str, Any]],
    ground_truth: dict[str, str],
    variants: tuple[str, ...],
    baseline_recall: float,
) -> dict[str, Any]:
    per_variant = {v: variant_report(sample, v, cache, ground_truth) for v in variants}
    best = max(variants, key=lambda v: per_variant[v]["recall_rate"])
    best_recall = per_variant[best]["recall_rate"]
    lift = best_recall / baseline_recall if baseline_recall else 0.0
    return {
        "sample_size": len(sample),
        "ground_truth_subset_size": sum(1 for s in sample if s in ground_truth),
        "baseline_overall_recall": round(baseline_recall, 4),
        "variants": per_variant,
        "best_variant": best,
        "best_variant_recall": best_recall,
        "lift_vs_baseline": round(lift, 2),
        "recommendation": recommendation_text(best, best_recall, lift, baseline_recall),
    }


def recommendation_text(best: str, recall: float, lift: float, baseline: float) -> str:
    if lift >= 2.0:
        return (
            f"Run full pass with '{best}' (sample recall {recall:.1%}, "
            f"{lift:.1f}x baseline overall match rate)."
        )
    return (
        f"No variant clears the 2x lift bar (best '{best}' at {recall:.1%} sample "
        f"recall vs {baseline:.1%} baseline); regenerate with class fix only."
    )
