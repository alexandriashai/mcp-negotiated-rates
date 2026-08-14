"""Serve the negotiated rates already extracted from the Anthem Colorado MRFs.

WHY THIS EXISTS. Two Transparency-in-Coverage in-network files sit in data/ at
7.5GB and 7.6GB, and someone has already run a two-pass extraction over both,
producing NPI-plus-code-plus-rate for 52 providers across 18 procedure codes —
including six gender-affirmation codes. Nothing served it. `/rates/find`
returns a pointer to the 7.5GB source and a note saying to "use
lookup_negotiated_rate", an endpoint that does not exist.

So the extraction was a producer with no consumer: the work was done, verified,
and unreachable by the chat that needed it.

WHAT A RATE HERE PROVES, AND WHAT IT DOES NOT. A negotiated rate is evidence of
a CONTRACT between this insurer and this provider for this code. It is not
evidence that the provider performs the procedure, takes new patients, or has
ever billed it once. Presenting it as "these surgeons do vaginoplasty" would be
the confident-wrong-answer failure this whole site is built against. It IS good
evidence for an in-network argument, which is a different and useful thing.

SCOPE, STATED EVERY TIME. Anthem, Colorado, two files. A provider absent from
this list is not absent from the world — they may be with another insurer,
another state, or simply not in the two files on this disk.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

DATA = Path(__file__).resolve().parents[2] / "data"

# The two-pass extractions, and what each covers. Named rather than globbed:
# data/ also holds targeted and single-provider result files with different
# shapes, and quietly folding those in would mix scopes without saying so.
SOURCES = {
    "co_ppo_innetwork_twopass_results.json": "Anthem Colorado PPO, in-network file",
    "co_anschutz_innetwork_twopass_results.json": "Anthem Colorado (CU Anschutz), in-network file",
}


@lru_cache(maxsize=1)
def _index() -> dict:
    """code -> {npi -> [rates]}, plus the source list. Cached; the files are static."""
    by_code: dict[str, dict[str, list]] = {}
    loaded: list[str] = []
    for fname, label in SOURCES.items():
        path = DATA / fname
        if not path.exists():
            continue
        try:
            raw = json.loads(path.read_text())
        except Exception:
            continue
        loaded.append(label)
        for key, val in (raw.get("npi_code_rates") or {}).items():
            # Keys are "<npi>_<code>". rpartition, not split: an NPI never
            # contains an underscore but splitting on the first one would break
            # the moment a code did.
            npi, _, code = key.rpartition("_")
            if not npi or not code:
                continue
            by_code.setdefault(code, {}).setdefault(npi, [])
            vals = val if isinstance(val, list) else [val]
            # The extraction stores [rate, modifier, rate, modifier, ...]; take
            # the entries that look like money and drop the rest.
            by_code[code][npi].extend(v for v in vals if isinstance(v, (int, float)) and v >= 100)
    return {"by_code": by_code, "sources": loaded}


def codes() -> list[str]:
    return sorted(_index()["by_code"])


def providers_for_code(billing_code: str) -> dict:
    """Every NPI in the extract with a negotiated rate for one code."""
    idx = _index()
    hits = idx["by_code"].get(str(billing_code), {})
    out = []
    for npi, rates in hits.items():
        clean = sorted(set(rates))
        out.append(
            {
                "npi": npi,
                "negotiated_rates": clean,
                "rate_low": clean[0] if clean else None,
                "rate_high": clean[-1] if clean else None,
            }
        )
    out.sort(key=lambda r: r["rate_low"] if r["rate_low"] is not None else 0)
    return {
        "billing_code": str(billing_code),
        "insurer": "anthem",
        "provider_count": len(out),
        "providers": out,
        "sources": idx["sources"],
        "scope": (
            "Anthem, Colorado only, from the in-network files held on this server. "
            "A provider missing here is not evidence they are out of network anywhere else."
        ),
        "means": (
            "A negotiated rate proves a CONTRACT between Anthem and this provider for this "
            "code. It is not evidence that they perform the procedure, take new patients, or "
            "have ever billed it. Confirm both with the provider's office."
        ),
    }
