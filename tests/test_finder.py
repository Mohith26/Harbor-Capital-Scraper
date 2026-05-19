"""Tests for Comp Finder scoring (comp_finder.compute_match_scores).

Bug context: Chad reported that the % match column always shows 0%
(or clusters at a few values — see B028).

These tests pin down the correct behavior:
- form-key weights ('distance','size','price','age') must be honored by the scorer
- subject vs comp deltas must produce a smooth, spread-out distribution
- closer / more-similar comps must outrank farther / less-similar ones
- missing subject data on one axis must NOT zero everything out;
  weights should re-distribute to the axes we DO have signal on
"""

import os
os.environ.setdefault("LEARNING_DB_PATH", "learning_local.db")

import pandas as pd
import pytest

from comp_finder import compute_match_scores


@pytest.fixture
def subject_4801_glenwood():
    """Same subject Chad used in B028."""
    return {
        "latitude": 35.8460,
        "longitude": -78.6970,
        "address": "4801 Glenwood Ave, Raleigh, NC 27612",
        "building_size": 40000,
    }


@pytest.fixture
def realistic_sale_comps():
    """Mimics the seeded sale comps in learning_local.db.

    Comps span size 12.5k - 150k SF, distances 0 - 9 mi from subject, with
    realistic year_built and closing_date values.
    """
    return pd.DataFrame([
        {"address": "4801 Glenwood Ave, Raleigh",      "latitude": 35.8460, "longitude": -78.6970,
         "building_size": 35000, "sale_price": 7_000_000, "price_per_sf": 200.0,
         "year_built": 2008, "closing_date": "2024-06-01"},
        {"address": "4501 Creedmoor Rd, Raleigh",      "latitude": 35.8514, "longitude": -78.6845,
         "building_size": 47000, "sale_price": 9_500_000, "price_per_sf": 202.0,
         "year_built": 2014, "closing_date": "2024-08-15"},
        {"address": "3501 Glenwood Ave, Raleigh",      "latitude": 35.8395, "longitude": -78.6850,
         "building_size": 62000, "sale_price": 12_000_000, "price_per_sf": 193.0,
         "year_built": 2001, "closing_date": "2024-03-20"},
        {"address": "8401 Six Forks Rd, Raleigh",      "latitude": 35.8740, "longitude": -78.6403,
         "building_size": 24500, "sale_price": 5_000_000, "price_per_sf": 204.0,
         "year_built": 1992, "closing_date": "2024-02-10"},
        {"address": "200 S Salisbury St, Raleigh",     "latitude": 35.7780, "longitude": -78.6420,
         "building_size": 85000, "sale_price": 16_000_000, "price_per_sf": 188.0,
         "year_built": 1985, "closing_date": "2024-01-05"},
        {"address": "1100 Crescent Green Dr, Cary",    "latitude": 35.7320, "longitude": -78.7660,
         "building_size": 110000, "sale_price": 20_000_000, "price_per_sf": 181.0,
         "year_built": 2019, "closing_date": "2024-09-12"},
        {"address": "2500 Blue Ridge Rd, Raleigh",     "latitude": 35.8108, "longitude": -78.7050,
         "building_size": 18000, "sale_price": 3_500_000, "price_per_sf": 194.0,
         "year_built": 2014, "closing_date": "2024-07-01"},
        {"address": "9700 Fayetteville Rd, Raleigh",   "latitude": 35.6843, "longitude": -78.6480,
         "building_size": 150000, "sale_price": 30_000_000, "price_per_sf": 200.0,
         "year_built": 1992, "closing_date": "2024-04-01"},
    ])


# ---------------------------------------------------------------------------
# 1. The bug: form-key weights must be honored
# ---------------------------------------------------------------------------

def test_form_keys_distance_and_age_actually_weight_scoring(
    subject_4801_glenwood, realistic_sale_comps
):
    """The HTML form posts weight_distance/weight_size/weight_price/weight_age.

    The route maps these into the dict as 'distance','size','price','age'.
    The scorer MUST recognize those keys and weight the proximity and age
    axes accordingly.

    Detection technique: if 'distance' is ignored, the final score equals the
    size_score alone (only axis Chad's form provides data for). If distance
    is honored, the final score must differ from size_score because nearby
    comps get a proximity bonus.
    """
    weights = {"distance": 0.4, "size": 0.3, "price": 0.2, "age": 0.1}
    scored = compute_match_scores(
        subject_4801_glenwood, realistic_sale_comps.copy(), "sales",
        weights, max_radius=10.0,
    )

    # Distance must actually contribute. A 0-mi comp and a 9-mi comp at
    # the SAME size cannot end up with the same match_score.
    scored = scored.set_index("address")
    # subject is at 4801 Glenwood (0 mi); 9700 Fayetteville is 9+ mi away
    # Both have size 35000 and 150000 respectively, but we want to confirm
    # proximity matters: insert two comps with identical size, different distances.
    # Use existing rows: 4801 Glenwood (0mi, 35k), 4501 Creedmoor (0.8mi, 47k)
    # vs 200 S Salisbury (5.6mi, 85k) — top should clearly outrank far/different.
    top = scored["match_score"].max()
    assert top > 0.5, (
        f"Top comp scored only {top:.2%} — weights aren't being applied. "
        f"Form key 'distance' must map to the proximity axis."
    )

    # The proximity column should be computed AND have varying values.
    # If the scorer never weights it, you can't tell from the final number,
    # so check that nearer comps clearly outrank farther same-size comps.
    nearest = scored.loc["4801 Glenwood Ave, Raleigh", "match_score"]  # 0 mi, 35k SF
    far_zero_overlap = scored.loc["9700 Fayetteville Rd, Raleigh", "match_score"]  # 9+ mi, 150k SF
    assert nearest > far_zero_overlap + 0.1, (
        f"Nearest comp scored {nearest:.2%}, far+dissimilar scored "
        f"{far_zero_overlap:.2%}. Distance/age weights are not being honored."
    )


def test_address_only_subject_does_not_zero_all_scores(
    realistic_sale_comps,
):
    """Chad's main bug report: opening Comp Finder with JUST an address
    (no building_size) returns 0% for every comp.

    Expected: distance still scores, so nearby comps rank near 100% and
    far comps decay toward 0%.
    """
    subject_no_size = {
        "latitude": 35.8460,
        "longitude": -78.6970,
        "address": "4801 Glenwood Ave, Raleigh, NC 27612",
    }
    weights = {"distance": 0.4, "size": 0.3, "price": 0.2, "age": 0.1}
    scored = compute_match_scores(
        subject_no_size, realistic_sale_comps.copy(), "sales",
        weights, max_radius=10.0,
    )

    assert scored["match_score"].max() > 0.5, (
        f"Address-only subject scored max {scored['match_score'].max():.2%}. "
        f"With distance weight enabled, nearby comps should be high. "
        f"Bug: missing subject data on size/price kills all scores."
    )


# ---------------------------------------------------------------------------
# 2. Scores should spread, not cluster (fixes B028)
# ---------------------------------------------------------------------------

def test_match_scores_are_spread_not_clustered(
    subject_4801_glenwood,
):
    """B028: with the full seeded DB (18 sale comps), scores cluster at
    5x78%, then 28%, 22%, 0%. They should form a smooth, decreasing curve.

    Uses load_comps against the seeded learning_local.db so this test is a
    realistic reproduction of Chad's complaint.
    """
    from comp_finder import load_comps
    comps_df = load_comps("sales")
    assert len(comps_df) >= 15, (
        f"Need a seeded DB with >=15 comps for this test, have {len(comps_df)}. "
        f"Run `python3 .context/seed_data.py` to seed."
    )

    weights = {"distance": 0.4, "size": 0.3, "price": 0.2, "age": 0.1}
    scored = compute_match_scores(
        subject_4801_glenwood, comps_df, "sales",
        weights, max_radius=15.0,
    )

    scores = scored["match_score"].tolist()

    # No more than ~30% of comps can share the same rounded score.
    # The original bug: 5/18 (28%) at 0.75 plus another 10/18 at 0.0.
    from collections import Counter
    most_common_score, most_common_count = Counter(round(s, 2) for s in scores).most_common(1)[0]
    assert most_common_count <= max(3, len(scores) // 3), (
        f"{most_common_count}/{len(scores)} comps cluster at {most_common_score:.2%}. "
        f"Scoring is binary-stepped instead of smooth. "
        f"Distribution: {sorted(Counter(round(s, 2) for s in scores).items())}"
    )

    # No more than ~25% of comps can be exactly zero — distance/recency
    # should give every comp at least some signal.
    zero_count = sum(1 for s in scores if s == 0.0)
    assert zero_count <= max(2, len(scores) // 4), (
        f"{zero_count}/{len(scores)} comps scored exactly 0%. "
        f"Scoring formula is zeroing out too many comps."
    )


def test_closer_more_similar_comp_outranks_farther_dissimilar(
    subject_4801_glenwood, realistic_sale_comps
):
    """A comp 0.8 mi away at 47k SF should outrank one 9 mi away at 150k SF."""
    weights = {"distance": 0.4, "size": 0.3, "price": 0.2, "age": 0.1}
    scored = compute_match_scores(
        subject_4801_glenwood, realistic_sale_comps.copy(), "sales",
        weights, max_radius=15.0,
    )

    scored = scored.set_index("address")
    near_similar = scored.loc["4501 Creedmoor Rd, Raleigh", "match_score"]
    far_dissimilar = scored.loc["9700 Fayetteville Rd, Raleigh", "match_score"]

    assert near_similar > far_dissimilar, (
        f"Near+similar comp scored {near_similar:.2%}, far+dissimilar "
        f"scored {far_dissimilar:.2%} — ranking is wrong."
    )


# ---------------------------------------------------------------------------
# 3. Missing-axis handling: re-weight, don't penalize as zero
# ---------------------------------------------------------------------------

def test_missing_subject_field_reweights_instead_of_zeroing(
    realistic_sale_comps,
):
    """If subject doesn't provide year_built, the 'age' weight should be
    redistributed across the other axes — not silently dropped (leaving the
    remaining weights summing < 1.0)."""
    subject = {
        "latitude": 35.8460,
        "longitude": -78.6970,
        "address": "test",
        "building_size": 40000,
        # no year_built, no sale_price
    }
    weights = {"distance": 0.4, "size": 0.3, "price": 0.2, "age": 0.1}
    scored = compute_match_scores(
        subject, realistic_sale_comps.copy(), "sales",
        weights, max_radius=15.0,
    )

    # Even with only distance + size signals, near-and-similar comp should be high.
    scored = scored.set_index("address")
    near_similar = scored.loc["4501 Creedmoor Rd, Raleigh", "match_score"]
    assert near_similar > 0.5, (
        f"With distance + size signals, a 0.8mi/47k-SF comp scored only "
        f"{near_similar:.2%}. Missing axes should be dropped (re-weighted), "
        f"not treated as 0."
    )
