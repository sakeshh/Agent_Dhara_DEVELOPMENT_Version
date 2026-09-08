"""Tests for PR 6 Multiset Row Identity Reconciliation and Clean XOR Quarantine.

Verifies:
- Stable technical row ID generation (source_snapshot + dataset + offset)
- Multiset equality: count_by_id(input) == count_by_id(clean) + count_by_id(quarantine)
- Strict mutual exclusivity: clean_ids INTERSECT quarantine_ids == empty
- Detection of row loss, duplication, and multiplicity mismatch
- Null-safe, type-aware raw companion verification
- Two-tier scoring (observed clean score vs full population score)
"""

import pytest
from agent.etl_pipeline.dry_run_validator import (
    generate_technical_row_ids,
    reconcile_row_identities,
    validate_raw_value_preservation,
    compute_two_tier_dq_scores,
)


def test_technical_row_id_generation_and_exclusivity():
    """Generates deterministic technical IDs and validates strict clean XOR quarantine."""
    input_ids = generate_technical_row_ids("dbo.Customers", 10, source_snapshot_id="snap_20260907")
    assert len(input_ids) == 10
    assert len(set(input_ids)) == 10  # All distinct

    # Partitioning scenario: 8 clean, 2 quarantined
    clean_ids = input_ids[:8]
    quar_ids = input_ids[8:]

    ok, violations = reconcile_row_identities(
        input_ids=input_ids,
        clean_ids=clean_ids,
        quarantine_ids=quar_ids,
        action_id="quarantine_row",
        is_partitioning=True,
    )
    assert ok is True
    assert len(violations) == 0


def test_multiset_row_loss_and_overlap_detection():
    """Detects when rows are duplicated, lost, or leak across both partitions."""
    input_ids = ["trid_01", "trid_02", "trid_03", "trid_04"]

    # 1. Row loss: trid_04 completely missing
    ok_loss, err_loss = reconcile_row_identities(
        input_ids=input_ids,
        clean_ids=["trid_01", "trid_02"],
        quarantine_ids=["trid_03"],
        is_partitioning=True,
    )
    assert ok_loss is False
    assert any("Row loss detected" in e for e in err_loss)

    # 2. Row overlap: trid_02 present in BOTH clean and quarantine
    ok_overlap, err_overlap = reconcile_row_identities(
        input_ids=input_ids,
        clean_ids=["trid_01", "trid_02"],
        quarantine_ids=["trid_02", "trid_03", "trid_04"],
        is_partitioning=True,
    )
    assert ok_overlap is False
    assert any("Mutual exclusivity violation" in e for e in err_overlap)

    # 3. Non-partitioning action with unexpected quarantine rows
    ok_non_part, err_non_part = reconcile_row_identities(
        input_ids=input_ids,
        clean_ids=input_ids,
        quarantine_ids=["trid_01"],
        action_id="flag_only",
        is_partitioning=False,
    )
    assert ok_non_part is False
    assert any("non-partitioning but emitted" in e for e in err_non_part)


def test_null_safe_raw_value_preservation():
    """Verifies that companion _raw columns preserve native types and null states without mutation."""
    inputs = [
        {"_trid": "r1", "Price": -15.5, "Age": 25, "Note": None},
        {"_trid": "r2", "Price": 100.0, "Age": None, "Note": "Valid"},
    ]
    outputs_clean = [
        {"_trid": "r1", "Price_raw": -15.5, "Age_raw": 25, "Note_raw": None},
        {"_trid": "r2", "Price_raw": 100.0, "Age_raw": None, "Note_raw": "Valid"},
    ]

    ok, errs = validate_raw_value_preservation(inputs, outputs_clean, target_column="Price")
    assert ok is True
    assert len(errs) == 0

    # Test failure when raw value is mutated or cast to string
    outputs_mutated = [
        {"_trid": "r1", "Price_raw": "-15.5"},  # Mutated from float to str
        {"_trid": "r2", "Price_raw": 100.0},
    ]
    ok_mut, errs_mut = validate_raw_value_preservation(inputs, outputs_mutated, target_column="Price")
    assert ok_mut is False
    assert any("Raw type mutation" in e for e in errs_mut)


def test_two_tier_dq_scores():
    """Reports clean flow score separately from full population coverage score."""
    # 100 clean score with 20% quarantined
    scores = compute_two_tier_dq_scores(
        clean_score=100.0,
        total_rows=100,
        quarantined_rows=20,
    )

    assert scores["observed_clean_score"] == 100.0
    assert scores["observed_full_population_score"] == 80.0
    assert scores["quarantine_percentage"] == 20.0
    assert scores["coverage_percentage"] == 80.0
    assert scores["release_gate_status"] == "blocked"


def test_captured_row_ids_uniqueness_and_32_char_hash_prefix():
    """Generates 32-hex-char prefix (128-bit space) and validates uniqueness invariant."""
    from agent.etl_pipeline.dry_run_validator import validate_captured_row_ids_uniqueness
    ids = generate_technical_row_ids("dbo.Orders", 25, source_snapshot_id="snap_v1", partition_id="part_0")
    assert len(ids) == 25
    # Format: trid_<32-hex>_<8-digit-offset>
    parts = ids[0].split("_")
    assert len(parts) == 3
    assert len(parts[1]) == 32  # 32-hex char entropy (128-bit)

    ok, err = validate_captured_row_ids_uniqueness(ids)
    assert ok is True
    assert err is None

    # Test collision / duplication failure
    dup_ids = ids + [ids[0]]
    ok_dup, err_dup = validate_captured_row_ids_uniqueness(dup_ids)
    assert ok_dup is False
    assert "Non-unique captured row IDs" in err_dup


def test_dry_run_identity_metadata():
    """Validates technical row identity versioning and provenance contract metadata."""
    from agent.etl_pipeline.dry_run_validator import build_dry_run_identity_metadata
    meta = build_dry_run_identity_metadata(
        source_identity_method="delta_commit_file_row_position",
        capture_uniqueness_validated=True,
    )
    assert meta["technical_row_id_version"] == "trid-v2"
    assert meta["hash_algorithm"] == "sha256"
    assert meta["hash_prefix_hex_length"] == 32
    assert meta["source_identity_method"] == "delta_commit_file_row_position"
    assert meta["capture_uniqueness_validated"] is True


