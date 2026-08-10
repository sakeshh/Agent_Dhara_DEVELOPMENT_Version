"""
Integration tests for Phase 2 readiness gates, column certificates, composite keys, cardinality, and waivers.
"""
from __future__ import annotations

import unittest
import datetime
from typing import Any, Dict, List

from agent.etl_pipeline.readiness_engine import (
    build_projected_column_certificates,
    build_actual_column_certificates,
    evaluate_operation_gate,
    evaluate_all_operation_gates
)


class TestPhase2ReadinessFlow(unittest.TestCase):
    def setUp(self) -> None:
        self.assessment = {
            "datasets": {
                "marketing.csv": {
                    "row_count": 1000,
                    "columns": {
                        "campaign_id": {
                            "dtype": "string",
                            "null_count": 0,
                            "distinct_count": 500,
                            "llm_hints": {"business_importance": "high"}
                        }
                    }
                },
                "sales.xml": {
                    "row_count": 1000,
                    "columns": {
                        "campaign_id": {
                            "dtype": "string",
                            "null_count": 50,
                            "distinct_count": 450,
                            "llm_hints": {"business_importance": "high"}
                        },
                        "product_id": {
                            "dtype": "string",
                            "null_count": 10,
                            "distinct_count": 300,
                            "llm_hints": {"business_importance": "medium"}
                        }
                    }
                }
            }
        }
        
        self.plan = {
            "plan_id": "test_plan",
            "generation_mode": "cleanse",
            "provenance_signature": "sha256-signature-123",
            "business_rules": {},
            "datasets": {
                "marketing.csv": {"steps": []},
                "sales.xml": {"steps": []}
            },
            "relationships": {
                "joins": [
                    {
                        "left_dataset": "marketing.csv",
                        "right_dataset": "sales.xml",
                        "left_keys": ["campaign_id"],
                        "right_keys": ["campaign_id"],
                        "cardinality": "ONE_TO_MANY",
                        "require_parent_unique": True
                    }
                ]
            },
            "manual_review": []
        }

    def test_projected_certificates_generation(self) -> None:
        certs = build_projected_column_certificates(self.plan, self.assessment)
        self.assertTrue(len(certs) > 0)
        
        # parent key: campaign_id in marketing
        parent_cert = next(c for c in certs if c["dataset_id"] == "marketing.csv" and c["source_column"] == "campaign_id")
        self.assertEqual(parent_cert["semantic_role"], "PARENT_KEY")
        self.assertEqual(parent_cert["readiness_status"], "READY")
        self.assertEqual(parent_cert["certificate_type"], "PROJECTED")
        
        # child key: campaign_id in sales. Has 50 nulls and allow_null is True (since it's a child key)
        child_cert = next(c for c in certs if c["dataset_id"] == "sales.xml" and c["source_column"] == "campaign_id")
        self.assertEqual(child_cert["semantic_role"], "CHILD_KEY")
        self.assertEqual(child_cert["readiness_status"], "READY_WITH_WARNINGS")

    def test_failed_certificate_blocks_phase2(self) -> None:
        # Introduce an unresolved manual review card on marketing.campaign_id (parent key)
        self.plan["manual_review"].append({
            "id": "mr_item_1",
            "dataset": "marketing.csv",
            "column": "campaign_id",
            "issue_type": "declared_key_null_violation",
            "message": "Campaign ID null values",
            "status": "pending" # unresolved
        })
        
        certs = build_projected_column_certificates(self.plan, self.assessment)
        parent_cert = next(c for c in certs if c["dataset_id"] == "marketing.csv" and c["source_column"] == "campaign_id")
        self.assertEqual(parent_cert["readiness_status"], "BLOCKED_BY_UNRESOLVED_POLICY")
        self.assertEqual(len(parent_cert["blocking_reasons"]), 1)

    def test_duplicate_parent_blocks_one_to_many(self) -> None:
        # Set duplicate count > 0 in parent metadata
        self.assessment["datasets"]["marketing.csv"]["columns"]["campaign_id"]["duplicate_key_count"] = 5
        
        # Get certificates
        certs = build_projected_column_certificates(self.plan, self.assessment)
        
        # Force duplicate key count in parent cert metrics
        parent_cert = next(c for c in certs if c["dataset_id"] == "marketing.csv" and c["source_column"] == "campaign_id")
        parent_cert["metrics"]["duplicate_key_count"] = 5
        
        # Evaluate gate
        child_cert = next(c for c in certs if c["dataset_id"] == "sales.xml" and c["source_column"] == "campaign_id")
        
        contract = {
            "operation_id": "op_test",
            "parent": {"dataset_id": "marketing.csv", "columns": ["campaign_id"]},
            "child": {"dataset_id": "sales.xml", "columns": ["campaign_id"]},
            "require_parent_unique": True
        }
        
        gate = evaluate_operation_gate([parent_cert], [child_cert], contract, [])
        self.assertEqual(gate["gate_status"], "BLOCKED")
        self.assertEqual(gate["gate_reason_code"], "BLOCKED_BY_PARENT_NON_UNIQUENESS")
        self.assertTrue(any("Parent uniqueness failure" in r for r in gate["blocking_reasons"]))

    def test_duplicate_child_allowed_in_one_to_many(self) -> None:
        # Duplicates on child key should NOT block ONE_TO_MANY
        certs = build_projected_column_certificates(self.plan, self.assessment)
        parent_cert = next(c for c in certs if c["dataset_id"] == "marketing.csv" and c["source_column"] == "campaign_id")
        child_cert = next(c for c in certs if c["dataset_id"] == "sales.xml" and c["source_column"] == "campaign_id")
        child_cert["metrics"]["duplicate_key_count"] = 10
        
        contract = {
            "operation_id": "op_test",
            "parent": {"dataset_id": "marketing.csv", "columns": ["campaign_id"]},
            "child": {"dataset_id": "sales.xml", "columns": ["campaign_id"]},
            "require_parent_unique": True
        }
        
        gate = evaluate_operation_gate([parent_cert], [child_cert], contract, [])
        self.assertEqual(gate["gate_status"], "READY_WITH_WARNINGS") # because child has nulls warning

    def test_valid_waiver_produces_ready_with_warnings(self) -> None:
        # Create a parent uniqueness block
        certs = build_projected_column_certificates(self.plan, self.assessment)
        parent_cert = next(c for c in certs if c["dataset_id"] == "marketing.csv" and c["source_column"] == "campaign_id")
        parent_cert["metrics"]["duplicate_key_count"] = 5
        child_cert = next(c for c in certs if c["dataset_id"] == "sales.xml" and c["source_column"] == "campaign_id")
        
        contract = {
            "operation_id": "op_test",
            "parent": {"dataset_id": "marketing.csv", "columns": ["campaign_id"]},
            "child": {"dataset_id": "sales.xml", "columns": ["campaign_id"]},
            "require_parent_unique": True
        }
        
        # Valid waiver for parent uniqueness
        waivers = [
            {
                "waiver_id": "WV-2026-001",
                "affected_requirement": "PARENT_NON_UNIQUE_ALLOWED",
                "scope": {"operation_id": "op_test"},
                "effective_at": "2026-01-01",
                "expires_at": "2026-12-31",
                "reason": "Approved parent duplicates exception"
            }
        ]
        
        gate = evaluate_operation_gate([parent_cert], [child_cert], contract, waivers)
        self.assertEqual(gate["gate_status"], "READY_WITH_WARNINGS")
        self.assertEqual(len(gate["blocking_reasons"]), 0)
        self.assertEqual(len(gate["warnings"]), 2) # Null warning + Waiver warning

    def test_expired_waiver_does_not_unlock_gate(self) -> None:
        certs = build_projected_column_certificates(self.plan, self.assessment)
        parent_cert = next(c for c in certs if c["dataset_id"] == "marketing.csv" and c["source_column"] == "campaign_id")
        parent_cert["metrics"]["duplicate_key_count"] = 5
        child_cert = next(c for c in certs if c["dataset_id"] == "sales.xml" and c["source_column"] == "campaign_id")
        
        contract = {
            "operation_id": "op_test",
            "parent": {"dataset_id": "marketing.csv", "columns": ["campaign_id"]},
            "child": {"dataset_id": "sales.xml", "columns": ["campaign_id"]},
            "require_parent_unique": True
        }
        
        # Expired waiver
        waivers = [
            {
                "waiver_id": "WV-2026-001",
                "affected_requirement": "PARENT_NON_UNIQUE_ALLOWED",
                "scope": {"operation_id": "op_test"},
                "effective_at": "2025-01-01",
                "expires_at": "2025-12-31", # expired
                "reason": "Expired parent duplicates exception"
            }
        ]
        
        gate = evaluate_operation_gate([parent_cert], [child_cert], contract, waivers)
        self.assertEqual(gate["gate_status"], "BLOCKED")
        self.assertEqual(gate["gate_reason_code"], "BLOCKED_BY_PARENT_NON_UNIQUENESS")

    def test_composite_key_type_parity(self) -> None:
        # Test composite key check with matching and mismatched types
        certs = build_projected_column_certificates(self.plan, self.assessment)
        parent_campaign = next(c for c in certs if c["dataset_id"] == "marketing.csv" and c["source_column"] == "campaign_id")
        
        # Let's mock a composite key parent Campaign ID + Tenant ID
        parent_tenant = parent_campaign.copy()
        parent_tenant["source_column"] = "tenant_id"
        parent_tenant["physical_type"] = "integer"
        
        child_campaign = next(c for c in certs if c["dataset_id"] == "sales.xml" and c["source_column"] == "campaign_id")
        child_tenant = child_campaign.copy()
        child_tenant["source_column"] = "tenant_id"
        child_tenant["physical_type"] = "string" # Mismatched type
        child_tenant["normalization_contract_hash"] = parent_tenant["normalization_contract_hash"]
        
        contract = {
            "operation_id": "composite_op",
            "parent": {"dataset_id": "marketing.csv", "columns": ["campaign_id", "tenant_id"]},
            "child": {"dataset_id": "sales.xml", "columns": ["campaign_id", "tenant_id"]}
        }
        
        gate = evaluate_operation_gate(
            [parent_campaign, parent_tenant],
            [child_campaign, child_tenant],
            contract,
            []
        )
        
        self.assertEqual(gate["gate_status"], "BLOCKED")
        self.assertEqual(gate["gate_reason_code"], "BLOCKED_BY_TYPE_MISMATCH")
        self.assertTrue(any("Type mismatch" in r for r in gate["blocking_reasons"]))

    def test_source_fingerprint_change_stales_certificate(self) -> None:
        certs = build_projected_column_certificates(self.plan, self.assessment)
        parent_cert = next(c for c in certs if c["dataset_id"] == "marketing.csv" and c["source_column"] == "campaign_id")
        child_cert = next(c for c in certs if c["dataset_id"] == "sales.xml" and c["source_column"] == "campaign_id")
        
        # Expected source fingerprint is different from the certificate's source fingerprint
        contract = {
            "operation_id": "op_test",
            "parent": {"dataset_id": "marketing.csv", "columns": ["campaign_id"]},
            "child": {"dataset_id": "sales.xml", "columns": ["campaign_id"]},
            "expected_parent_fingerprint": "sha256-some-other-newer-fingerprint"
        }
        
        gate = evaluate_operation_gate([parent_cert], [child_cert], contract, [])
        self.assertEqual(gate["gate_status"], "BLOCKED")
        self.assertEqual(gate["gate_reason_code"], "BLOCKED_BY_SOURCE_MISMATCH")
        self.assertTrue(any("source fingerprint mismatch" in r for r in gate["blocking_reasons"]))

    def test_normalization_contract_change_blocks_gate(self) -> None:
        certs = build_projected_column_certificates(self.plan, self.assessment)
        parent_cert = next(c for c in certs if c["dataset_id"] == "marketing.csv" and c["source_column"] == "campaign_id")
        child_cert = next(c for c in certs if c["dataset_id"] == "sales.xml" and c["source_column"] == "campaign_id")
        
        # Mismatched normalization hashes
        parent_cert["normalization_contract_hash"] = "sha256:normalized_contract_A"
        child_cert["normalization_contract_hash"] = "sha256:normalized_contract_B"
        
        contract = {
            "operation_id": "op_test",
            "parent": {"dataset_id": "marketing.csv", "columns": ["campaign_id"]},
            "child": {"dataset_id": "sales.xml", "columns": ["campaign_id"]}
        }
        
        gate = evaluate_operation_gate([parent_cert], [child_cert], contract, [])
        self.assertEqual(gate["gate_status"], "BLOCKED")
        self.assertEqual(gate["gate_reason_code"], "BLOCKED_BY_NORMALIZATION_MISMATCH")

    def test_composite_key_column_order_matters(self) -> None:
        certs = build_projected_column_certificates(self.plan, self.assessment)
        parent_campaign = next(c for c in certs if c["dataset_id"] == "marketing.csv" and c["source_column"] == "campaign_id")
        child_campaign = next(c for c in certs if c["dataset_id"] == "sales.xml" and c["source_column"] == "campaign_id")
        
        parent_tenant = parent_campaign.copy()
        parent_tenant["source_column"] = "tenant_id"
        parent_tenant["physical_type"] = "integer"
        
        child_tenant = child_campaign.copy()
        child_tenant["source_column"] = "tenant_id"
        child_tenant["physical_type"] = "string"
        child_tenant["normalization_contract_hash"] = parent_tenant["normalization_contract_hash"]
        
        # Order is different in parent and child column lists
        contract = {
            "operation_id": "composite_op",
            "parent": {"dataset_id": "marketing.csv", "columns": ["campaign_id", "tenant_id"]},
            "child": {"dataset_id": "sales.xml", "columns": ["tenant_id", "campaign_id"]}
        }
        
        gate = evaluate_operation_gate(
            [parent_campaign, parent_tenant],
            [child_campaign, child_tenant],
            contract,
            []
        )
        # Zip logic checks order element-by-element, so matching campaign_id (string) vs tenant_id (string)
        # and tenant_id (integer) vs campaign_id (string) will fail physical type check on the integer/string mismatch
        self.assertEqual(gate["gate_status"], "BLOCKED")
        self.assertEqual(gate["gate_reason_code"], "BLOCKED_BY_TYPE_MISMATCH")

    def test_missing_required_dataset_blocks_operation(self) -> None:
        certs = build_projected_column_certificates(self.plan, self.assessment)
        child_cert = next(c for c in certs if c["dataset_id"] == "sales.xml" and c["source_column"] == "campaign_id")
        
        contract = {
            "operation_id": "op_test",
            "parent": {"dataset_id": "marketing.csv", "columns": ["campaign_id"]},
            "child": {"dataset_id": "sales.xml", "columns": ["campaign_id"]}
        }
        
        # Pass empty parent certificates
        gate = evaluate_operation_gate([], [child_cert], contract, [])
        self.assertEqual(gate["gate_status"], "BLOCKED")
        self.assertEqual(gate["gate_reason_code"], "BLOCKED_BY_MISSING_DATASET")

    def test_missing_required_column_blocks_operation(self) -> None:
        certs = build_projected_column_certificates(self.plan, self.assessment)
        parent_cert = next(c for c in certs if c["dataset_id"] == "marketing.csv" and c["source_column"] == "campaign_id")
        child_cert = next(c for c in certs if c["dataset_id"] == "sales.xml" and c["source_column"] == "campaign_id")
        
        # Contract requests a missing key column
        contract = {
            "operation_id": "op_test",
            "parent": {"dataset_id": "marketing.csv", "columns": ["campaign_id", "missing_col"]},
            "child": {"dataset_id": "sales.xml", "columns": ["campaign_id", "missing_col"]}
        }
        
        gate = evaluate_operation_gate([parent_cert], [child_cert], contract, [])
        self.assertEqual(gate["gate_status"], "BLOCKED")
        self.assertEqual(gate["gate_reason_code"], "BLOCKED_BY_MISSING_COLUMN")


if __name__ == "__main__":
    unittest.main()
