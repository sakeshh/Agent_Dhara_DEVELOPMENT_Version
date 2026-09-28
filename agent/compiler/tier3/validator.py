from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from agent.compiler.models.profile import DatasetProfile
from agent.compiler.models.tier3_resolution import Tier3ResolutionProposal
from agent.compiler.models.validation import DiagnosticSeverity
from agent.compiler.tier3.conflict_detector import Tier3ConflictDetector
from agent.compiler.tier3.contract_validator import Tier3ContractValidator
from agent.compiler.tier3.safety_validator import Tier3SafetyValidator
from agent.compiler.tier3.schema_validator import Tier3SchemaValidator
from agent.compiler.tier3.type_validator import Tier3TypeValidator
from agent.compiler.tier3.validation_result import (
    Tier3ValidationResult,
    ValidationCode,
    ValidationFinding,
    ValidationSeverity,
)


class Tier3ValidationOrchestrator:
    """
    Orchestrates the complete Tier 3 validation pipeline:
      ContractValidator -> SchemaValidator -> TypeValidator -> SafetyValidator -> ConflictDetector
    
    Guarantees:
      - Does not mutate the original proposal.
      - Aggregates independent findings across layers in a single pass.
      - Enforces strict fail-closed semantic boundaries.
    """

    def __init__(self, default_schema: str = "dbo"):
        self.contract_validator = Tier3ContractValidator()
        self.schema_validator = Tier3SchemaValidator(default_schema=default_schema)
        self.type_validator = Tier3TypeValidator()
        self.safety_validator = Tier3SafetyValidator()
        self.conflict_detector = Tier3ConflictDetector()

    def validate_proposal(
        self,
        proposal: Union[Dict[str, Any], Tier3ResolutionProposal],
        dataset_profiles: Dict[str, DatasetProfile],
    ) -> Tier3ValidationResult:
        all_findings: List[ValidationFinding] = []

        # 1. Contract Validation
        contract_res = self.contract_validator.validate(proposal)
        if not contract_res.is_valid:
            for issue in contract_res.issues:
                sev = ValidationSeverity.ERROR if issue.severity in (DiagnosticSeverity.ERROR, DiagnosticSeverity.BLOCKING) else ValidationSeverity.WARNING
                all_findings.append(
                    ValidationFinding(
                        validator="ContractValidator",
                        code=ValidationCode.MISSING_PARAMETER if "MISSING" in issue.issue_code else ValidationCode.ACTION_TYPE_INCOMPATIBLE,
                        severity=sev,
                        message=issue.message,
                    )
                )
            return Tier3ValidationResult(valid=False, findings=all_findings, normalized_proposal=None)

        validated_proposal = contract_res.proposal
        if not validated_proposal:
            return Tier3ValidationResult(valid=False, findings=all_findings, normalized_proposal=None)

        # 2. Schema Validation
        schema_res = self.schema_validator.validate(validated_proposal, dataset_profiles)
        all_findings.extend(schema_res.findings)

        # If schema is invalid, we cannot reliably inspect column types
        if schema_res.has_errors or not schema_res.normalized_proposal:
            return Tier3ValidationResult(
                valid=False,
                findings=all_findings,
                normalized_proposal=None,
                proposal_fingerprint=validated_proposal.fingerprint,
            )

        normalized_p = schema_res.normalized_proposal

        # 3. Type Validation
        type_res = self.type_validator.validate(normalized_p, dataset_profiles)
        all_findings.extend(type_res.findings)

        # 4. Safety Validation (operational bounds & risk policies)
        safety_res = self.safety_validator.validate(normalized_p)
        all_findings.extend(safety_res.findings)

        is_valid = not any(f.severity == ValidationSeverity.ERROR for f in all_findings)
        return Tier3ValidationResult(
            valid=is_valid,
            findings=all_findings,
            normalized_proposal=normalized_p if is_valid else None,
            proposal_fingerprint=normalized_p.fingerprint,
        )

    def validate_batch(
        self,
        proposals: List[Tier3ResolutionProposal],
        dataset_profiles: Dict[str, DatasetProfile],
    ) -> Dict[str, Tier3ValidationResult]:
        results: Dict[str, Tier3ValidationResult] = {}
        normalized_valid: List[Tier3ResolutionProposal] = []

        for p in proposals:
            res = self.validate_proposal(p, dataset_profiles)
            results[p.proposal_id] = res
            if res.valid and res.normalized_proposal:
                normalized_valid.append(res.normalized_proposal)

        # 4. Cross-proposal Conflict Detection on valid candidates
        if len(normalized_valid) >= 2:
            conflict_res = self.conflict_detector.validate_proposals(normalized_valid)
            for finding in conflict_res.findings:
                if finding.proposal_id and finding.proposal_id in results:
                    p_res = results[finding.proposal_id]
                    p_res.findings.append(finding)
                    if finding.severity == ValidationSeverity.ERROR:
                        p_res.valid = False
                        p_res.normalized_proposal = None
                else:
                    # Apply conflict to all affected proposals
                    for p in normalized_valid:
                        results[p.proposal_id].findings.append(finding)
                        if finding.severity == ValidationSeverity.ERROR:
                            results[p.proposal_id].valid = False
                            results[p.proposal_id].normalized_proposal = None

        return results
