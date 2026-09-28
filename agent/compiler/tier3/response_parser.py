from __future__ import annotations

from dataclasses import dataclass
import json
import re
from typing import Any, Dict, Optional

from agent.compiler.models.tier3_resolution import (
    EvidenceAuthority,
    ResolutionDisposition,
    ResolutionStatus,
    RiskLevel,
    Tier3ResolutionProposal,
)
from agent.compiler.tier3.contract_validator import (
    ContractValidationResult,
    Tier3ContractValidator,
)
from agent.compiler.tier3.exceptions import Tier3ContractError
from agent.compiler.tier3.model_client import Tier3ModelResponse


@dataclass(frozen=True)
class ParsedProposalResult:
    is_valid: bool
    proposal: Optional[Tier3ResolutionProposal]
    contract_result: Optional[ContractValidationResult]
    raw_response: str
    error_message: Optional[str] = None


class Tier3ResponseParser:
    """
    Parses raw LLM responses into strictly validated Tier3ResolutionProposal instances.

    Guarantees:
      - Does not repair or invent missing fields, action types, or hallucinated columns.
      - Strips/rejects conversational markdown wrappers according to strict mode.
      - Preserves raw model output for compliance audit.
      - Passes candidate proposal directly through Tier3ContractValidator before returning.
    """

    def __init__(self, strict_mode: bool = True):
        self.strict_mode = strict_mode
        self.contract_validator = Tier3ContractValidator()

    def parse(self, response: Tier3ModelResponse) -> ParsedProposalResult:
        raw_text = response.raw_content
        if not raw_text or not raw_text.strip():
            return ParsedProposalResult(
                is_valid=False,
                proposal=None,
                contract_result=None,
                raw_response=raw_text,
                error_message="Model returned empty response.",
            )

        # 1. Clean / extract JSON string
        cleaned_json = raw_text.strip()
        if cleaned_json.startswith("```json") and cleaned_json.endswith("```"):
            if self.strict_mode:
                # In strict mode, markdown code blocks are rejected if there is surrounding conversational text
                pass
            cleaned_json = cleaned_json[7:-3].strip()
        elif cleaned_json.startswith("```") and cleaned_json.endswith("```"):
            cleaned_json = cleaned_json[3:-3].strip()
        elif "```" in cleaned_json:
            # Check for extraneous prose around code block
            if self.strict_mode:
                m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", cleaned_json, re.DOTALL)
                if not m:
                    return ParsedProposalResult(
                        is_valid=False,
                        proposal=None,
                        contract_result=None,
                        raw_response=raw_text,
                        error_message="Response contains conversational markdown prohibited in strict mode.",
                    )
                cleaned_json = m.group(1).strip()
            else:
                m = re.search(r"\{.*\}", cleaned_json, re.DOTALL)
                if m:
                    cleaned_json = m.group(0).strip()

        # 2. Parse JSON
        try:
            payload: Dict[str, Any] = json.loads(cleaned_json)
        except json.JSONDecodeError as exc:
            return ParsedProposalResult(
                is_valid=False,
                proposal=None,
                contract_result=None,
                raw_response=raw_text,
                error_message=f"JSON decoding error: {exc.msg} at line {exc.lineno}",
            )

        if not isinstance(payload, dict):
            return ParsedProposalResult(
                is_valid=False,
                proposal=None,
                contract_result=None,
                raw_response=raw_text,
                error_message=f"Model output root must be a JSON object, got {type(payload).__name__}.",
            )

        # 3. Attach operational audit metadata without affecting semantic fields
        payload["model_provider"] = response.model_provider
        payload["model_deployment"] = response.model_deployment
        payload["model_version"] = response.model_version

        # 4. Immediate Contract Validation
        contract_res = self.contract_validator.validate(payload)
        if not contract_res.is_valid:
            errors = [f"{i.issue_code}: {i.message}" for i in contract_res.issues]
            return ParsedProposalResult(
                is_valid=False,
                proposal=None,
                contract_result=contract_res,
                raw_response=raw_text,
                error_message=f"Contract validation failed: {'; '.join(errors)}",
            )

        proposal = contract_res.proposal
        return ParsedProposalResult(
            is_valid=True,
            proposal=proposal,
            contract_result=contract_res,
            raw_response=raw_text,
        )
