from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from agent.compiler.models.profile import DatasetProfile
from agent.compiler.models.tier3_resolution import Tier3ResolutionProposal
from agent.compiler.tier3.cache import Tier3ReplayCache
from agent.compiler.tier3.model_client import Tier3ModelClient, Tier3ResolutionRequest
from agent.compiler.tier3.prompt_builder import Tier3PromptBuilder
from agent.compiler.tier3.response_parser import Tier3ResponseParser


@dataclass(frozen=True)
class ProposalGenerationResult:
    """
    Result of an orchestrated proposal generation attempt.
    Separates transport/syntax failures from semantic validation results.
    """
    success: bool
    proposal: Optional[Tier3ResolutionProposal] = None
    from_cache: bool = False
    attempts: int = 1
    status: str = "SUCCESS"  # SUCCESS | MODEL_UNAVAILABLE | INVALID_PROPOSAL | REJECTED
    error_message: Optional[str] = None
    raw_response: Optional[str] = None


class Tier3ProposalService:
    """
    Orchestrates Tier 3 proposal generation with:
      - Deterministic replay caching (with live active schema re-validation before reuse)
      - Isolated prompt formatting
      - Bounded retry policy (retries allowed ONLY for transient transport or syntax/JSON truncation)
      - Strict prohibition on retrying semantic failures (unknown action, unknown column, bad disposition)
      - Strict preservation of raw model responses for audit lineage
    """

    def __init__(
        self,
        client: Tier3ModelClient,
        prompt_builder: Optional[Tier3PromptBuilder] = None,
        parser: Optional[Tier3ResponseParser] = None,
        cache: Optional[Tier3ReplayCache] = None,
        max_retries: int = 1,
    ):
        self.client = client
        self.prompt_builder = prompt_builder or Tier3PromptBuilder()
        self.parser = parser or Tier3ResponseParser(strict_mode=True)
        self.cache = cache or Tier3ReplayCache()
        self.max_retries = max(0, max_retries)

    def generate_proposal(
        self,
        request: Tier3ResolutionRequest,
        dataset_profiles: Dict[str, DatasetProfile],
    ) -> ProposalGenerationResult:
        # 1. Check Replay Cache First
        if self.cache is not None:
            cached = self.cache.get(request, dataset_profiles)
            if cached is not None:
                return ProposalGenerationResult(
                    success=True,
                    proposal=cached,
                    from_cache=True,
                    attempts=0,
                    status="SUCCESS",
                )

        # 2. Invoke Model Client with Bounded Retries
        attempts = 0
        last_error: Optional[str] = None
        last_raw_response: Optional[str] = None

        while attempts <= self.max_retries:
            attempts += 1
            response = self.client.propose_resolution(request)
            last_raw_response = response.raw_content

            # Transient transport or timeout errors -> retryable
            if response.status in ("TIMEOUT", "UNAVAILABLE"):
                last_error = response.error_message or "Model service connection timed out or unavailable."
                continue

            if response.status == "RETRYABLE_ERROR":
                last_error = response.error_message or "Transient rate limit or server error."
                continue

            # Parse Structured Output
            parsed_result = self.parser.parse(response)
            if not parsed_result.is_valid:
                err_msg = parsed_result.error_message or "Invalid model response."
                last_error = err_msg

                # Truncated or malformed JSON syntax is allowed to be retried within budget
                if "JSON decoding error" in err_msg:
                    continue

                # Any semantic, contract, unknown action, or schema failure is STRICTLY NOT RETRIABLE
                return ProposalGenerationResult(
                    success=False,
                    proposal=None,
                    from_cache=False,
                    attempts=attempts,
                    status="INVALID_PROPOSAL",
                    error_message=err_msg,
                    raw_response=last_raw_response,
                )

            # Successfully parsed and validated contract
            proposal = parsed_result.proposal
            if proposal is not None and self.cache is not None:
                self.cache.put(request, proposal)

            return ProposalGenerationResult(
                success=True,
                proposal=proposal,
                from_cache=False,
                attempts=attempts,
                status="SUCCESS",
                raw_response=last_raw_response,
            )

        # Max retries exhausted for transport or syntax issues
        return ProposalGenerationResult(
            success=False,
            proposal=None,
            from_cache=False,
            attempts=attempts,
            status="MODEL_UNAVAILABLE",
            error_message=f"Exhausted retries ({attempts}). Last error: {last_error}",
            raw_response=last_raw_response,
        )
