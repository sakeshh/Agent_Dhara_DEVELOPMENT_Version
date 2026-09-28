from __future__ import annotations

from typing import Dict, List, Optional
from agent.compiler.keys.key_candidate import KeyCandidate, KeyResolution, KeySource
from agent.compiler.keys.key_policy import KeyPolicy
from agent.compiler.models.profile import DatasetProfile


class BusinessKeyResolver:
    """
    Universal Business Key Resolver implementing the strict 7-level priority order:
    1. User-approved key
    2. Data contract primary key
    3. Database primary-key constraint
    4. Unique database index
    5. Previously approved knowledge-base key
    6. High-confidence inferred key
    7. Human approval required
    """

    def __init__(self, knowledge_store=None):
        self.knowledge_store = knowledge_store

    @classmethod
    def resolve_key(
        cls_or_self,
        dataset_name: str,
        profile: Optional[DatasetProfile | Dict[str, Any]] = None,
        user_approved_keys: Optional[List[str]] = None,
        data_contract_keys: Optional[List[str]] = None,
        db_primary_keys: Optional[List[str]] = None,
        unique_index_keys: Optional[List[str]] = None,
    ) -> KeyResolution:
        knowledge_store = getattr(cls_or_self, "knowledge_store", None)

        if isinstance(profile, dict):
            if "columns" in profile and isinstance(profile["columns"], dict):
                cols_dict = profile["columns"]
                row_count = int(profile.get("row_count") or 100)
            else:
                cols_dict = profile
                row_count = 100
            from agent.compiler.models.profile import ColumnProfile
            col_profiles = {}
            for col_name, c_info in cols_dict.items():
                if isinstance(c_info, ColumnProfile):
                    col_profiles[col_name] = c_info
                elif isinstance(c_info, dict):
                    col_profiles[col_name] = ColumnProfile(
                        name=col_name,
                        dtype=c_info.get("dtype") or c_info.get("type") or "string",
                        null_count=int(c_info.get("null_count") or (c_info.get("null_ratio", 0) * row_count)),
                        unique_count=int(c_info.get("unique_count") or c_info.get("distinct_count") or (c_info.get("distinct_ratio", 1.0) * row_count)),
                    )
                else:
                    col_profiles[col_name] = ColumnProfile(name=col_name, dtype="string")
            profile = DatasetProfile(dataset_name=dataset_name, columns=col_profiles, row_count=row_count)

        # Priority 1: User-approved key
        if user_approved_keys:
            return KeyResolution(
                columns=user_approved_keys,
                source=KeySource.USER_APPROVED,
                confidence=100.0,
                approved=True,
                evidence=["Explicit user approval provided in UI/API"],
            )

        # Priority 2: Data contract primary key
        if data_contract_keys:
            return KeyResolution(
                columns=data_contract_keys,
                source=KeySource.DATA_CONTRACT,
                confidence=100.0,
                approved=True,
                evidence=["Explicit data contract specification"],
            )

        # Priority 3: Database primary key constraint
        if db_primary_keys:
            return KeyResolution(
                columns=db_primary_keys,
                source=KeySource.DATABASE_PK,
                confidence=99.0,
                approved=True,
                evidence=["Authoritative database PRIMARY KEY constraint"],
            )

        # Priority 4: Unique database index
        if unique_index_keys:
            return KeyResolution(
                columns=unique_index_keys,
                source=KeySource.UNIQUE_INDEX,
                confidence=95.0,
                approved=True,
                evidence=["Database UNIQUE INDEX constraint"],
            )

        # Priority 5: Historical Knowledge Base
        if knowledge_store:
            pattern = knowledge_store.get_approved_key_pattern(dataset_name)
            if pattern:
                return KeyResolution(
                    columns=pattern.columns,
                    source=KeySource.KNOWLEDGE_BASE,
                    confidence=92.0,
                    approved=False,
                    evidence=["Previously verified business key pattern from Enterprise Knowledge Base"],
                )

        # Priority 6: High-confidence inference from DatasetProfile
        candidates: List[KeyCandidate] = []
        if profile and profile.columns:
            total_rows = max(1, profile.row_count)
            for col_name, cmeta in profile.columns.items():
                n_count = getattr(cmeta, "null_count", 0)
                u_count = getattr(cmeta, "distinct_count", getattr(cmeta, "unique_count", total_rows))
                null_ratio = n_count / total_rows
                uniqueness_ratio = u_count / total_rows
                has_id_name = any(
                    col_name.lower().endswith(x) or col_name.lower() == x
                    for x in ("_id", "id", "_key", "key", "sku", "code", "num", "number")
                )

                confidence, evidence, warnings = KeyPolicy.calculate_score(
                    col_name=col_name,
                    null_ratio=null_ratio,
                    uniqueness_ratio=uniqueness_ratio,
                    has_id_naming=has_id_name,
                    is_database_pk=False,
                )

                candidates.append(
                    KeyCandidate(
                        columns=[col_name],
                        source=KeySource.INFERRED,
                        confidence=confidence,
                        null_ratio=null_ratio,
                        uniqueness_ratio=uniqueness_ratio,
                        evidence=evidence,
                        warnings=warnings,
                    )
                )

        # Pick best candidate
        if candidates:
            candidates.sort(key=lambda c: c.confidence, reverse=True)
            best = candidates[0]
            if best.confidence >= 95.0:
                return KeyResolution(
                    columns=best.columns,
                    source=KeySource.INFERRED,
                    confidence=best.confidence,
                    approved=True,
                    evidence=best.evidence,
                    warnings=best.warnings,
                )
            elif best.confidence >= 80.0:
                return KeyResolution(
                    columns=best.columns,
                    source=KeySource.INFERRED,
                    confidence=best.confidence,
                    approved=False,
                    evidence=best.evidence,
                    warnings=best.warnings + ["Key recommendation requires user approval before destructive action"],
                )

        # Priority 7: Human Approval Required
        return KeyResolution(
            columns=[],
            source=KeySource.HUMAN_APPROVAL_PENDING,
            confidence=0.0,
            approved=False,
            warnings=["No authoritative or high-confidence business key could be determined automatically."],
        )
