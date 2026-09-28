from __future__ import annotations


class Tier3Error(Exception):
    """Base exception for all Tier 3 structured resolution operations."""
    pass


class Tier3ContractError(Tier3Error):
    """Raised when a proposal violates contract specifications or raw SQL prohibitions."""
    pass


class Tier3ValidationError(Tier3Error):
    """Raised when proposal fails schema, type, safety, or conflict validation."""
    pass


class Tier3ApprovalError(Tier3Error):
    """Raised when an approval gate check fails or approval record is invalid/stale/unauthorized."""
    pass


class Tier3IntegrationError(Tier3Error):
    """Raised when a proposal cannot be converted to a StructuredAction or integrated into compiler."""
    pass


class Tier3RenderingError(Tier3Error):
    """Raised when deterministic SQL AST rendering fails."""
    pass
