from __future__ import annotations
from typing import List

ALLOWED_CLEARTEXT_ROLES = {"data_steward", "administrator", "owner"}

def is_authorized_for_cleartext(roles: List[str]) -> bool:
    """
    Check if the user/role has permission to access unmasked cleartext diagnostic samples.
    """
    if not roles:
        return False
    user_roles = {str(r).lower().strip() for r in roles}
    return bool(user_roles & ALLOWED_CLEARTEXT_ROLES)
