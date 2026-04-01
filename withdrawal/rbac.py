"""RBAC and audit trail utilities for the withdrawal service."""

import uuid
from datetime import datetime, timezone

ROLE_PERMISSIONS = {
    "admin": ["approve_withdrawal", "view_transactions"],
    "system": ["publish_events", "reconcile"],
    "signer": ["sign_transaction"],
}

VALID_TRANSITIONS = {
    None: ["pending_policy"],
    "pending_policy": ["approved", "rejected"],
    "approved": ["signed", "failed"],
    "signed": ["broadcast", "failed"],
    "broadcast": ["confirmed", "failed"],
    "rejected": [],
    "confirmed": [],
    "failed": [],
}


def check_permission(actor_role, required_permission):
    """Raise PermissionError if role lacks the required permission."""
    perms = ROLE_PERMISSIONS.get(actor_role, [])
    if required_permission not in perms:
        raise PermissionError(
            f"Role '{actor_role}' lacks permission "
            f"'{required_permission}'"
        )


def validate_transition(current_status, new_status):
    """Python-side pre-validation (defense-in-depth with DB trigger).

    Raises ValueError on invalid transitions.
    """
    allowed = VALID_TRANSITIONS.get(current_status, [])
    if new_status not in allowed:
        raise ValueError(
            f"Invalid transition: {current_status} -> {new_status}"
        )


async def insert_audit_event(
    conn, actor_role, actor_id, action,
    resource_type, resource_id,
    request_id=None, trace_id=None, metadata=None,
):
    """Append a row to the audit_events table."""
    await conn.execute(
        "INSERT INTO audit_events "
        "(request_id, trace_id, actor_role, actor_id, "
        "action, resource_type, resource_id, metadata, "
        "created_at) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        request_id,
        trace_id,
        actor_role,
        actor_id,
        action,
        resource_type,
        resource_id,
        metadata or "{}",
        datetime.now(tz=timezone.utc),
    )
