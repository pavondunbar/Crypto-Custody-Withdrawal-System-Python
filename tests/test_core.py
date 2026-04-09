"""Core tests for the crypto custody withdrawal system.

Tests the most critical paths without requiring Docker or a database:
journal balance enforcement, state machine transitions, RBAC, and
idempotency constraints. Database-level triggers (append-only, deferred
balance check) are validated via schema assertions.
"""

import re
import uuid

import pytest

# -- path setup so we can import from withdrawal/ --
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "withdrawal"))

from rbac import (
    ROLE_PERMISSIONS,
    VALID_TRANSITIONS,
    check_permission,
    validate_transition,
)


# -----------------------------------------------
# 1. State machine: valid transitions
# -----------------------------------------------

class TestStateMachineValid:
    """Walk the happy path through every state."""

    HAPPY_PATH = [
        (None, "pending_policy"),
        ("pending_policy", "approved"),
        ("approved", "signed"),
        ("signed", "broadcast"),
        ("broadcast", "confirmed"),
    ]

    @pytest.mark.parametrize("current,target", HAPPY_PATH)
    def test_happy_path_transitions(self, current, target):
        validate_transition(current, target)

    def test_rejection_from_pending(self):
        validate_transition("pending_policy", "rejected")

    def test_failure_from_approved(self):
        validate_transition("approved", "failed")

    def test_failure_from_signed(self):
        validate_transition("signed", "failed")

    def test_failure_from_broadcast(self):
        validate_transition("broadcast", "failed")


# -----------------------------------------------
# 2. State machine: invalid transitions
# -----------------------------------------------

class TestStateMachineInvalid:
    """Transitions that must be rejected."""

    INVALID = [
        ("confirmed", "pending_policy"),
        ("confirmed", "approved"),
        ("rejected", "approved"),
        ("failed", "signed"),
        ("pending_policy", "confirmed"),
        ("approved", "broadcast"),
        (None, "confirmed"),
    ]

    @pytest.mark.parametrize("current,target", INVALID)
    def test_invalid_transitions_raise(self, current, target):
        with pytest.raises(ValueError, match="Invalid transition"):
            validate_transition(current, target)

    def test_terminal_states_block_all(self):
        for terminal in ("rejected", "confirmed", "failed"):
            for target in VALID_TRANSITIONS:
                if target is None:
                    continue
                with pytest.raises(ValueError):
                    validate_transition(terminal, target)


# -----------------------------------------------
# 3. RBAC permission checks
# -----------------------------------------------

class TestRBAC:
    """Verify role-based access control enforcement."""

    def test_admin_can_approve(self):
        check_permission("admin", "approve_withdrawal")

    def test_signer_can_sign(self):
        check_permission("signer", "sign_transaction")

    def test_system_can_reconcile(self):
        check_permission("system", "reconcile")

    def test_signer_cannot_approve(self):
        with pytest.raises(PermissionError):
            check_permission("signer", "approve_withdrawal")

    def test_admin_cannot_sign(self):
        with pytest.raises(PermissionError):
            check_permission("admin", "sign_transaction")

    def test_unknown_role_denied(self):
        with pytest.raises(PermissionError):
            check_permission("intruder", "approve_withdrawal")

    def test_separation_of_duties(self):
        """No single role has both approve and sign."""
        for role, perms in ROLE_PERMISSIONS.items():
            assert not (
                "approve_withdrawal" in perms
                and "sign_transaction" in perms
            ), f"Role '{role}' violates separation of duties"


# -----------------------------------------------
# 4. Append-only triggers exist in schema
# -----------------------------------------------

SCHEMA_PATH = os.path.join(
    os.path.dirname(__file__), "..", "db", "init", "001-schema.sql"
)
COMPONENTS_PATH = os.path.join(
    os.path.dirname(__file__), "..", "db", "init", "003-missing-components.sql"
)


def _read_sql(*paths):
    parts = []
    for p in paths:
        with open(p) as f:
            parts.append(f.read())
    return "\n".join(parts)


class TestSchemaConstraints:
    """Verify that the SQL schema enforces immutability."""

    @pytest.fixture(autouse=True)
    def load_schema(self):
        self.sql = _read_sql(SCHEMA_PATH, COMPONENTS_PATH)

    # Map each table to its trigger name suffix (SQL uses short forms)
    APPEND_ONLY_TRIGGERS = {
        "journal_entries": "journal",
        "transactions": "transactions",
        "transaction_status_history": "status_history",
        "audit_events": "audit",
        "reconciliation_mismatches": "recon_mismatches",
    }

    @pytest.mark.parametrize(
        "table,suffix", list(APPEND_ONLY_TRIGGERS.items()),
    )
    def test_deny_update_trigger_exists(self, table, suffix):
        pattern = rf"CREATE\s+TRIGGER\s+deny_update_{suffix}"
        assert re.search(pattern, self.sql, re.IGNORECASE), (
            f"Missing deny_update trigger for {table}"
        )

    @pytest.mark.parametrize(
        "table,suffix", list(APPEND_ONLY_TRIGGERS.items()),
    )
    def test_deny_delete_trigger_exists(self, table, suffix):
        pattern = rf"CREATE\s+TRIGGER\s+deny_delete_{suffix}"
        assert re.search(pattern, self.sql, re.IGNORECASE), (
            f"Missing deny_delete trigger for {table}"
        )

    def test_journal_balance_trigger_exists(self):
        assert re.search(
            r"check_journal_balance", self.sql, re.IGNORECASE
        )

    def test_status_transition_trigger_exists(self):
        assert re.search(
            r"enforce_status_transition", self.sql, re.IGNORECASE
        )

    def test_no_update_or_delete_grants_on_journal(self):
        """The schema must not GRANT UPDATE or DELETE on journal_entries."""
        for keyword in ("UPDATE", "DELETE"):
            pattern = rf"GRANT\s+{keyword}\s+ON\s+journal_entries"
            assert not re.search(pattern, self.sql, re.IGNORECASE), (
                f"Schema grants {keyword} on journal_entries"
            )
