from __future__ import annotations

import pytest

from agent.compiler.governance.reconciliation import ReconciliationLedger, ReconciliationResult
from agent.errors import ReconciliationError


class TestReconciliationIdentity:
    """Verifies the row-identity and terminal-destination reconciliation model."""

    def test_flagged_rows_retained_in_clean_output_do_not_fail_reconciliation(self):
        """
        Critical test: 100 input rows -> 100 clean rows, where 10 rows are flagged and retained.
        Must NOT double-count to 110. Terminal accounted = 100, delta = 0.
        """
        res = ReconciliationLedger.reconcile(
            dataset_name="dbo.EmployeeMaster",
            source_distinct_rows=100,
            clean_distinct_rows=100,
            flagged_and_retained_distinct_rows=10,
            quarantined_distinct_rows=0,
            rejected_distinct_rows=0,
            quality_event_count=15,
        )

        assert res.is_reconciled is True
        assert res.source_distinct_rows == 100
        assert res.clean_distinct_rows == 100
        assert res.flagged_and_retained_distinct_rows == 10
        assert res.missing_source_row_count == 0
        assert res.duplicated_terminal_row_count == 0
        assert res.irreversibly_lost_row_count == 0

    def test_quarantined_and_clean_rows_reconciliation(self):
        """1000 input rows -> 990 clean rows (17 flagged and retained) + 10 quarantined rows."""
        res = ReconciliationLedger.reconcile(
            dataset_name="dbo.CustomerMaster",
            source_distinct_rows=1000,
            clean_distinct_rows=990,
            flagged_and_retained_distinct_rows=17,
            quarantined_distinct_rows=10,
            rejected_distinct_rows=0,
            quality_event_count=23,
        )

        assert res.is_reconciled is True
        assert res.source_distinct_rows == 1000
        assert res.clean_distinct_rows == 990
        assert res.quarantined_distinct_rows == 10
        assert res.missing_source_row_count == 0

        summary = res.to_summary_dict()
        assert summary["clean_distinct_rows"] == 990
        assert summary["flagged_and_retained_distinct_rows"] == 17
        assert summary["quarantined_distinct_rows"] == 10
        assert summary["is_reconciled"] is True

    def test_unaccounted_row_loss_raises_reconciliation_error(self):
        """100 input rows -> 95 accounted rows (5 rows lost without destination) raises ReconciliationError."""
        with pytest.raises(ReconciliationError) as exc_info:
            ReconciliationLedger.reconcile(
                dataset_name="dbo.EmployeeMaster",
                source_distinct_rows=100,
                clean_distinct_rows=95,
                quarantined_distinct_rows=0,
                rejected_distinct_rows=0,
                raise_on_mismatch=True,
            )

        assert "5 rows irreversibly lost" in str(exc_info.value)
