from __future__ import annotations
import time
import threading
from typing import Any, Dict, List, Optional

class TelemetryCounter:
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
        self._value = 0
        self._lock = threading.Lock()

    def inc(self, value: int = 1) -> None:
        with self._lock:
            self._value += value

    @property
    def value(self) -> int:
        return self._value

class TelemetryHistogram:
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
        self._values: List[float] = []
        self._lock = threading.Lock()

    def observe(self, amount: float) -> None:
        with self._lock:
            self._values.append(amount)

    @property
    def count(self) -> int:
        return len(self._values)

    @property
    def sum(self) -> float:
        return sum(self._values)

    @property
    def avg(self) -> float:
        with self._lock:
            if not self._values:
                return 0.0
            return sum(self._values) / len(self._values)

# Global Telemetry Registry
class TelemetryRegistry:
    def __init__(self):
        self._lock = threading.Lock()
        self.counters: Dict[str, TelemetryCounter] = {}
        self.histograms: Dict[str, TelemetryHistogram] = {}

        # Register standard P12 metrics
        self.register_counter("dhara_assessment_runs_total", "Total assessment runs executed.")
        self.register_counter("dhara_assessment_failures_total", "Total failed assessment runs.")
        self.register_counter("dhara_datasets_processed_total", "Total datasets processed during execution.")
        self.register_counter("dhara_rows_scanned_total", "Total number of rows scanned across datasets.")
        self.register_counter("dhara_rules_evaluated_total", "Total number of rules/assertions evaluated.")
        self.register_counter("dhara_rules_skipped_total", "Total rules skipped during profiling.")
        self.register_counter("dhara_gating_issues_total", "Total gating issues identified.")
        self.register_counter("dhara_advisory_issues_total", "Total advisory/non-gating issues identified.")
        self.register_counter("dhara_child_orphan_rows_total", "Total referential integrity child orphan rows detected.")
        self.register_counter("dhara_unknown_readiness_total", "Total times an UNKNOWN readiness status is yielded.")
        self.register_counter("dhara_score_reconciliation_failures_total", "Total score ledger reconciliation failures.")
        self.register_counter("dhara_report_generation_failures_total", "Total failures during report rendering.")
        self.register_counter("dhara_remediation_actions_total", "Total remediation modifications performed.")
        
        self.register_histogram("dhara_assessment_duration_seconds", "Assessment run durations in seconds.")

    def register_counter(self, name: str, description: str) -> TelemetryCounter:
        with self._lock:
            if name not in self.counters:
                self.counters[name] = TelemetryCounter(name, description)
            return self.counters[name]

    def register_histogram(self, name: str, description: str) -> TelemetryHistogram:
        with self._lock:
            if name not in self.histograms:
                self.histograms[name] = TelemetryHistogram(name, description)
            return self.histograms[name]

    def inc_counter(self, name: str, value: int = 1) -> None:
        if name in self.counters:
            self.counters[name].inc(value)

    def observe_histogram(self, name: str, amount: float) -> None:
        if name in self.histograms:
            self.histograms[name].observe(amount)

    def get_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "counters": {k: v.value for k, v in self.counters.items()},
                "histograms": {
                    k: {"count": v.count, "sum": round(v.sum, 4), "avg": round(v.avg, 4)}
                    for k, v in self.histograms.items()
                }
            }

# Global singleton
metrics_registry = TelemetryRegistry()
