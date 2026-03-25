from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Protocol


@dataclass(frozen=True)
class CohortMetricPoint:
    day: date
    cohort: str
    value: Decimal


class CohortAnalyticsPlugin(Protocol):
    """Extension point for future cohort analytics modules."""

    name: str

    def compute_for_day(self, day: date) -> list[CohortMetricPoint]:
        ...


# TODO(phase3): implement age-band distribution plugin.
# TODO(phase3): implement LTH/STH approximation plugin.
# TODO(phase3): implement miner heuristic plugin.
# TODO(phase3): implement entity clustering plugin.
