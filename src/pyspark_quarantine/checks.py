from dataclasses import dataclass
from typing import Callable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class Check:
    """One row-level rule. `predicate` takes the whole DataFrame (so it can
    reference more than one column) and returns a boolean Column that is
    True for rows that PASS this check."""

    name: str
    predicate: Callable[[DataFrame], Column]
    reason: str


@dataclass(frozen=True)
class QuarantineResult:
    valid: DataFrame
    quarantined: DataFrame


def quarantine(
    df: DataFrame,
    checks: list[Check],
    reason_col: str = "_quarantine_reasons",
) -> QuarantineResult:
    """Split df into (valid, quarantined) against every check at once.

    A row that fails more than one check is quarantined exactly once, with
    every reason it failed collected into a `reason_col` array column - no
    row is silently dropped, and no row is duplicated across reasons.
    """
    if not checks:
        empty = df.sparkSession.createDataFrame([], df.schema)
        return QuarantineResult(valid=df, quarantined=empty)

    failure_exprs = [
        F.when(~check.predicate(df), F.lit(check.reason)) for check in checks
    ]
    reasons = F.filter(F.array(*failure_exprs), lambda r: r.isNotNull())
    tagged = df.withColumn(reason_col, reasons)

    quarantined = tagged.where(F.size(F.col(reason_col)) > 0)
    valid = tagged.where(F.size(F.col(reason_col)) == 0).drop(reason_col)

    return QuarantineResult(valid=valid, quarantined=quarantined)
