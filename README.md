# pyspark-quarantine

[![CI](https://github.com/thisismairaj/pyspark-quarantine/actions/workflows/ci.yml/badge.svg)](https://github.com/thisismairaj/pyspark-quarantine/actions/workflows/ci.yml)
![License](https://img.shields.io/badge/license-MIT-blue)
![Python](https://img.shields.io/badge/python-3.9%2B-blue)

Split a PySpark DataFrame into **valid** and **quarantined** rows, with a
reason recorded for every rejected row, plus a small schema-drift detector.
Pure PySpark - `pip install pyspark pytest` is enough to run the test suite,
no Databricks or Snowflake account needed.

## Why this exists

The bronze → silver → **quarantine** → gold pattern in my other repos
([data-lab2](https://github.com/thisismairaj/data-lab2),
[fhir-dataset](https://github.com/thisismairaj/fhir-dataset),
[dbt-sales-medallion](https://github.com/thisismairaj/dbt-sales-medallion))
hand-rolls the same "split rows, keep a reason" SQL every time - and not
even the same way twice. `data-lab2` writes one quarantine row per bad
record with every failed rule id joined together; `dbt-sales-medallion`
uses a `UNION ALL` of one row per reason, so a row failing two checks shows
up twice and the quarantine count stops matching the number of bad rows.
This pulls the pattern into a small, tested, reusable library and settles
on one behavior: a row is quarantined once, with every reason it failed.

`detect_schema_drift` is the same idea for schemas: it's what
`data-lab2`'s hand-written rename/drop drift tests were checking for,
generalized into a reusable diff.

## Install

```bash
pip install -e ".[test]"
```

## Usage

```python
from pyspark_quarantine import Check, quarantine

checks = [
    Check(
        name="qty_positive",
        predicate=lambda df: df.quantity > 0,
        reason="quantity must be positive",
    ),
    Check(
        name="customer_present",
        predicate=lambda df: df.customer_id.isNotNull(),
        reason="customer_id is required",
    ),
]

result = quarantine(orders_df, checks)
result.valid           # rows that passed every check
result.quarantined     # rejected rows + a "_quarantine_reasons" array column
```

```python
from pyspark_quarantine import detect_schema_drift

drift = detect_schema_drift(incoming_df, expected_schema)
if drift.has_drift:
    print(drift.added, drift.removed, drift.type_changed)
```

## Testing

```bash
pytest -v
```

Runs against a local `SparkSession` (`local[2]`) - no cluster needed. CI
(`.github/workflows/ci.yml`) runs the same suite on every push with Java 17 +
Python 3.11; see the badge above for the current status.

## Status

API covers row-level quarantine (multi-check, multi-reason) and schema-drift
detection, both with passing unit tests in CI. Not yet covered: an SCD2/Delta
merge helper - `dbt-sales-medallion`'s snapshot already demonstrates that
pattern, so it wasn't worth duplicating here for v1.
