from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from pyspark_quarantine import Check, quarantine


def test_valid_rows_pass_through(spark):
    df = spark.createDataFrame([(1, 5), (2, -1), (3, 0)], ["id", "quantity"])
    checks = [
        Check(
            name="qty_positive",
            predicate=lambda d: d.quantity > 0,
            reason="quantity must be positive",
        )
    ]

    result = quarantine(df, checks)

    assert [r.id for r in result.valid.collect()] == [1]
    assert sorted(r.id for r in result.quarantined.collect()) == [2, 3]


def test_row_failing_two_checks_gets_both_reasons(spark):
    # Explicit schema: a single-row, all-null customer_id column can't have
    # its type inferred from the data alone.
    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("quantity", IntegerType()),
            StructField("customer_id", StringType()),
        ]
    )
    df = spark.createDataFrame([(1, -1, None)], schema)
    checks = [
        Check(
            name="qty_positive",
            predicate=lambda d: d.quantity > 0,
            reason="quantity must be positive",
        ),
        Check(
            name="customer_present",
            predicate=lambda d: d.customer_id.isNotNull(),
            reason="customer_id is required",
        ),
    ]

    result = quarantine(df, checks)

    reasons = result.quarantined.collect()[0]["_quarantine_reasons"]
    assert set(reasons) == {"quantity must be positive", "customer_id is required"}
    assert result.valid.count() == 0


def test_no_checks_returns_everything_valid(spark):
    df = spark.createDataFrame([(1,), (2,)], ["id"])

    result = quarantine(df, [])

    assert result.valid.count() == 2
    assert result.quarantined.count() == 0


def test_custom_reason_column_name(spark):
    df = spark.createDataFrame([(1, -1)], ["id", "quantity"])
    checks = [
        Check(
            name="qty_positive",
            predicate=lambda d: d.quantity > 0,
            reason="quantity must be positive",
        )
    ]

    result = quarantine(df, checks, reason_col="why_rejected")

    assert "why_rejected" in result.quarantined.columns
    assert "why_rejected" not in result.valid.columns
