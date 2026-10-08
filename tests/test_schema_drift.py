from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from pyspark_quarantine import detect_schema_drift


def test_detects_added_removed_and_type_changed_columns():
    expected = StructType(
        [
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("removed_col", StringType()),
        ]
    )
    actual = StructType(
        [
            StructField("id", LongType()),  # type changed: int -> bigint
            StructField("name", StringType()),  # unchanged
            StructField("added_col", StringType()),  # added
            # removed_col is missing -> removed
        ]
    )

    drift = detect_schema_drift(actual, expected)

    assert drift.added == ["added_col"]
    assert drift.removed == ["removed_col"]
    assert drift.type_changed == [("id", "int", "bigint")]
    assert drift.has_drift is True


def test_no_drift_when_schemas_match():
    schema = StructType([StructField("id", IntegerType())])

    drift = detect_schema_drift(schema, schema)

    assert drift.has_drift is False
    assert drift.added == []
    assert drift.removed == []
    assert drift.type_changed == []


def test_accepts_a_dataframe_directly(spark):
    # Explicit schema on creation - createDataFrame would otherwise infer
    # "id" as a bigint from the Python int, not the IntegerType() below.
    schema = StructType(
        [StructField("id", IntegerType()), StructField("name", StringType())]
    )
    df = spark.createDataFrame([(1, "a")], schema)

    drift = detect_schema_drift(df, schema)

    assert drift.has_drift is False
