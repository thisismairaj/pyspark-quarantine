from dataclasses import dataclass
from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


@dataclass(frozen=True)
class SchemaDrift:
    added: list[str]
    removed: list[str]
    type_changed: list[tuple[str, str, str]]  # (column, expected_type, actual_type)

    @property
    def has_drift(self) -> bool:
        return bool(self.added or self.removed or self.type_changed)


def detect_schema_drift(
    actual: Union[DataFrame, StructType], expected: StructType
) -> SchemaDrift:
    """Compare an actual schema against an expected one: columns added,
    columns removed, and columns that kept their name but changed type."""
    actual_schema = actual.schema if isinstance(actual, DataFrame) else actual

    actual_fields = {f.name: f.dataType.simpleString() for f in actual_schema.fields}
    expected_fields = {f.name: f.dataType.simpleString() for f in expected.fields}

    added = sorted(set(actual_fields) - set(expected_fields))
    removed = sorted(set(expected_fields) - set(actual_fields))
    type_changed = sorted(
        (name, expected_fields[name], actual_fields[name])
        for name in (set(actual_fields) & set(expected_fields))
        if actual_fields[name] != expected_fields[name]
    )

    return SchemaDrift(added=added, removed=removed, type_changed=type_changed)
