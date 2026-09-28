from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, ConfigDict, Field


class RuleType(str, Enum):
    UNIQUE = "unique"
    NOT_NULL = "not_null"
    IS_NULL = "is_null"
    RANGE = "range"
    IN_SET = "in_set"
    NOT_IN_SET = "not_in_set"
    COMPARISON = "comparison"
    REGEX = "regex"
    LENGTH = "length"
    COMPOUND = "compound"


class ComparisonOperator(str, Enum):
    EQ = "eq"
    NEQ = "neq"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    LIKE = "like"


class CompoundLogicalOperator(str, Enum):
    AND = "and"
    OR = "or"
    NOT = "not"


class RuleExpression(BaseModel):
    rule_type: RuleType = Field(alias="type")
    column: Optional[str] = None
    target_table: Optional[str] = None

    # Range
    min_value: Optional[Union[int, float, str]] = None
    max_value: Optional[Union[int, float, str]] = None
    inclusive_min: bool = True
    inclusive_max: bool = True

    # In/Not-In Set
    allowed_values: Optional[List[Any]] = None
    forbidden_values: Optional[List[Any]] = None

    # Comparison
    operator: Optional[ComparisonOperator] = None
    compare_column: Optional[str] = None
    compare_value: Optional[Any] = None

    # Regex / String
    pattern: Optional[str] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None

    # Compound rules
    logical_operator: Optional[CompoundLogicalOperator] = None
    conditions: Optional[List[RuleExpression]] = None

    description: Optional[str] = None
    model_config = ConfigDict(populate_by_name=True)

    def to_sql_condition(self, dialect: str = "tsql") -> str:
        """Translates rule into deterministic SQL WHERE clause without eval()."""
        col = f"[{self.column}]" if self.column else ""

        if self.rule_type == RuleType.NOT_NULL:
            return f"{col} IS NOT NULL"
        elif self.rule_type == RuleType.IS_NULL:
            return f"{col} IS NULL"
        elif self.rule_type == RuleType.UNIQUE:
            # SQL unique check is usually done via window count or grouping
            return f"COUNT(*) OVER (PARTITION BY {col}) = 1"
        elif self.rule_type == RuleType.RANGE:
            clauses = []
            if self.min_value is not None:
                op = ">=" if self.inclusive_min else ">"
                val = f"'{self.min_value}'" if isinstance(self.min_value, str) else self.min_value
                clauses.append(f"{col} {op} {val}")
            if self.max_value is not None:
                op = "<=" if self.inclusive_max else "<"
                val = f"'{self.max_value}'" if isinstance(self.max_value, str) else self.max_value
                clauses.append(f"{col} {op} {val}")
            return " AND ".join(clauses) if clauses else "1=1"
        elif self.rule_type == RuleType.IN_SET and self.allowed_values:
            formatted = ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in self.allowed_values)
            return f"{col} IN ({formatted})"
        elif self.rule_type == RuleType.NOT_IN_SET and self.forbidden_values:
            formatted = ", ".join(f"'{v}'" if isinstance(v, str) else str(v) for v in self.forbidden_values)
            return f"{col} NOT IN ({formatted})"
        elif self.rule_type == RuleType.COMPARISON:
            op_map = {
                ComparisonOperator.EQ: "=",
                ComparisonOperator.NEQ: "<>",
                ComparisonOperator.GT: ">",
                ComparisonOperator.GTE: ">=",
                ComparisonOperator.LT: "<",
                ComparisonOperator.LTE: "<=",
                ComparisonOperator.LIKE: "LIKE",
            }
            sql_op = op_map.get(self.operator or ComparisonOperator.EQ, "=")
            if self.compare_column:
                return f"{col} {sql_op} [{self.compare_column}]"
            val = f"'{self.compare_value}'" if isinstance(self.compare_value, str) else self.compare_value
            return f"{col} {sql_op} {val}"
        elif self.rule_type == RuleType.LENGTH:
            len_func = "LEN" if dialect == "tsql" else "LENGTH"
            clauses = []
            if self.min_length is not None:
                clauses.append(f"{len_func}({col}) >= {self.min_length}")
            if self.max_length is not None:
                clauses.append(f"{len_func}({col}) <= {self.max_length}")
            return " AND ".join(clauses) if clauses else "1=1"
        elif self.rule_type == RuleType.COMPOUND and self.conditions:
            sub = [c.to_sql_condition(dialect) for c in self.conditions]
            joiner = f" {self.logical_operator.value.upper()} " if self.logical_operator else " AND "
            return f"({joiner.join(sub)})"
        return "1=1"

    def to_pyspark_condition(self) -> str:
        """Translates rule into deterministic PySpark col() expression string."""
        col = f"col('{self.column}')" if self.column else ""

        if self.rule_type == RuleType.NOT_NULL:
            return f"{col}.isNotNull()"
        elif self.rule_type == RuleType.IS_NULL:
            return f"{col}.isNull()"
        elif self.rule_type == RuleType.UNIQUE:
            return f"# Unique expectation on {self.column}"
        elif self.rule_type == RuleType.RANGE:
            clauses = []
            if self.min_value is not None:
                op = ">=" if self.inclusive_min else ">"
                clauses.append(f"({col} {op} {repr(self.min_value)})")
            if self.max_value is not None:
                op = "<=" if self.inclusive_max else "<"
                clauses.append(f"({col} {op} {repr(self.max_value)})")
            return " & ".join(clauses) if clauses else "lit(True)"
        elif self.rule_type == RuleType.IN_SET and self.allowed_values:
            return f"{col}.isin({self.allowed_values})"
        elif self.rule_type == RuleType.NOT_IN_SET and self.forbidden_values:
            return f"(~{col}.isin({self.forbidden_values}))"
        elif self.rule_type == RuleType.COMPARISON:
            op_map = {
                ComparisonOperator.EQ: "==",
                ComparisonOperator.NEQ: "!=",
                ComparisonOperator.GT: ">",
                ComparisonOperator.GTE: ">=",
                ComparisonOperator.LT: "<",
                ComparisonOperator.LTE: "<=",
            }
            py_op = op_map.get(self.operator or ComparisonOperator.EQ, "==")
            if self.compare_column:
                return f"({col} {py_op} col('{self.compare_column}'))"
            return f"({col} {py_op} {repr(self.compare_value)})"
        elif self.rule_type == RuleType.COMPOUND and self.conditions:
            sub = [c.to_pyspark_condition() for c in self.conditions]
            joiner = " & " if self.logical_operator == CompoundLogicalOperator.AND else " | "
            return f"({joiner.join(sub)})"
        return "lit(True)"

    def to_gx_expectation(self) -> Dict[str, Any]:
        """Translates rule into Great Expectations format."""
        if self.rule_type == RuleType.UNIQUE:
            return {
                "expectation_type": "expect_column_values_to_be_unique",
                "kwargs": {"column": self.column},
            }
        elif self.rule_type == RuleType.NOT_NULL:
            return {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": self.column},
            }
        elif self.rule_type == RuleType.RANGE:
            return {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": self.column,
                    "min_value": self.min_value,
                    "max_value": self.max_value,
                },
            }
        elif self.rule_type == RuleType.IN_SET:
            return {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {"column": self.column, "value_set": self.allowed_values or []},
            }
        return {
            "expectation_type": "expect_column_values_to_match_regex",
            "kwargs": {"column": self.column, "regex": self.pattern or ".*"},
        }
