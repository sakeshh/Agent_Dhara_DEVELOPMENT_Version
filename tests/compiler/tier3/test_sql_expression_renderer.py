from __future__ import annotations

import pytest

from agent.compiler.models.tier3_resolution import (
    AstNodeType,
    BetweenExpression,
    BooleanExpression,
    BooleanOperator,
    ColumnRef,
    ComparisonExpression,
    ComparisonOperator,
    InListExpression,
    LikeExpression,
    LiteralValue,
    NullCheckExpression,
)
from agent.compiler.renderers.sql_expression_renderer import SqlExpressionRenderer
from agent.compiler.tier3.exceptions import Tier3RenderingError


@pytest.fixture
def renderer() -> SqlExpressionRenderer:
    return SqlExpressionRenderer()


def test_render_column_unqualified(renderer: SqlExpressionRenderer):
    col = ColumnRef(dataset="", column="Salary")
    sql = renderer.render(col)
    assert sql == "[Salary]"


def test_render_column_qualified(renderer: SqlExpressionRenderer):
    col = ColumnRef(dataset="dbo.Employees", column="Salary")
    sql = renderer.render(col)
    assert sql == "[dbo].[Employees].[Salary]"


def test_render_literal_types(renderer: SqlExpressionRenderer):
    assert renderer.render(LiteralValue(data_type="INTEGER", value=42)) == "42"
    assert renderer.render(LiteralValue(data_type="DECIMAL", value=19.99)) == "19.99"
    assert renderer.render(LiteralValue(data_type="STRING", value="Active")) == "N'Active'"
    assert renderer.render(LiteralValue(data_type="STRING", value="O'Reilly")) == "N'O''Reilly'"
    assert renderer.render(LiteralValue(data_type="BOOLEAN", value=True)) == "1"
    assert renderer.render(LiteralValue(data_type="BOOLEAN", value=False)) == "0"
    assert renderer.render(LiteralValue(data_type="NULL", value=None)) == "NULL"


def test_render_comparison_expression(renderer: SqlExpressionRenderer):
    comp = ComparisonExpression(
        operator=ComparisonOperator.LT,
        left=ColumnRef(dataset="", column="Age"),
        right=LiteralValue(data_type="INTEGER", value=18),
    )
    sql = renderer.render(comp)
    assert sql == "[Age] < 18"


def test_render_comparison_ne_standardized(renderer: SqlExpressionRenderer):
    comp = ComparisonExpression(
        operator=ComparisonOperator.NE,
        left=ColumnRef(dataset="", column="Status"),
        right=LiteralValue(data_type="STRING", value="Terminated"),
    )
    sql = renderer.render(comp)
    assert sql == "[Status] <> N'Terminated'"


def test_render_null_check(renderer: SqlExpressionRenderer):
    is_null_node = NullCheckExpression(
        column=ColumnRef(dataset="", column="Email"),
        is_null=True,
    )
    assert renderer.render(is_null_node) == "[Email] IS NULL"

    not_null_node = NullCheckExpression(
        column=ColumnRef(dataset="", column="Email"),
        is_null=False,
    )
    assert renderer.render(not_null_node) == "[Email] IS NOT NULL"


def test_render_in_list(renderer: SqlExpressionRenderer):
    in_node = InListExpression(
        column=ColumnRef(dataset="", column="Department"),
        values=[
            LiteralValue(data_type="STRING", value="HR"),
            LiteralValue(data_type="STRING", value="Finance"),
            LiteralValue(data_type="STRING", value="IT"),
        ],
        negated=False,
    )
    assert renderer.render(in_node) == "[Department] IN (N'HR', N'Finance', N'IT')"

    not_in_node = InListExpression(
        column=ColumnRef(dataset="", column="Code"),
        values=[
            LiteralValue(data_type="INTEGER", value=1),
            LiteralValue(data_type="INTEGER", value=2),
        ],
        negated=True,
    )
    assert renderer.render(not_in_node) == "[Code] NOT IN (1, 2)"


def test_render_between(renderer: SqlExpressionRenderer):
    between_node = BetweenExpression(
        column=ColumnRef(dataset="", column="Score"),
        lower=LiteralValue(data_type="INTEGER", value=0),
        upper=LiteralValue(data_type="INTEGER", value=100),
        negated=False,
    )
    assert renderer.render(between_node) == "[Score] BETWEEN 0 AND 100"

    not_between_node = BetweenExpression(
        column=ColumnRef(dataset="", column="Score"),
        lower=LiteralValue(data_type="INTEGER", value=0),
        upper=LiteralValue(data_type="INTEGER", value=100),
        negated=True,
    )
    assert renderer.render(not_between_node) == "[Score] NOT BETWEEN 0 AND 100"


def test_render_like(renderer: SqlExpressionRenderer):
    like_node = LikeExpression(
        column=ColumnRef(dataset="", column="Email"),
        pattern="%@company.com",
        negated=False,
    )
    assert renderer.render(like_node) == "[Email] LIKE N'%@company.com'"

    not_like_node = LikeExpression(
        column=ColumnRef(dataset="", column="Email"),
        pattern="%@spam.com",
        negated=True,
    )
    assert renderer.render(not_like_node) == "[Email] NOT LIKE N'%@spam.com'"


def test_render_boolean_and_or_not(renderer: SqlExpressionRenderer):
    c1 = ComparisonExpression(
        operator=ComparisonOperator.GT,
        left=ColumnRef(dataset="", column="Salary"),
        right=LiteralValue(data_type="INTEGER", value=50000),
    )
    c2 = ComparisonExpression(
        operator=ComparisonOperator.EQ,
        left=ColumnRef(dataset="", column="Department"),
        right=LiteralValue(data_type="STRING", value="Engineering"),
    )
    and_expr = BooleanExpression(
        operator=BooleanOperator.AND,
        operands=[c1, c2],
    )
    assert renderer.render(and_expr) == "(([Salary] > 50000) AND ([Department] = N'Engineering'))"

    not_expr = BooleanExpression(
        operator=BooleanOperator.NOT,
        operands=[c1],
    )
    assert renderer.render(not_expr) == "NOT ([Salary] > 50000)"


def test_render_from_dict_and_rejection_of_unknown(renderer: SqlExpressionRenderer):
    dict_node = {
        "node_type": "COMPARISON",
        "operator": "GTE",
        "left": {"node_type": "COLUMN", "column": "Age"},
        "right": {"node_type": "LITERAL", "value": 21, "data_type": "INTEGER"},
    }
    assert renderer.render(dict_node) == "[Age] >= 21"

    with pytest.raises(Tier3RenderingError, match="Unknown or unsupported"):
        renderer.render({"node_type": "ARBITRARY_RAW_SQL", "sql": "DROP TABLE Employees"})

    with pytest.raises(Tier3RenderingError, match="Cannot render None"):
        renderer.render(None)
