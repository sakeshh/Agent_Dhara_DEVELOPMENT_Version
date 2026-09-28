from __future__ import annotations

from typing import Any, Dict, List, Union

from agent.compiler.models.tier3_resolution import AstNodeType
from agent.compiler.templates.engine import quote_identifier, render_literal
from agent.compiler.tier3.exceptions import Tier3RenderingError


class SqlExpressionRenderer:
    """
    Deterministically renders validated Condition AST nodes into safe T-SQL expressions.
    Guarantees:
      - Strictly allowlists node types (COLUMN, LITERAL, COMPARISON, BOOLEAN, NULL_CHECK, IN_LIST, BETWEEN, LIKE).
      - Safely brackets identifiers and escapes literals via standard compiler functions.
      - Rejects arbitrary SQL, comments, batch separators, and unknown AST structures.
    """

    ALLOWED_NODE_TYPES = {
        AstNodeType.COLUMN,
        AstNodeType.LITERAL,
        AstNodeType.COMPARISON,
        AstNodeType.BOOLEAN,
        AstNodeType.NULL_CHECK,
        AstNodeType.IN_LIST,
        AstNodeType.BETWEEN,
        AstNodeType.LIKE,
    }

    COMPARISON_OPERATOR_MAP = {
        "EQ": "=",
        "=": "=",
        "NE": "<>",
        "NEQ": "<>",
        "!=": "<>",
        "<>": "<>",
        "LT": "<",
        "<": "<",
        "LTE": "<=",
        "<=": "<=",
        "GT": ">",
        ">": ">",
        "GTE": ">=",
        ">=": ">=",
    }

    def render(self, node: Union[Dict[str, Any], Any]) -> str:
        """Renders an AST node or dictionary into a deterministic T-SQL expression string."""
        if node is None:
            raise Tier3RenderingError("Cannot render None AST node.")

        node_dict = node.model_dump() if hasattr(node, "model_dump") else (node.to_dict() if hasattr(node, "to_dict") else node)
        if not isinstance(node_dict, dict):
            raise Tier3RenderingError(f"AST node must be a dict or model, got {type(node).__name__}")

        raw_type = node_dict.get("node_type")
        if not raw_type:
            raise Tier3RenderingError("AST node missing 'node_type' field.")

        if hasattr(raw_type, "value"):
            raw_type_str = str(raw_type.value).upper()
        elif "." in str(raw_type):
            raw_type_str = str(raw_type).split(".")[-1].upper()
        else:
            raw_type_str = str(raw_type).upper()

        try:
            node_type = AstNodeType(raw_type_str)
        except ValueError:
            raise Tier3RenderingError(f"Unknown or unsupported AST node type: '{raw_type}'.")

        if node_type not in self.ALLOWED_NODE_TYPES:
            raise Tier3RenderingError(f"Prohibited AST node type: '{node_type}'.")

        if node_type == AstNodeType.COLUMN:
            return self._render_column(node_dict)
        elif node_type == AstNodeType.LITERAL:
            return self._render_literal(node_dict)
        elif node_type == AstNodeType.COMPARISON:
            return self._render_comparison(node_dict)
        elif node_type == AstNodeType.BOOLEAN:
            return self._render_boolean(node_dict)
        elif node_type == AstNodeType.NULL_CHECK:
            return self._render_null_check(node_dict)
        elif node_type == AstNodeType.IN_LIST:
            return self._render_in_list(node_dict)
        elif node_type == AstNodeType.BETWEEN:
            return self._render_between(node_dict)
        elif node_type == AstNodeType.LIKE:
            return self._render_like(node_dict)
        else:
            raise Tier3RenderingError(f"Unhandled node type: '{node_type}'.")

    def _render_column(self, node: Dict[str, Any]) -> str:
        col = node.get("column")
        if not col or not str(col).strip():
            raise Tier3RenderingError("COLUMN node missing column identifier.")
        dataset = node.get("dataset")
        col_quoted = quote_identifier(col)
        if dataset and str(dataset).strip():
            ds_clean = str(dataset).strip()
            # If dataset is schema-qualified (dbo.Table), quote each part
            if "." in ds_clean:
                parts = ds_clean.split(".")
                ds_quoted = ".".join(quote_identifier(p) for p in parts)
            else:
                ds_quoted = quote_identifier(ds_clean)
            return f"{ds_quoted}.{col_quoted}"
        return col_quoted

    def _render_literal(self, node: Dict[str, Any]) -> str:
        return render_literal(node.get("value"))

    def _render_comparison(self, node: Dict[str, Any]) -> str:
        left = node.get("left")
        right = node.get("right")
        raw_op = node.get("operator")
        if left is None or right is None or raw_op is None:
            raise Tier3RenderingError("COMPARISON node requires 'left', 'right', and 'operator'.")

        op_str = str(raw_op.value if hasattr(raw_op, "value") else raw_op).upper().strip()
        sql_op = self.COMPARISON_OPERATOR_MAP.get(op_str)
        if not sql_op:
            raise Tier3RenderingError(f"Unsupported comparison operator: '{op_str}'.")

        left_sql = self.render(left)
        right_sql = self.render(right)
        return f"{left_sql} {sql_op} {right_sql}"

    def _render_boolean(self, node: Dict[str, Any]) -> str:
        raw_op = node.get("operator")
        operands = node.get("operands") or []
        if raw_op is None:
            raise Tier3RenderingError("BOOLEAN node requires 'operator'.")

        op_str = str(raw_op.value if hasattr(raw_op, "value") else raw_op).upper().strip()
        if op_str == "NOT":
            if len(operands) != 1:
                raise Tier3RenderingError(f"NOT boolean operator expects exactly 1 operand, got {len(operands)}.")
            return f"NOT ({self.render(operands[0])})"

        if op_str in ("AND", "OR"):
            if not operands:
                raise Tier3RenderingError(f"{op_str} boolean operator requires at least 1 operand.")
            if len(operands) == 1:
                return self.render(operands[0])
            rendered_operands = [f"({self.render(op)})" for op in operands]
            return f"({f' {op_str} '.join(rendered_operands)})"

        raise Tier3RenderingError(f"Unsupported boolean operator: '{op_str}'.")

    def _render_null_check(self, node: Dict[str, Any]) -> str:
        col = node.get("column")
        if not col:
            raise Tier3RenderingError("NULL_CHECK node requires 'column'.")
        col_sql = self.render(col)
        is_null = node.get("is_null", True)
        return f"{col_sql} IS NULL" if is_null else f"{col_sql} IS NOT NULL"

    def _render_in_list(self, node: Dict[str, Any]) -> str:
        col = node.get("column")
        values = node.get("values")
        if not col:
            raise Tier3RenderingError("IN_LIST node requires 'column'.")
        if values is None or len(values) == 0:
            raise Tier3RenderingError("IN_LIST node requires a non-empty 'values' list.")

        col_sql = self.render(col)
        rendered_vals = []
        for v in values:
            if isinstance(v, dict):
                rendered_vals.append(render_literal(v.get("value")))
            elif hasattr(v, "value"):
                rendered_vals.append(render_literal(v.value))
            else:
                rendered_vals.append(render_literal(v))

        op = "NOT IN" if node.get("negated", False) else "IN"
        return f"{col_sql} {op} ({', '.join(rendered_vals)})"

    def _render_between(self, node: Dict[str, Any]) -> str:
        col = node.get("column")
        lower = node.get("lower")
        upper = node.get("upper")
        if not col or lower is None or upper is None:
            raise Tier3RenderingError("BETWEEN node requires 'column', 'lower', and 'upper'.")

        col_sql = self.render(col)
        lower_val = lower.get("value") if isinstance(lower, dict) else (lower.value if hasattr(lower, "value") else lower)
        upper_val = upper.get("value") if isinstance(upper, dict) else (upper.value if hasattr(upper, "value") else upper)

        lower_sql = render_literal(lower_val)
        upper_sql = render_literal(upper_val)
        op = "NOT BETWEEN" if node.get("negated", False) else "BETWEEN"
        return f"{col_sql} {op} {lower_sql} AND {upper_sql}"

    def _render_like(self, node: Dict[str, Any]) -> str:
        col = node.get("column")
        pattern = node.get("pattern")
        if not col or pattern is None:
            raise Tier3RenderingError("LIKE node requires 'column' and 'pattern'.")

        col_sql = self.render(col)
        pat_sql = render_literal(pattern)
        op = "NOT LIKE" if node.get("negated", False) else "LIKE"
        return f"{col_sql} {op} {pat_sql}"
