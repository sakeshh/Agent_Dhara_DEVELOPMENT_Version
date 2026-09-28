import pytest

from agent.compiler.models.logical import ActionType
from agent.compiler.physical.physical_planner import PhysicalPlanner
from agent.compiler.templates.engine import TemplateEngine


def test_every_sql_strategy_has_a_template():
    """Validates that every registered SQL strategy maps to a template that exists on disk."""
    engine = TemplateEngine.get_instance()
    for action_type, (strat_name, tmpl_name) in PhysicalPlanner.SQL_STRATEGIES.items():
        template_path = f"sql/{tmpl_name}"
        assert engine.template_exists(template_path), (
            f"ActionType.{action_type.name} with strategy '{strat_name}' maps to missing template '{template_path}'"
        )


def _get_fixture_params(action_type: ActionType) -> dict:
    """Provides minimum valid parameters to render each strategy template."""
    base = {
        "step_id": "step_test_1",
        "description": "Test step description",
        "dataset_name": "Dhara_Employee",
        "staging_table": "[dbo].[Dhara_Employee_Staging]",
        "columns": ["TestCol"],
        "target_column": "TestCol",
        "pipeline_id": "plan_test_123",
        "never_drop_rows": False,
        "disposition": "NULLIFY",
    }
    specials = {
        ActionType.CAST_TYPE: {"target_type": "INT", "target_dtype": "int"},
        ActionType.COERCE_NUMERIC: {"target_type": "NUMERIC(18,2)"},
        ActionType.FILL_NULLS: {"fill_value": "Unknown"},
        ActionType.RANGE_CLIP: {"lower_bound": 0, "upper_bound": 100},
        ActionType.CAP_OUTLIERS: {"lower_bound": 0, "upper_bound": 100},
        ActionType.DEDUPLICATE: {"keys": ["TestCol"], "partition_by": ["TestCol"], "order_by": ["TestCol"]},
        ActionType.VALIDATE_REFERENTIAL_INTEGRITY: {
            "parent_dataset": "Dhara_Department",
            "parent_key": "Id",
            "child_key": "TestCol",
        },
        ActionType.PREFIX_ADD: {"prefix": "EMP_"},
        ActionType.SUFFIX_ADD: {"suffix": "_ID"},
        ActionType.PATTERN_REPLACE: {"pattern": "bad", "replacement": "good"},
        ActionType.VALUE_MAP: {"mapping": {"M": "Male", "F": "Female"}},
        ActionType.PAD: {"width": 8, "pad_char": "0", "direction": "left"},
        ActionType.CONDITIONAL_UPDATE: {
            "sql_condition": "[TestCol] < 0",
            "rendered_new_value": "0",
            "rendered_else_value": "[TestCol]",
        },
        ActionType.PARSE_DATES: {"styles": [120, 103, 101]},
        ActionType.REPLACE_SENTINELS: {"sentinel_values": ["-999", "N/A"]},
        ActionType.ZERO_TO_NULL: {},
        ActionType.TRIM: {},
        ActionType.LOWERCASE: {},
        ActionType.UPPERCASE: {},
        ActionType.SANITIZE_EMAIL: {"disposition": "REJECT"},
        ActionType.NORMALIZE_PHONE: {"disposition": "REJECT"},
        ActionType.MASK_PHONE: {},
        ActionType.STANDARDIZE_BOOLEAN: {},
    }
    base.update(specials.get(action_type, {}))
    return base


def test_every_strategy_renders_executable_sql():
    """Validates that rendering every registered strategy produces executable SQL statements (not comment-only)."""
    engine = TemplateEngine.get_instance()
    executable_keywords = ("UPDATE", "INSERT", "DELETE", "SELECT", "WITH", "SET")

    for action_type, (strat_name, tmpl_name) in PhysicalPlanner.SQL_STRATEGIES.items():
        template_path = f"sql/{tmpl_name}"
        params = _get_fixture_params(action_type)
        sql = engine.render(template_path, **params)

        assert sql.strip(), f"Template '{template_path}' rendered empty string"

        # Check non-comment lines
        non_comment_lines = [l.strip() for l in sql.splitlines() if l.strip() and not l.strip().startswith("--")]
        assert len(non_comment_lines) > 0, f"Template '{template_path}' rendered comment-only SQL"

        # Ensure executable SQL verbs exist
        combined_non_comment = " ".join(non_comment_lines).upper()
        assert any(verb in combined_non_comment for verb in executable_keywords), (
            f"Template '{template_path}' rendered SQL without executable verbs: {combined_non_comment[:100]}"
        )
