import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agent.transformation_suggester import suggest_transformations
from agent.etl_pipeline.issue_to_step_compiler import compile_issues_to_steps

assessment = {
    "datasets": {
        "test_ds": {
            "columns": {
                "col1": {"semantic_type": "text"}
            }
        }
    },
    "data_quality_issues": {
        "datasets": {
            "test_ds": {
                "issues": [
                    {"type": "whitespace", "column": "col1", "severity": "medium", "count": 10},
                    {"type": "all_caps_values", "column": "col1", "severity": "medium", "count": 5}
                ]
            }
        }
    }
}

sugs = suggest_transformations(assessment)
suggestions = sugs["suggested_transformations"]
rules = {}
sem_schema = {}

print("Suggestions:")
for sug in suggestions:
    print(sug)

datasets_steps, manual_review = compile_issues_to_steps(suggestions, rules, sem_schema)
print("datasets_steps:", datasets_steps)
print("manual_review:", manual_review)
