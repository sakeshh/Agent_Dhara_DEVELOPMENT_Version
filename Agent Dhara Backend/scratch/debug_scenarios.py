import json
import pandas as pd
from agent.intelligent_data_assessment import profile_dataframe, analyze_dataset_quality, load_dq_thresholds
from agent.etl_pipeline.planner import build_etl_plan
from agent.etl_pipeline.phase_classifier import split_plan_phases

# Load cleanse_only
path = r"tests/etl_scenarios/fixtures/cleanse_only.json"
with open(path, "r") as f:
    data = json.load(f)
df = pd.DataFrame(data)

profile = profile_dataframe(df)
dq = analyze_dataset_quality("dbo.cleanse_only", df, profile, load_dq_thresholds())
profile["quality"] = dq

assessment = {"datasets": {"dbo.cleanse_only": profile}}
plan = build_etl_plan(assessment, {}, engine="sql")

cleanse_plan, transform_plan = split_plan_phases(plan)
print("Cleanse steps:")
for s in cleanse_plan.get("datasets", {}).get("dbo.cleanse_only", {}).get("steps", []):
    print(" -", s.get("action"), s.get("column"))
print("Transform steps:")
for s in transform_plan.get("datasets", {}).get("dbo.cleanse_only", {}).get("steps", []):
    print(" -", s.get("action"), s.get("column"))
