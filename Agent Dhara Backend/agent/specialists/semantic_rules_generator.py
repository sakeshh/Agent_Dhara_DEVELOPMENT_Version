import json
import logging
import pandas as pd
from typing import Dict, Any, Optional
from agent.model_config import load_llm_config

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """
You are a data quality specialist. Your task is to analyze the schema, column datatypes, and sample rows of one or more datasets and discover logical, semantic business rules and constraints.

For each dataset provided, analyze the columns and their values. Determine:
1. Which columns are semantically "required" (must exist and are essential to the entity).
2. Which columns must be "non-nullable" (cannot be null, e.g., keys, IDs, statuses, critical fields).
3. "valid_values": Categorical columns and their lookup category lists (lists of valid values, e.g. status values, category types). Key must be formatted as "dataset_name.column_name" to avoid collisions.
4. "custom_assertions": Cross-column logical constraints/rules that must hold true within a single dataset.
   - These MUST be valid Python/Pandas `.eval()` expressions referencing only columns in the same dataset.
   - For example: `age > 18`, `start_date < end_date`, `status != 'Active' or email.notnull()`, etc.
   - Do NOT reference columns across different datasets in the same assertion.
   - Each assertion should have a "severity" ("high", "medium", or "low") and a clear "message" explaining the rule violation.

Your response must be a single, valid JSON object with the following structure:
{
  "required_columns": ["col_name_1", "col_name_2"],
  "non_nullable": ["col_name_1", "col_name_2"],
  "valid_values": {
    "dataset_name.column_name": ["value1", "value2"]
  },
  "custom_assertions": [
    {
      "assertion": "column_a > column_b",
      "severity": "high",
      "message": "Column A must be greater than Column B"
    }
  ]
}

Only return valid JSON. Do not return any markdown code block formatting (no ```json).
""".strip()

def generate_semantic_rules_from_metadata(datasets: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    Analyzes schemas and samples of datasets to generate business rules using an LLM.
    """
    default_rules = {
        "required_columns": [],
        "non_nullable": [],
        "valid_values": {},
        "custom_assertions": []
    }
    
    cfg = load_llm_config(purpose="semantic_rules_discovery")
    if cfg is None:
        logger.warning("LLM not configured for semantic_rules_discovery. Returning empty rules.")
        return default_rules

    # Gather schemas and a small sample (3-5 rows) for each dataset
    prompt_data = {}
    for name, df in datasets.items():
        if df.empty:
            continue
        # limit to 3-5 rows
        sample_df = df.head(5)
        # convert schema details to string
        columns_info = {}
        for col in df.columns:
            columns_info[col] = {
                "type": str(df[col].dtype),
                "null_count": int(df[col].isnull().sum()),
                "distinct_count": int(df[col].nunique())
            }
        
        prompt_data[name] = {
            "columns": columns_info,
            "sample_rows": sample_df.to_dict(orient="records")
        }

    if not prompt_data:
        return default_rules

    user_prompt = json.dumps(prompt_data, indent=2, default=str)

    try:
        if cfg.provider == "azure_openai":
            from openai import AzureOpenAI  # type: ignore
            client = AzureOpenAI(
                api_key=cfg.api_key,
                api_version=cfg.api_version or "2024-02-01",
                azure_endpoint=cfg.endpoint,
            )
            resp = client.chat.completions.create(
                model=cfg.model,
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.1,
                max_tokens=2000,
            )
        else:
            from openai import OpenAI  # type: ignore
            client = OpenAI(api_key=cfg.api_key)
            resp = client.chat.completions.create(
                model=cfg.model,
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.1,
                max_tokens=2000,
            )
        
        raw_content = (resp.choices[0].message.content or "").strip()
        # Clean markdown formatting if returned
        if raw_content.startswith("```"):
            lines = raw_content.splitlines()
            if lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].startswith("```"):
                lines = lines[:-1]
            raw_content = "\n".join(lines).strip()
            
        obj = json.loads(raw_content)
        
        # Normalize and validate structure
        rules = {
            "required_columns": obj.get("required_columns") or [],
            "non_nullable": obj.get("non_nullable") or [],
            "valid_values": obj.get("valid_values") or {},
            "custom_assertions": obj.get("custom_assertions") or []
        }
        
        # Ensure correct list types
        if not isinstance(rules["required_columns"], list):
            rules["required_columns"] = []
        if not isinstance(rules["non_nullable"], list):
            rules["non_nullable"] = []
        if not isinstance(rules["valid_values"], dict):
            rules["valid_values"] = {}
        if not isinstance(rules["custom_assertions"], list):
            rules["custom_assertions"] = []
            
        return rules
    except Exception as e:
        logger.error(f"Failed to generate semantic rules: {e}")
        return default_rules
