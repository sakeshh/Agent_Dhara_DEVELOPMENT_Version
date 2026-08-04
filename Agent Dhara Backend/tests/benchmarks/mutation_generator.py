"""
Synthetic Mutation Defect Generator for Continuous Benchmarking.
Injects controlled defects into clean data to evaluate Precision, Recall, and F1.
"""

import random
from typing import Any, Dict, List, Tuple
import pandas as pd
from pydantic import BaseModel, Field, ConfigDict


class MutationLog(BaseModel):
    model_config = ConfigDict(extra="allow")
    mutation_type: str
    column: str
    row_index: int
    original_value: Any
    mutated_value: Any


def inject_null_defects(df: pd.DataFrame, column: str, count: int = 5) -> Tuple[pd.DataFrame, List[MutationLog]]:
    """
    Injects null values into specified column at random row indexes.
    """
    mutated_df = df.copy()
    logs = []
    rows = list(range(len(df)))
    sample_rows = random.sample(rows, min(count, len(rows)))

    for r in sample_rows:
        orig = mutated_df.at[r, column]
        mutated_df.at[r, column] = None
        logs.append(MutationLog(
            mutation_type="null_injection",
            column=column,
            row_index=r,
            original_value=orig,
            mutated_value=None
        ))
    return mutated_df, logs


def inject_whitespace_defects(df: pd.DataFrame, column: str, count: int = 5) -> Tuple[pd.DataFrame, List[MutationLog]]:
    """
    Injects outer whitespace into string values at random row indexes.
    """
    mutated_df = df.copy()
    logs = []
    rows = list(range(len(df)))
    sample_rows = random.sample(rows, min(count, len(rows)))

    for r in sample_rows:
        orig = mutated_df.at[r, column]
        mutated = f"  {orig}  "
        mutated_df.at[r, column] = mutated
        logs.append(MutationLog(
            mutation_type="whitespace_mutation",
            column=column,
            row_index=r,
            original_value=orig,
            mutated_value=mutated
        ))
    return mutated_df, logs
