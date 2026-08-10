import ast
import sys

# Verify file syntax
with open('agent/etl_pipeline/llm_codegen.py', 'r', encoding='utf-8') as f:
    source = f.read()
try:
    ast.parse(source)
    print('SYNTAX OK')
except SyntaxError as e:
    print(f'SYNTAX ERROR at line {e.lineno}: {e.msg}')
    sys.exit(1)

# Quick test of the injector
from agent.etl_pipeline.llm_codegen import _inject_pyspark_imports

bad_code = '"""\nplan_id: plan_123\n"""\n\ndef _resolve_data_path(location):\n    return location\n\ndef transform_data(df: DataFrame):\n    from pyspark.sql import functions as F\n    import logging\n    return df.withColumn("x", F.trim(F.col("x")))\n\nif __name__ == "__main__":\n    from pyspark.sql import SparkSession\n    spark = SparkSession.builder.getOrCreate()\n'

fixed = _inject_pyspark_imports(bad_code)
print('\nInjected result (first 12 lines):')
for i, line in enumerate(fixed.splitlines()[:12]):
    print(f'  L{i+1}: {line}')

# Verify the fix works with the validator
from agent.etl_pipeline.validate_pyspark import validate_pyspark_source
ok, errs = validate_pyspark_source(fixed)
print()
print(f'Validation after injection: ok={ok}, errs={errs}')
