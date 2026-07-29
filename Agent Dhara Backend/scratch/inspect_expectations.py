import great_expectations as gx
import pandas as pd

df = pd.DataFrame({'a': [1, 2, 3]})
context = gx.get_context()
datasource = context.data_sources.add_pandas(name="test_ds")
asset = datasource.add_dataframe_asset(name="test_asset")
batch_definition = asset.add_batch_definition_whole_dataframe("test_batch_def")
batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
suite = context.suites.add(gx.ExpectationSuite(name="test_suite"))
validator = context.get_validator(batch=batch, expectation_suite_name="test_suite")

methods = [m for m in dir(validator) if m.startswith("expect_")]
for m in sorted(methods):
    print(m)
