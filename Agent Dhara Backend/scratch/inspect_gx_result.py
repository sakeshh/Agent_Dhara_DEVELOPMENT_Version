import great_expectations as gx
import pandas as pd
import json

df = pd.DataFrame({'a': [1, None, 3, None, 5]})
context = gx.get_context()
datasource = context.data_sources.add_pandas(name="test_ds_check_c")
asset = datasource.add_dataframe_asset(name="test_asset_check_c")
batch_definition = asset.add_batch_definition_whole_dataframe("test_batch_def_check_c")
batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
suite = context.suites.add(gx.ExpectationSuite(name="test_suite_check_c"))
validator = context.get_validator(batch=batch, expectation_suite_name="test_suite_check_c")

validator.expect_column_values_to_not_be_null("a")
res = validator.validate(result_format="COMPLETE")

# Print the validation result in JSON-like structure
print(json.dumps(res.to_json_dict(), indent=2))
