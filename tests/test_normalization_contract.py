import unittest
import pandas as pd
from agent.etl_pipeline.relationship_key_contract import RelationshipKeyContract, DEFAULT_CONTRACT
from agent.etl_pipeline.relationship_key_normalization_pandas import normalize_relationship_key_pandas

class TestNormalizationContract(unittest.TestCase):

    def test_trim_strips_whitespace(self):
        contract = RelationshipKeyContract(trim=True, case_fold=False)
        s = pd.Series(["  abc  ", "xyz", "  "])
        res = normalize_relationship_key_pandas(s, contract)
        self.assertEqual(res.iloc[0], "abc")
        self.assertEqual(res.iloc[1], "xyz")
        # Empty space strips to "" which matches null token "" by default
        self.assertIsNone(res.iloc[2])

    def test_null_token_na_normalized(self):
        contract = RelationshipKeyContract(trim=True, case_fold=True, null_tokens=("na", "n/a"))
        s = pd.Series(["NA", "N/A", "na", "n/a", "valid"])
        res = normalize_relationship_key_pandas(s, contract)
        self.assertIsNone(res.iloc[0])
        self.assertIsNone(res.iloc[1])
        self.assertIsNone(res.iloc[2])
        self.assertIsNone(res.iloc[3])
        self.assertEqual(res.iloc[4], "valid")

    def test_case_fold_disabled(self):
        contract = RelationshipKeyContract(trim=True, case_fold=False, null_tokens=("na",))
        s = pd.Series(["NA", "na"])
        res = normalize_relationship_key_pandas(s, contract)
        # Case fold disabled: "NA" is not normalized because it is uppercase, "na" is normalized.
        self.assertEqual(res.iloc[0], "NA")
        self.assertIsNone(res.iloc[1])

    def test_contract_hash_stable_across_instantiations(self):
        c1 = RelationshipKeyContract()
        c2 = RelationshipKeyContract()
        self.assertEqual(c1.contract_hash(), c2.contract_hash())
        
        c3 = RelationshipKeyContract(case_fold=True)
        self.assertNotEqual(c1.contract_hash(), c3.contract_hash())

if __name__ == "__main__":
    unittest.main()
