#!/usr/bin/env python
"""Test DQ issue extraction for TEST 4 format"""

from agent.etl_pipeline.build_etl_plan import _extract_dq_issues

assessment = {
    'datasets': {
        'dbo.Products': {
            'columns': {
                'ProductID': {'dtype': 'int64', 'candidate_primary_key': True},
                'ProductName': {'dtype': 'object', 'null_percentage': 0.08},
                'UnitPrice': {'dtype': 'float64', 'null_percentage': 0.03, 'mean': 29.99, 'median': 24.99}
            }
        }
    },
    'data_quality_issues': {
        'datasets': {
            'dbo.Products': {
                'issues': [
                    {
                        'id': 'trace_issue_1',
                        'column': 'ProductName',
                        'issue_type': 'missing_values',
                        'severity': 'medium',
                        'row_count_affected': 40
                    },
                    {
                        'id': 'trace_issue_2',
                        'column': 'UnitPrice',
                        'issue_type': 'negative_values',
                        'severity': 'high',
                        'row_count_affected': 5
                    }
                ]
            }
        }
    }
}

issues = _extract_dq_issues(assessment, 'dbo.Products')
print(f'Extracted issues: {issues}')
if issues:
    for issue in issues:
        iid = issue.get('id')
        col = issue.get('column')
        itype = issue.get('issue_type')
        print(f'  id={iid}, column={col}, issue_type={itype}')