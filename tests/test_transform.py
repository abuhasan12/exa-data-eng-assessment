import unittest
from src.etl.pipeline_functions.transform import *

class TestTransform(unittest.TestCase):
    def setUp(self):
        self.file_paths = 'tests/test_files/test_data/'
    
    def test_get_value_for_col_single_quote_in_string(self):
        test_json = {
            "resourceType": "Test_Bundle",
            "type": "test_transaction",
            "entry": [ {
                "fullUrl": "urn:uuid:test_url",
                "resource": {
                    "resourceType": "TestResource",
                    "id": "test_id"
                },
                "request": {
                    "method": "'TEST'",
                    "url": "TestResource"
                }
            }
            ]
        }
        path_to_value = ['entry', 0, 'request', 'method']
        expected_value = "''TEST''"
        self.assertEqual(expected_value, get_value_for_col(test_json, path_to_value))

    def test_get_value_for_col_list(self):
        test_json = {
            "resourceType": "Test_Bundle",
            "type": "test_transaction",
            "entry": [ {
                "fullUrl": "urn:uuid:test_url",
                "resource": {
                    "resourceType": "TestResource",
                    "id": "test_id"
                },
                "request": {
                    "method": "TEST",
                    "url": "TestResource"
                }
            }
            ]
        }
        path_to_value = ['entry']
        expected_value = '[{"fullUrl": "urn:uuid:test_url", "resource": {"resourceType": "TestResource", "id": "test_id"}, "request": {"method": "TEST", "url": "TestResource"}}]'
        self.assertEqual(expected_value, get_value_for_col(test_json, path_to_value))