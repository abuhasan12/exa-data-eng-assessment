import unittest
from src.etl.pipeline_functions.extract import *

class TestExtract(unittest.TestCase):
    def setUp(self):
        self.file_paths = ['tests/test_files/test_data/test.json']
    
    def test_extract_json(self):
        expected_json_data = {
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
        for json_data in extract_json(self.file_paths):
            self.assertEqual(json_data, expected_json_data)
    
    def test_extract_resources(self):
        expected_resource = {
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
        for resource in extract_resources(self.file_paths):
            self.assertEqual(resource, expected_resource)