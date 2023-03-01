import unittest
from src.database.create_infrastructure import *
from tests.test_files.test_config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD

class TestCreateInfrastructure(unittest.TestCase):
    def setUp(self):
        self.client = PostgresClient(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD
        )

    def test_create_database(self):
        create_database(client=self.client, database_sql='tests/test_files/test_database.sql')
        self.client.connect()
        result = self.client.run_query("SELECT datname FROM pg_database WHERE datname='fhir_database_test'")
        self.assertEqual(result[0][0], 'fhir_database_test')

    def test_create_infrastructure(self):
        create_infrastructure(client=self.client, database_sql='tests/test_files/test_database.sql', tables_sql='')
        self.client.connect()
        result = self.client.run_query("SELECT datname FROM pg_database WHERE datname='fhir_database_test'")
        self.assertEqual(result[0][0], 'fhir_database_test')

    def tearDown(self):
        self.client.connect()
        self.client.enable_autocommit()
        self.client.add_sql("DROP DATABASE IF EXISTS fhir_database_test")
        self.client.commit_sql()
        self.client.close_connection()

if __name__ == '__main__':
    unittest.main()
