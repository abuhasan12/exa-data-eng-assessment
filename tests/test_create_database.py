import unittest
import psycopg2
from src.database.create_database import *
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
        self.client.connect_default()
        result = self.client.run_query("SELECT datname FROM pg_database WHERE datname='fhir_database_test'")
        self.assertEqual(result[0][0], 'fhir_database_test')
        self.client.close_connection()

    def test_create_tables(self):
        create_database(client=self.client, database_sql='tests/test_files/test_database.sql')
        create_tables(client=self.client, database='fhir_database_test', tables_sql='tests/test_files/test_tables.sql')
        self.client.connect(database='fhir_database_test')
        result = self.client.run_query("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'test_table_one')")
        self.assertEqual(result[0][0], True)
        result = self.client.run_query("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'test_table_two')")
        self.assertEqual(result[0][0], True)
        self.client.close_connection()

    def test_create_infrastructure(self):
        create_infrastructure(client=self.client, new_database='fhir_database_test', database_sql='tests/test_files/test_database.sql', tables_sql='tests/test_files/test_tables.sql')
        self.client.connect_default()
        result = self.client.run_query("SELECT datname FROM pg_database WHERE datname='fhir_database_test'")
        self.assertEqual(result[0][0], 'fhir_database_test')
        self.client.close_connection()

    def tearDown(self):
        self.client.connect_default()
        self.client.enable_autocommit()
        self.client.add_sql("DROP DATABASE IF EXISTS fhir_database_test")
        self.client.commit_sql()
        self.client.close_connection()

if __name__ == '__main__':
    unittest.main()
