import unittest
from src.utils import PostgresClient
from tests.test_files.test_config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_DATABASE

class TestPostgresClient(unittest.TestCase):
    def setUp(self):
        self.client = PostgresClient(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_DATABASE
        )

    def test_switch_database(self):
        self.client.switch_database('test_database')
        self.assertEqual(self.client.database, 'test_database')

    def test_connect_default(self):
        self.client.connect_default()
        self.client.close_connection()

    def test_connect(self):
        self.client.connect(database='test_database')
        self.client.close_connection()

    def test_add_sql(self):
        self.client.connect(database='test_database')
        sql = 'SELECT * FROM test_table;'
        self.client.add_sql(sql)
        self.client.close_connection()

    def test_add_sql_file(self):
        file_path = 'tests/test_files/test_sql_file.sql'
        self.client.connect(database='test_database')
        self.client.add_sql_file(file_path)
        self.client.close_connection()

    def test_run_query(self):
        self.client.connect(database='test_database')
        sql = 'SELECT * FROM test_table;'
        results = self.client.run_query(sql)
        print(results)
        self.client.close_connection()

    def test_commit_sql(self):
        self.client.connect(database='test_database')
        sql = 'SELECT * FROM test_table;'
        self.client.add_sql(sql)
        self.client.commit_sql()
        self.client.close_connection()

    def test_enable_autocommit(self):
        self.client.connect(database='test_database')
        self.client.enable_autocommit()
        self.assertTrue(self.client.conn.autocommit)
        self.client.close_connection()

    def test_disable_autocommit(self):
        self.client.connect(database='test_database')
        self.client.disable_autocommit()
        self.assertFalse(self.client.conn.autocommit)
        self.client.close_connection()

    def test_close_connection(self):
        self.client.connect(database='test_database')
        self.client.close_connection()

if __name__ == '__main__':
    unittest.main()
