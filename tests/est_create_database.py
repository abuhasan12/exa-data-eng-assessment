import unittest
import psycopg2
from src.database.create_database import *
from tests.test_files.test_config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD

class TestCreateDatabase(unittest.TestCase):
    def setUp(self):
        self.server_config = {
            'HOST':DB_HOST,
            'PORT':DB_PORT,
            'USER':DB_USER,
            'PASSWORD':DB_PASSWORD,
            'DATABASE': None
        }

    def test_execute_sql_file_database(self):
        execute_sql_file(server_config=self.server_config, sql_file='tests/test_files/test_database.sql', autocommit=True)
        conn = psycopg2.connect(
            host=self.server_config['HOST'],
            port=self.server_config['PORT'],
            user=self.server_config['USER'],
            password=self.server_config['PASSWORD']
        )
        cursor = conn.cursor()
        cursor.execute("SELECT datname FROM pg_database WHERE datname='fhir_database_test'")
        result = cursor.fetchall()
        conn.commit()
        self.assertEqual(result[0][0], 'fhir_database_test')
        cursor.close()
        conn.close()

    def test_execute_sql_file_tables(self):
        execute_sql_file(server_config=self.server_config, sql_file='tests/test_files/test_database.sql', autocommit=True)
        execute_sql_file(server_config=self.server_config, sql_file='tests/test_files/test_tables.sql', replace_database='fhir_database_test')
        conn = psycopg2.connect(
            host=self.server_config['HOST'],
            port=self.server_config['PORT'],
            user=self.server_config['USER'],
            password=self.server_config['PASSWORD'],
            database='fhir_database_test'
        )
        cursor = conn.cursor()
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'test_table_one')")
        result = cursor.fetchall()
        self.assertEqual(result[0][0], True)
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'test_table_two')")
        result = cursor.fetchall()
        self.assertEqual(result[0][0], True)
        cursor.close()
        conn.close()

    def test_create_database(self):
        create_database(server_config=self.server_config, new_database='fhir_database_test', database_sql='tests/test_files/test_database.sql', tables_sql='tests/test_files/test_tables.sql')
        conn = psycopg2.connect(
            host=self.server_config['HOST'],
            port=self.server_config['PORT'],
            user=self.server_config['USER'],
            password=self.server_config['PASSWORD']
        )
        cursor = conn.cursor()
        cursor.execute("SELECT datname FROM pg_database WHERE datname='fhir_database_test'")
        result = cursor.fetchall()
        conn.commit()
        self.assertEqual(result[0][0], 'fhir_database_test')
        cursor.close()
        conn.close()
        conn = psycopg2.connect(
            host=self.server_config['HOST'],
            port=self.server_config['PORT'],
            user=self.server_config['USER'],
            password=self.server_config['PASSWORD'],
            database='fhir_database_test'
        )
        cursor = conn.cursor()
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'test_table_one')")
        result = cursor.fetchall()
        self.assertEqual(result[0][0], True)
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'test_table_two')")
        result = cursor.fetchall()
        self.assertEqual(result[0][0], True)
        cursor.close()
        conn.close()

    def tearDown(self):
        conn = psycopg2.connect(
            host=self.server_config['HOST'],
            port=self.server_config['PORT'],
            user=self.server_config['USER'],
            password=self.server_config['PASSWORD']
        )
        cursor = conn.cursor()
        conn.autocommit = True
        cursor.execute("DROP DATABASE IF EXISTS fhir_database_test")
        conn.commit()
        cursor.close()
        conn.close()

if __name__ == '__main__':
    unittest.main()
