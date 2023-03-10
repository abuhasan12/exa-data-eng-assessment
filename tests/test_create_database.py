import unittest
import psycopg2
from src.database.create_database import *
from tests.test_files.test_config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_DATABASE
from tests.test_files import test_database, test_tables

class TestCreateDatabase(unittest.TestCase):
    def setUp(self):
        self.server_config = {
            'HOST':DB_HOST,
            'PORT':DB_PORT,
            'USER':DB_USER,
            'PASSWORD':DB_PASSWORD,
            'DATABASE': DB_DATABASE
        }

    def test_execute_sql_commands_database(self):
        conn = psycopg2.connect(
            host=self.server_config['HOST'],
            port=self.server_config['PORT'],
            user=self.server_config['USER'],
            password=self.server_config['PASSWORD']
        )
        conn.autocommit = True
        execute_sql_commands(conn=conn, sql=test_database.database_sql)
        cursor = conn.cursor()
        cursor.execute(f"SELECT datname FROM pg_database WHERE datname='fhir_database_test'")
        result = cursor.fetchall()
        conn.commit()
        self.assertEqual(result[0][0], 'fhir_database_test')
        cursor.close()
        conn.close()

    def test_execute_sql_file_tables(self):
        conn = psycopg2.connect(
            host=self.server_config['HOST'],
            port=self.server_config['PORT'],
            user=self.server_config['USER'],
            password=self.server_config['PASSWORD']
        )
        conn.autocommit = True
        execute_sql_commands(conn=conn, sql=test_database.database_sql)
        conn.close()
        conn = psycopg2.connect(
            host=self.server_config['HOST'],
            port=self.server_config['PORT'],
            user=self.server_config['USER'],
            password=self.server_config['PASSWORD'],
            database=self.server_config['DATABASE']
        )
        execute_sql_commands(conn=conn, sql=test_tables.tables_sql)
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
