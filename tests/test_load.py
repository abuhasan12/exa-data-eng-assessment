import unittest
from src.etl.pipeline_functions.load import *
import psycopg2
from src.database.create_database import execute_sql_file
from tests.test_files.test_config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_DATABASE
from tests.test_files.test_schemas import SCHEMAS

class TestLoad(unittest.TestCase):
    def setUp(self):
        self.server_config = {
            'HOST':DB_HOST,
            'PORT':DB_PORT,
            'USER':DB_USER,
            'PASSWORD':DB_PASSWORD,
            'DATABASE': DB_DATABASE
        }
        conn = psycopg2.connect(
            host=self.server_config['HOST'],
            port=self.server_config['PORT'],
            user=self.server_config['USER'],
            password=self.server_config['PASSWORD']
        )
        conn.autocommit = True
        execute_sql_file(conn=conn, sql_file='tests/test_files/test_database.sql')
        conn.close()
        conn = psycopg2.connect(
            host=self.server_config['HOST'],
            port=self.server_config['PORT'],
            user=self.server_config['USER'],
            password=self.server_config['PASSWORD'],
            database=self.server_config['DATABASE']
        )
        execute_sql_file(conn=conn, sql_file='tests/test_files/test_tables.sql')
        conn.close()

    def test_upload_resources(self):
        conn = psycopg2.connect(
            host=self.server_config['HOST'],
            port=self.server_config['PORT'],
            user=self.server_config['USER'],
            password=self.server_config['PASSWORD'],
            database=self.server_config['DATABASE']
        )
        rows = [['test'], ['test_two']]
        upload_resources(conn, 'test_table_one', 1, rows)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM test_table_one")
        result = cursor.fetchall()
        self.assertEqual(result, [('test',), ('test_two',)])
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