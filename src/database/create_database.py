"""
create_database() function is used to create the fhir_database and its associated tables. The table names can be found in the tables.py file.
The function takes a configuration dictionary containing postgreSQL database server connection details.
It creates the database and tables by executing the SQL command in the database.py file and the tables.py file.
"""


import argparse

import psycopg2
import psycopg2.extensions
import psycopg2.errors

from src.database.database_functions.database_connection import create_connection
from src.database.database_sql import database, tables


def execute_sql_commands(conn: psycopg2.extensions.connection, sql: list):
    """
    Executes SQL commands specified in a file, and commits the execution.

    :param conn: psycopg2 connection to a postgreSQL sever database.
    :param sql: contents of the SQL file to be executed.
    """
    with conn.cursor() as cursor:
        for sql_statement in sql:
            try:
                cursor.execute(sql_statement)
            except (psycopg2.errors.DuplicateDatabase, psycopg2.errors.DuplicateTable) as e:
                conn.rollback()
                print(e)
            except psycopg2.errors.SyntaxError as e:
                conn.rollback()
                cursor.close()
                conn.close()
                raise Exception(f"Could not execute SQL statement\n{sql_statement}\ndue to error: {e}")
            sql_statement = ''
        conn.commit()


def create_database(server_config: dict):
    """
    Creates the database and associated tables in the default schema. The database is called fhir_database.
    The table names can be found in the tables.py file.

    :param server_config:
        The database server configuration dictionary which includes host, port, user, password, and database information.
    """
    # Creating database using database.py file.
    print("Creating fhir_database...")

    conn = create_connection(server_config=server_config)
    conn.autocommit = True

    execute_sql_commands(conn=conn, sql=database.database_sql)

    conn.close()
    print("Connection closed.")

    # Creating tables using tables.py file.
    print("Creating fhir_database tables...")
        
    server_config['DATABASE'] = 'fhir_database'
    conn = create_connection(server_config=server_config)
    
    execute_sql_commands(conn=conn, sql=tables.tables_sql)

    conn.close()
    print("Connection closed.")
    
    print("Database and tables successfully created.")


def get_args() -> argparse.ArgumentParser:
    """
    Argument parser function for this file. Sets arguments and parses them for this file.

    :returns:
        Parsed arguments.
    """
    # Argument parser
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--host', type=str, required=True,
        help='The host name of the postgreSQL database server to connect to.'
    )
    parser.add_argument(
        '--port', type=str, required=True,
        help='The port of the postgreSQL database server to connect to.'
    )
    parser.add_argument(
        '--user', type=str, required=True,
        help='The username of the postgreSQL database server to connect to.'
    )
    parser.add_argument(
        '--password', type=str, required=True,
        help='The password of the postgreSQL database server to connect to.'
    )
    parser.add_argument(
        '--database', type=str, required=False, default=None,
        help='The database of the postgreSQL database server to connect to. If not specified, user will require access to default database.'
    )

    # Parse args
    args = parser.parse_args()

    return args


def main():
    """
    Running this file will create the database and associated tables in the postgreSQL server whose details are provided in config.ini.
    """
    # Get args
    args = get_args()

    # Read postgreSQL configuration from config.ini file.
    server_config = {
        'HOST': args.host,
        'PORT': args.port,
        'USER': args.user,
        'PASSWORD': args.password,
        'DATABASE': args.database
    }
    print("Server credentials loaded.")

    # Create database and tables.
    create_database(server_config=server_config)
    

if __name__ == '__main__':
    main()