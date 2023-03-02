"""
create_database() function is used to create the fhir_database and its associated tables. The table names can be found in the tables.sql file.
The function takes a configuration dictionary containing postgreSQL database server connection details.
It creates the database and tables by executing the SQL command in the database.sql file and the tables.sql file.
"""


import configparser

import psycopg2
import psycopg2.extensions
import psycopg2.errors


def create_connection(server_config: dict, database_connection: bool) -> psycopg2.extensions.connection:
    """
    Creates a connection to the database using the credentials provided by the config.ini file.
    If database_connection is False, connects to default database.

    :param server_config:
        The database server configuration dictionary which includes host, port, user, password, and database information.
    :param database_connection:
        True of False, whether or not to connect to the database in the config.ini file (otherwise connect to the default database).
    """
    try:
        if database_connection:
            conn = psycopg2.connect(
                host=server_config['HOST'],
                port=server_config['PORT'],
                user=server_config['USER'],
                password=server_config['PASSWORD'],
                database=server_config['DATABASE']
            )
        else:
            conn = psycopg2.connect(
                host=server_config['HOST'],
                port=server_config['PORT'],
                user=server_config['USER'],
                password=server_config['PASSWORD']
            )
        return conn
    except psycopg2.OperationalError as e:
        raise Exception("Error connecting to postgreSQL server due to error: {e}")


def execute_sql_file(conn: psycopg2.extensions.connection, sql_file: str):
    """
    Executes SQL commands specified in a file, and commits the execution.

    :param conn: psycopg2 connection to a postgreSQL sever database.
    :param sql_file: path for the SQL file to be executed.
    """
    cursor = conn.cursor()
    try:
        with open(sql_file, 'r') as f:
            sql_statement = ''
            for each_line in f:
                if each_line.startswith('--'):
                    continue
                sql_statement += each_line.strip() + ' '
                if sql_statement.strip().endswith(';'):
                    try:
                        print(f"Executing SQL command:\n{sql_statement}")
                        cursor.execute(sql_statement)
                        print("SQL command successfully executed.")
                    except (psycopg2.errors.DuplicateDatabase, psycopg2.errors.DuplicateTable) as e:
                        print(e)
                    except psycopg2.errors.SyntaxError as e:
                        cursor.close()
                        conn.close()
                        raise Exception(f"Could not execute SQL statement\n{sql_statement}\ndue to error: {e}")
                    sql_statement = ''
    except FileNotFoundError as e:
        cursor.close()
        conn.close()
        raise Exception(f"Could not open {sql_file} due to error: {e}")
    conn.commit()
    print("SQL commands commited.")
    cursor.close()


def create_database(server_config: dict):
    """
    Creates the database and associated tables in the default schema. The database is called fhir_database.
    The table names can be found in the tables.sql file.

    :param server_config:
        The database server configuration dictionary which includes host, port, user, password, and database information.
    """
    print("Opening connection to server...")
    conn = create_connection(server_config=server_config, database_connection=False)
    print("Connection established.")
    
    conn.autocommit = True

    # Creating database using database.sql file.
    print("Creating fhir_database...")
    execute_sql_file(conn=conn, sql_file='src/database/database_sql/database.sql')

    conn.close()
        
    conn = create_connection(server_config=server_config, database_connection=True)

    # Creating tables using tables.sql file.
    print("Creating fhir_database tables...")
    execute_sql_file(conn=conn, sql_file='src/database/database_sql/tables.sql')
    
    print("Database and tables successfully created.")

    print("Closing connection...")
    conn.close()
    print("Connection closed.")


def main():
    """
    Running this file will create the database and associated tables in the postgreSQL server whose details are provided in config.ini.
    """
    config = configparser.ConfigParser()

    # Read postgreSQL configuration from config.ini file.
    try:
        print("Reading credentials...")
        config.read('config.ini')

        # Set up config dictionary.
        server_config = {
            'HOST': config.get('Server', 'HOST'),
            'PORT': config.get('Server', 'PORT'),
            'USER': config.get('Server', 'USER'),
            'PASSWORD': config.get('Server', 'PASSWORD'),
            'DATABASE': config.get('Server', 'DATABASE')
        }
        print("Credential loaded.")
    except configparser.Error as e:
        raise Exception(f"Could not read config file due to error: {e}")

    # Create database and tables.
    create_database(server_config=server_config)
    

if __name__ == '__main__':
    main()