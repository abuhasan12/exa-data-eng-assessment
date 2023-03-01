import argparse
import psycopg2

def create_database(server_config, database_sql):
    conn = psycopg2.connect(
        host=server_config['host'],
        port=server_config['port'],
        user=server_config['user'],
        password=server_config['password']
    )
    conn.autocommit = True
    cursor = conn.cursor()
    with open (database_sql, 'r') as f:
        create_db_sql = f.read()
    cursor.execute(create_db_sql)
    conn.commit()
    cursor.close()
    conn.close()
    
def create_tables(server_config, tables_sql, database):
    conn = psycopg2.connect(
        host=server_config['host'],
        port=server_config['port'],
        user=server_config['user'],
        password=server_config['password'],
        database=database
    )
    cursor = conn.cursor()
    with open (tables_sql, 'r') as f:
        create_tables_sql = f.read()
    cursor.execute(create_tables_sql)
    conn.commit()
    cursor.close()
    conn.close()

def create_infrastructure(server_config, new_database='fhir_database', database_sql='src/database/sql/database.sql', tables_sql='src/database/sql/tables.sql'):
    create_database(server_config=server_config, database_sql=database_sql)
    create_tables(server_config=server_config, tables_sql=tables_sql, database=new_database)

def main():
    """
    Creates the database and the tables
    """
    # Get args
    args = get_args()

    server_config = {'host': args.host, 'port': args.port, 'user': args.user, 'password': args.password}
    create_infrastructure(server_config=server_config, new_database=args.new_database, database_sql=args.db_sql, tables_sql=args.tables_sql)

def get_args() -> argparse.ArgumentParser:
    """
    Argument parser function for this file. Sets arguments and parses them for this file.

    :returns:
        Passed arguments.
    """
    # Argument parser
    parser = argparse.ArgumentParser()

    # Add arguments
    parser.add_argument(
        '--host', type=str, required=True,
        help='The host of the postgreSQL server.'
    )
    parser.add_argument(
        '--port', type=int, required=True,
        help='The port to connect to the postgreSQL server.'
    )
    parser.add_argument(
        '--user', type=str, required=True,
        help='The username to authenticate connection to the server.'
    )
    parser.add_argument(
        '--password', type=str, required=True,
        help='The password to authenticate connection to the server.'
    )
    parser.add_argument(
        '--database', type=str, required=False, default=None,
        help='A database to connect to. Not required.'
    )
    parser.add_argument(
        '--new-database', type=str, required=False, default='fhir_database',
        help='The database to create (specified in the database sql file).'
    )
    parser.add_argument(
        '--db_sql', type=str, required=False, default='src/database/sql/database.sql',
        help='The database sql file path.'
    )
    parser.add_argument(
        '--tables_sql', type=str, required=False, default='src/database/sql/tables.sql',
        help='The folder path for the sql of all the tables.'
    )

    # Parse args
    args = parser.parse_args()

    return args

if __name__ == '__main__':
    main()