import argparse
from src.utils import PostgresClient

def create_database(client, database_sql):
    client.connect_default()
    client.enable_autocommit()
    client.add_sql_file(database_sql)
    client.commit_sql()
    client.close_connection()
    
def create_tables(client, tables_sql, database):
    client.switch_database(database=database)
    client.connect()
    client.add_sql_file(tables_sql)
    client.commit_sql()
    client.close_connection()

def create_infrastructure(client, new_database='fhir_database', database_sql='src/database/sql/database.sql', tables_sql='src/database/sql/tables.sql'):
    create_database(client=client, database_sql=database_sql)
    create_tables(client=client, tables_sql=tables_sql, database=new_database)

def main():
    """
    Creates the database and the tables
    """
    # Get args
    args = get_args()

    client = PostgresClient(host=args.host, port=args.port, user=args.user, password=args.password, database=args.database)
    create_infrastructure(client=client, new_database=args.new_database, database_sql=args.db_sql, tables_sql=args.tables_sql)

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
        help='A database to connect to.'
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