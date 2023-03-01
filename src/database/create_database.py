import argparse
import configparser
import psycopg2

def execute_sql_file(server_config, sql_file, replace_database=None, autocommit=False):
    if replace_database:
        server_config['DATABASE'] = replace_database

    if not server_config['DATABASE']:
        conn = psycopg2.connect(
            host=server_config['HOST'],
            port=server_config['PORT'],
            user=server_config['USER'],
            password=server_config['PASSWORD']
        )
    else:
        conn = psycopg2.connect(
            host=server_config['HOST'],
            port=server_config['PORT'],
            user=server_config['USER'],
            password=server_config['PASSWORD'],
            database=server_config['DATABASE']
        )
    conn.autocommit = autocommit
    cursor = conn.cursor()
    with open(sql_file, 'r') as f:
        sql_statement = ''
        for each_line in f:
            if each_line.startswith('--'):
                continue
            sql_statement += each_line.strip() + ' '
            if sql_statement.strip().endswith(';'):
                cursor.execute(sql_statement)
                sql_statement = ''
    conn.commit()
    cursor.close()
    conn.close()

def create_database(server_config, new_database='fhir_database', database_sql='src/database/sql/database.sql', tables_sql='src/database/sql/tables.sql'):
    # Create database
    execute_sql_file(server_config=server_config, sql_file=database_sql, autocommit=True)
    # Create tables in database
    execute_sql_file(server_config=server_config, sql_file=tables_sql, replace_database=new_database)

def main():
    """
    Creates the database and the tables
    """
    # Get args
    args = get_args()

    config = configparser.ConfigParser()
    config.read(args.config_file)

    server_config = {
        'HOST': config.get('Server', 'HOST'),
        'PORT': config.get('Server', 'PORT'),
        'USER': config.get('Server', 'USER'),
        'PASSWORD': config.get('Server', 'PASSWORD'),
        'DATABASE': config.get('Server', 'DATABASE', fallback=None)
    }

    create_database(server_config=server_config, new_database=args.new_database, database_sql=args.db_sql, tables_sql=args.tables_sql)

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
        '--config_file', type=str, required=False, default='config.ini',
        help='The config file of the postgreSQL server'
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