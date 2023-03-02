import configparser
import psycopg2

def execute_sql_file(conn, sql_file):
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

def create_database(server_config):
    conn = psycopg2.connect(
        host=server_config['HOST'],
        port=server_config['PORT'],
        user=server_config['USER'],
        password=server_config['PASSWORD']
    )
    conn.autocommit = True
    # Create database
    execute_sql_file(conn=conn, sql_file='src/database/database_sql/database.sql')
    conn.close()
    conn = psycopg2.connect(
        host=server_config['HOST'],
        port=server_config['PORT'],
        user=server_config['USER'],
        password=server_config['PASSWORD'],
        database='fhir_database'
    )
    # Create tables in database
    execute_sql_file(conn=conn, sql_file='src/database/database_sql/tables.sql')
    conn.close()

def main():
    """
    Creates the database and the tables
    """
    config = configparser.ConfigParser()
    config.read('config.ini')

    server_config = {
        'HOST': config.get('Server', 'HOST'),
        'PORT': config.get('Server', 'PORT'),
        'USER': config.get('Server', 'USER'),
        'PASSWORD': config.get('Server', 'PASSWORD'),
        'DATABASE': config.get('Server', 'DATABASE', fallback=None)
    }

    create_database(server_config=server_config)

if __name__ == '__main__':
    main()