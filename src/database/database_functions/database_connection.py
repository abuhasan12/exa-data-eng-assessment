"""
Utility file to make database connections.
"""


import psycopg2
import psycopg2.extensions


def create_connection(server_config: dict) -> psycopg2.extensions.connection:
    """
    Creates a connection to the database using the credentials provided by the config.ini file.
    If database_connection is False, connects to default database.

    :param server_config:
        The database server configuration dictionary which includes host, port, user, password, and database information.
    :param database_connection:
        True of False, whether or not to connect to the database in the config.ini file (otherwise connect to the default database).

    :returns:
        postgreSQL database server connection object
    """
    try:
        if server_config['DATABASE']:
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
        print("Connection to server established.")
        return conn
    except psycopg2.OperationalError as e:
        raise Exception(f"Error connecting to postgreSQL server due to error: {e}")