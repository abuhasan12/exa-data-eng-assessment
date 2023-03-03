"""
Main file for complete FHIR Database creator and data processing pipeline.
The complete solution creates a database fhir_database in a postgreSQL server.
The data is held in a 'data' folder, and the database configuration details are held in the config.ini file in the root directory.
"""


import os
import sys
import argparse

from src.database.create_database import create_database
from src.etl.pipeline import pipeline


def get_args() -> argparse.ArgumentParser:
    """
    Argument parser function for this file. Sets arguments and parses them for this file.

    :returns:
        Parsed arguments.
    """
    # Argument parser
    parser = argparse.ArgumentParser()

    # Add arguments
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
    parser.add_argument(
        '--path', type=str, required=False, default=None,
        help='The path to the data directory.'
    )

    # Parse args
    args = parser.parse_args()

    return args


def main():
    """
    Running this file will create the fhir_database and necessary tables in the postgreSQL server specified in the config.ini file.
    Then, insert all the processed data extracted and processed from each file to the database.
    """
    # Get args
    args = get_args()

    data_dir = args.path

    if not data_dir:
        if os.environ.get('DOCKER_CONTAINER'):
            data_dir = 'data/'
        else:
            print("""usage: fhir-load.py [-h] --path PATH --host HOST --port PORT --user USER
                        --password PASSWORD [--database DATABASE]
                \nthe following arguments are required: --path, --host, --port, --user, --password
            """)
            sys.exit(1)

    if not os.path.isdir(data_dir):
        print(f"{data_dir} is not a folder.")
        sys.exit(1)
    
    for file_path in os.listdir(data_dir):
        name, extension = os.path.splitext(file_path)
        if extension != '.json':
            print(f"{file_path} is not a JSON file.")
            sys.exit(1)
    
    json_file_paths = [os.path.join(data_dir, file_name) for file_name in os.listdir(data_dir)]
    
    server_config = {
        'HOST': args.host,
        'PORT': args.port,
        'USER': args.user,
        'PASSWORD': args.password,
        'DATABASE': args.database
    }
    print("Server credentials loaded.")

    # Create database.
    create_database(server_config=server_config)
    # Run ETL pipeline.
    pipeline(json_file_paths=json_file_paths, server_config=server_config)

if __name__ == '__main__':
    main()