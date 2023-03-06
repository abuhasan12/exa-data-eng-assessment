"""
The main ETL pipeline file.
Extracts the JSON data, one resource at a time, from the JSON files in the specified file path.
Transforms the JSON data by converting each dictionary-like structure to a row of values.
Loads the rows generated by the transformations to the database tables in batches.
"""


import os
import sys
import argparse
import time

from src.database.database_functions.database_connection import create_connection
from src.etl.pipeline_functions import extract, transform, load
from src.etl.resources.schemas import SCHEMAS


def get_duplicate_entries_count(logs_dir):
    dup_str = 'duplicate key value'

    log_files = [os.path.join(logs_dir, file_name) for file_name in os.listdir(logs_dir)]
    log_file = sorted(log_files, key=os.path.getmtime, reverse=True)[0]

    count = 0
    with open(log_file, 'r') as f:
        for line in f:
            count += line.count(dup_str)
    
    return count


def pipeline(json_file_paths: list, server_config: dict):
    """
    Pipeline function creates a dictionary to hold the lists of all the rows for each resource type to load into the database.
    Calls the extract_resources() function to extract each resource one by one from all the files, loops over them, and transforms them to rows.
    Each returned row by transform_resource() is appended to the dictionary for that resrouce type.
    If any list reaches 1000 items, the 1000 rows are inserted to the table for that resource as a batch.
    Once the iterations are completed, any remaining rows in the dictionary are inserted.

    :param json_file_paths:
        List of strings of valid file paths of the JSON files to process.
    :param server_config:
        The database server configuration dictionary which includes host, port, user, password, and database information.
    """
    # Logging
    logs_dir = 'logs'
    if not os.path.exists(logs_dir):
        try:
            os.makedirs(logs_dir)
        except Exception as e:
            raise Exception(f"Could not create directory for logs due to error: {e}")
    num_logs = len(os.listdir(logs_dir))

    # Initialise dictionary to hold the rows of data.
    data_rows = {
        'CarePlan': [],
        'Claim': [],
        'Condition': [],
        'DiagnosticReport': [],
        'DocumentReference': [],
        'Encounter': [],
        'ExplanationOfBenefit': [],
        'MedicationRequest': [],
        'Patient': [],
        'Procedure': []
    }
    
    # Connect to database.
    conn = create_connection(server_config=server_config)

    print(f"Loading {len(json_file_paths)} files to database.")
    start_time = time.time()
    
    # Resources count
    i = 0

    # Iterate over resources from the JSON data extracted from the files.
    for resource in extract.extract_resources(json_file_paths=json_file_paths):


        # Only transform resource types in dictionary.
        if resource['resource']['resourceType'] in data_rows:
            transformed_resource = transform.transform_resource(resource)
            
            resource_type = resource['resource']['resourceType']

            # Append the transformed row to the dictionary.
            data_rows[resource_type].append(transformed_resource)
        
            # If row amounts reach 10000, insert to database as batch.
            if len(data_rows[resource_type]) == 10000:
                table_name = SCHEMAS[resource_type]['table_meta']['table_name']
                num_cols = len(SCHEMAS[resource_type]['json_schema'])

                load.upload_resources(conn=conn, table_name=table_name, num_cols=num_cols, rows=data_rows[resource_type])

                # Empty list of rows for next batch.
                data_rows[resource_type] = []

            i += 1

    # Loop through the data if any left and insert the data as batches to table
    for resource_type, rows in data_rows.items():
        if rows:
            table_name = SCHEMAS[resource_type]['table_meta']['table_name']
            num_cols = len(SCHEMAS[resource_type]['json_schema'])

            load.upload_resources(conn=conn, table_name=table_name, num_cols=num_cols, rows=rows)
    
    end_time = time.time()

    conn.close()

    duplicates = get_duplicate_entries_count(logs_dir=logs_dir)

    i -= duplicates

    print(f"{i} resources extracted, processed, and loaded to database in {end_time - start_time} seconds.")

    if len(os.listdir(logs_dir)) > num_logs:
        print("WARNING: There were some errors when inserting to database. Check log file.")


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
    Running this file will insert all the processed data extracted and processed from each file to the database.
    """
    # Specify the directory holding the data.
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
    
    # Run pipeline.
    pipeline(json_file_paths=json_file_paths, server_config=server_config)

if __name__ == '__main__':
    main()