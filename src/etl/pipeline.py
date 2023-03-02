import os
import configparser
from src.etl.pipeline_functions import extract, transform, load
import sys
import psycopg2

def check_resource_format(resource):
    if 'resource' not in resource:
        if 'fullUrl' in resource:
            print(f"Resource with {resource['fullUrl']} ID not formatted properly. Skipping.")
        else:
            print(f"{resource} not formatted properly. Skipping.")
        return False
    return True

def pipeline(file_paths, server_config):
    data_rows = {
        'Encounter': []
    }
    
    conn = psycopg2.connect(
        host=server_config['HOST'],
        port=server_config['PORT'],
        user=server_config['USER'],
        password=server_config['PASSWORD'],
        database=server_config['DATABASE']
    )

    for resource in extract.extract_resources(file_paths):
        if not check_resource_format(resource):
            continue

        resource_type = resource['resource']['resourceType']

        if resource['resource']['resourceType'] == 'Encounter':
            transformed_resource = transform.transform_resource(resource)
            data_rows[resource_type].append(transformed_resource)
        
            if len(data_rows[resource_type]) == 1000:
                load.upload_resources(conn, resource_type, data_rows[resource_type])
                data_rows[resource_type] = []

    for resource_type, rows in data_rows.items():
        if rows:
            load.upload_resources(conn, resource_type, rows)
            
    conn.close()

def main():
    data_dir = "data/"
    file_paths = [os.path.join(data_dir, file_name) for file_name in os.listdir(data_dir)]

    config = configparser.ConfigParser()
    config.read('config.ini')

    server_config = {
        'HOST': config.get('Server', 'HOST'),
        'PORT': config.get('Server', 'PORT'),
        'USER': config.get('Server', 'USER'),
        'PASSWORD': config.get('Server', 'PASSWORD'),
        'DATABASE': config.get('Server', 'DATABASE')
    }

    pipeline(file_paths=file_paths, server_config=server_config)

if __name__ == '__main__':
    main()