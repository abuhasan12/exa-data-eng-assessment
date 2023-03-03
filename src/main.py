"""
Main file for complete FHIR Database creator and data processing pipeline.
The complete solution creates a database fhir_database in a postgreSQL server.
The data is held in a 'data' folder, and the database configuration details are held in the config.ini file in the root directory.
"""


import os
import configparser

from src.database.create_database import create_database
from src.etl.pipeline import pipeline


def main():
    """
    Running this file will create the fhir_database and necessary tables in the postgreSQL server specified in the config.ini file.
    Then, insert all the processed data extracted and processed from each file to the database.
    """
    # Specify the directory holding the data.
    data_dir = "data/"
    file_paths = [os.path.join(data_dir, file_name) for file_name in os.listdir(data_dir)]

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

    # Create database.
    create_database(server_config=server_config)
    # Run ETL pipeline.
    pipeline(file_paths=file_paths, server_config=server_config)

if __name__ == '__main__':
    main()