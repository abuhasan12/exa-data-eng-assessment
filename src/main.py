import os
import configparser
from src.database.create_database import create_database
from src.etl.pipeline import pipeline

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

    create_database(server_config=server_config)
    pipeline(file_paths=file_paths, server_config=server_config)

if __name__ == '__main__':
    main()