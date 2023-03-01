import os
import configparser
import json
import psycopg2
from schemas import SCHEMAS

data_dir = "data/"
file_paths = [os.path.join(data_dir, file_name) for file_name in os.listdir(data_dir)]
config = configparser.ConfigParser()
config.read('config.ini')

conn = psycopg2.connect(
    host=config.get('Server', 'HOST'),
    port=config.get('Server', 'PORT'),
    user=config.get('Server', 'USER'),
    password=config.get('Server', 'PASSWORD'),
    database="fhir_database"
)
cursor = conn.cursor()

def load_resource(json_data):
    for resource in json_data['entry']:
        yield resource

def load_json(file_path):
    with open(file_path,'r', encoding='utf-8') as f:
        json_data = json.load(f)
        yield json_data

data = {}

for file_path in file_paths:
    for json_data in load_json(file_path=file_path):
        for r in load_resource(json_data):
            resource_type = r['resource']['resourceType']
            if resource_type == 'Encounter':
                if resource_type not in data:
                    data[resource_type] = []
                row = []
                for key, path in SCHEMAS[r['resource']['resourceType']].items():
                    value = r
                    for nested_key in path:
                        if isinstance(value, dict):
                            value = value.get(nested_key, 'null')
                        elif isinstance(value, list):
                            value = value[nested_key]
                    if isinstance(value, list):
                        value = json.dumps(value, ensure_ascii=False)
                    if isinstance(value, str):
                        value = value.replace("'", "''")
                    row.append(value)
                data[resource_type].append(row)

i = 0
for row in data['Encounter']:
    query = "INSERT INTO encounters VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')".format(*row)
    cursor.execute(query)
    i += 1
    if i == 1000:
        conn.commit()
        i = 0
conn.commit()
cursor.close()
conn.close()