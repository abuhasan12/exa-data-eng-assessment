import json
from src.etl.transform_resources.schemas import SCHEMAS

def get_value_for_col(resource, path_to_value):
    value = resource
    for nested_key in path_to_value:
        if isinstance(value, dict):
            value = value.get(nested_key, 'null')
        elif isinstance(value, list):
            value = value[nested_key]
    if isinstance(value, list):
        value = json.dumps(value, ensure_ascii=False)
    if isinstance(value, str):
        value = value.replace("'", "''")
    return value

def transform_resource(resource):
    schema = SCHEMAS[resource['resource']['resourceType']]
    new_row = []
    for col, path_to_value in schema.items():
        value = get_value_for_col(resource, path_to_value)
        new_row.append(value)
    return new_row