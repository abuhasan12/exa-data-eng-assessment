"""
Transform function transform_resource() returns a newly created row which contains the values to insert to the table that the resource references.
It creates the row by extracting the values from JSON structured data, some of which is nested, by calling on the get_value_for_col() function.
"""


import json

from src.etl.resources.schemas import SCHEMAS


def get_value_for_col(resource: dict, path_to_value: list):
    """
    Iterative function to retrieve the required value by iterating into the nested layers of the resource dictionary.

    :param resource:
        JSON data containing values nested in that need to be extracted to insert to the table of the resource type.
    :param path_to_value:
        List containing the key path to the nested value within the JSON data.
    """
    # Initialise value to the full JSON data.
    value = resource

    # Iterate into the JSON data to retrieve the nested value required.
    for nested_key in path_to_value:
        if isinstance(value, dict):
            value = value.get(nested_key, 'null')

    # Reformatting necessary to match data types for SQL tables.        
    if isinstance(value, list):
        value = json.dumps(value, ensure_ascii=False)
    if isinstance(value, str):
        value = value.replace("'", "''")

    return value


def transform_resource(resource: dict) -> list:
    """
    Uses a stored 'schema' (JSON structure) for each resource type to extract the values needed for the columns in that resource table.
    Appends these values to a list to represent a row which will be inserted to the table.

    :param resource:
        JSON data containing values nested in that need to be extracted to insert to the table of the resource type.

    :returns:
        The row of extracted values as a list.
    """
    # Map for columns to the values nested in the JSON data.
    schema = SCHEMAS[resource['resource']['resourceType']]['json_schema']

    new_row = []

    # Iterate over each 'column' in the schema to get the value for that column.
    for col, path_to_value in schema.items():
        value = get_value_for_col(resource=resource, path_to_value=path_to_value)
        # Add the value to the row.
        new_row.append(value)

    return new_row