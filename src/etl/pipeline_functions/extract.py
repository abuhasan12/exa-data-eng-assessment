"""
Extract function extract_resources() is used to return the resources of each file.
"""


import json


def extract_resources(json_file_paths: list):
    """
    Iterates over the files in the file path and parses JSON data from each file.
    Iterates over the json data and startes generating the 'resources' in the 'entry' json object'.

    :param json_file_paths:
        List of strings of valid file paths of the JSON files to process.
    """
    for json_file_path in json_file_paths:
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
                try:
                    for resource in json_data['entry']:
                        yield resource
                except (KeyError, TypeError) as e:
                    print(f"Could not extract resources: {e}")
        except (FileNotFoundError) as e:
            print(f"There was an error when opening file {json_file_path}: {e}")
        except json.JSONDecodeError as e:
            print(f"There was an error when decoding the file {json_file_path}: {e}")