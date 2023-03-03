"""
Extract functions extract_json() and extract_resources() are used to yield data one at a time to process.
extract_json() parses the json data of each file and extract_resources returns the resources one-by-one in the 'entry' list of each file.
"""


import json


def extract_json(json_file_paths: list):
    """
    Iterates over the files in the file path and parses JSON data from each file to generate for the processing iterations.

    :param json_file_paths:
        List of strings of valid file paths of the JSON files to process.
    """
    for json_file_path in json_file_paths:
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
                yield json_data
        except (FileNotFoundError) as e:
            print(f"There was an error when opening file {json_file_path}: {e}")
        except json.JSONDecodeError as e:
            print(f"There was an error when decoding the file {json_file_path}: {e}")


def extract_resources(json_file_paths: list):
    """
    Iterates over the JSON data returned by extract_json() and starts generating the iteration for the 'resources' in the 'entry' list.

    :param json_file_paths:
        List of strings of valid file paths of the JSON files to process.
    """
    for json_data in extract_json(json_file_paths):
        try:
            for resource in json_data['entry']:
                yield resource
        except (KeyError, TypeError) as e:
            print(f"Could not extract resources: {e}")