import json

def extract_json(file_path):
    try:    
        with open(file_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
            yield json_data
    except (FileNotFoundError, IOError, OSError) as e:
        print(f"There was an error when opening file {file_path} to read: {e}")
    except json.JSONDecodeError as e:
        print(f"There was an error when decoding the file {file_path}: {e}")

def extract_resources(file_path):
    for json_data in extract_json(file_path):
        try:
            for resource in json_data['entry']:
                yield resource
        except (KeyError, TypeError) as e:
            print(f"Could not extract resources for {file_path}: {e}")