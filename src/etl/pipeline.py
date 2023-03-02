import os
from src.etl.pipeline_functions import extract, transform
import sys

data_dir = "data/"
file_paths = [os.path.join(data_dir, file_name) for file_name in os.listdir(data_dir)]

data = {
    'Encounter': []
}

def pipeline(file_paths):
    for resource in extract.extract_resources(file_paths):
        if 'resource' not in resource and 'resourceType' not in resource:
            if 'fullUrl' in resource:
                print(f"Resource with {resource['fullUrl']} ID not formatted properly. Skipping.")
            elif 'id' in resource:
                print(f"Resource with {resource['id']} ID not formatted properly. Skipping.")
            else:
                print(f"{resource} not formatted properly. Skipping.")
            continue
        # print(resource)
        # sys.exit()
        resource_type = resource['resource']['resourceType']
        if resource['resource']['resourceType'] == 'Encounter':
            transformed_resource = transform.transform_resource(resource)
            data[resource_type].append(transformed_resource)
            # print(data[resource_type])
            # sys.exit()
        if len(data[resource_type]) === 1000:
            load_resources(data[resource_type])
            data[resource_type] = []
pipeline(file_paths)
    # for resource_type in data:
    #     if data[resource_type]:
    #         load_resources(data[resource_type])