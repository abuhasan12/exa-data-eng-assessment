from src.etl.pipeline import extract

def pipeline(file_paths):
    for file_path in file_paths:
        for resource in extract.extract_resources(file_path):
            if 'resource' not in resource and 'resourceType' not in resource:
                if 'fullUrl' in resource:
                    print(f"Resource with {resource['fullUrl']} ID not formatted properly. Skipping.")
                elif 'id' in resource:
                    print(f"Resource with {resource['id']} ID not formatted properly. Skipping.")
                else:
                    print(f"Resource in {file_path} not formatted properly. Could not locate. Skipping.")
                continue
            resource_type = resource['resource']['resourceType']
            transformed_resource = transform_resource(resource)
            data[resource_type] = data[resource_type].append(transformed_resource)
            if len(data[resource_type]) === 1000:
                load_resources(data[resource_type])
                data[resource_type] = []
    for resource_type in data:
        if data[resource_type]:
            load_resources(data[resource_type])