SCHEMAS = {
        'Encounter': {
            'table_meta': {
                'table_name': 'encounters',
            },
            'json_schema': {
                'id': ['fullUrl'],
                'request_method': ['request', 'method'],
                'resource_type': ['resource', 'resourceType'],
                'resource_id': ['resource', 'id'],
                'status': ['resource', 'status'],
                'class_code': ['resource', 'class', 'code'],
                'type_coding': ['resource', 'type', 0, 'coding'],
                'type': ['resource', 'type', 0, 'text'],
                'subject_reference': ['resource', 'subject', 'reference'],
                'subject': ['resource', 'subject', 'display'],
                'participant_type_coding': ['resource', 'participant', 0, 'type', 0, 'coding'],
                'participant_type': ['resource', 'participant', 0, 'type', 0, 'text'],
                'participant_period_start': ['resource', 'participant', 0, 'period', 'start'],
                'participant_period_end': ['resource', 'participant', 0, 'period', 'end'],
                'participant_individual_reference': ['resource', 'participant', 0, 'individual', 'reference'],
                'participant_individual': ['resource', 'participant', 0, 'individual', 'display'],
                'period_start': ['resource', 'period', 'start'],
                'period_end': ['resource', 'period', 'end'],
                'reason_code_coding': ['resource', 'reasonCode', 0, 'coding'],
                'hospitalization_discharge_disposition_coding': ['resource', 'hospitalization', 'dischargeDisposition', 'coding'],
                'hospitalization_discharge_disposition': ['resource', 'hospitalization', 'dischargeDisposition', 'text'],
                'location_reference': ['resource', 'location', 0, 'location', 'reference'],
                'location': ['resource', 'location', 0, 'location', 'display'],
                'service_provider_reference': ['resource', 'serviceProvider', 'reference'],
                'service_provider': ['resource', 'serviceProvider', 'display']
            }
        }
    }