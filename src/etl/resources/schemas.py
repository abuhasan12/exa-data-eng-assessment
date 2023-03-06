SCHEMAS = {
        'AllergyIntolerance': {
            'table_meta': {
                'table_name': 'allergy_intolerances',
            },
            'json_schema': {
                'id': ['fullUrl'],
                'request_method': ['request', 'method'],
                'resource_type': ['resource', 'resourceType'],
                'resource_id': ['resource', 'id'],
                'clinical_status': ['resource', 'clinicalStatus', 'coding'],
                'verification_status': ['resource', 'verificationStatus', 'coding'],
                'type': ['resource', 'type'],
                'category': ['resource', 'category'],
                'criticality': ['resource', 'criticality'],
                'code_coding': ['resource', 'code', 'coding'],
                'patient_reference': ['resource', 'patient', 'reference'],
                'recorded_date': ['resource', 'recordedDate'],
                'reaction': ['resource', 'reaction']
            }
        },
        'CarePlan': {
            'table_meta': {
                'table_name': 'care_plans',
            },
            'json_schema': {
                'id': ['fullUrl'],
                'request_method': ['request', 'method'],
                'resource_type': ['resource', 'resourceType'],
                'resource_id': ['resource', 'id'],
                'status': ['resource', 'status'],
                'intent': ['resource', 'intent'],
                'category': ['resource', 'category'],
                'subject_reference': ['resource', 'subject', 'reference'],
                'encounter_reference': ['resource', 'encounter', 'reference'],
                'period_start': ['resource', 'period', 'start'],
                'period_end': ['resource', 'period', 'end'],
                'care_team': ['resource', 'careTeam'],
                'addresses': ['resource', 'addresses'],
                'activity': ['resource', 'activity']
            }
        },
        'Claim': {
            'table_meta': {
                'table_name': 'claims',
            },
            'json_schema': {
                'id': ['fullUrl'],
                'request_method': ['request', 'method'],
                'resource_type': ['resource', 'resourceType'],
                'resource_id': ['resource', 'id'],
                'status': ['resource', 'status'],
                'type_coding': ['resource', 'type', 'coding'],
                'use': ['resource', 'use'],
                'patient_reference': ['resource', 'patient', 'reference'],
                'patient': ['resource', 'patient', 'display'],
                'billable_period_start': ['resource', 'billablePeriod', 'start'],
                'billable_period_end': ['resource', 'billablePeriod', 'end'],
                'created': ['resource', 'created'],
                'provider_reference': ['resource', 'provider', 'reference'],
                'provider': ['resource', 'provider', 'display'],
                'priority_coding': ['resource', 'priority', 'coding'],
                'facility_reference': ['resource', 'facility', 'reference'],
                'facility': ['resource', 'facility', 'display'],
                'diagnosis': ['resource', 'diagnosis'],
                'supporting_info': ['resource', 'supportingInfo'],
                'procedure': ['resource', 'procedure'],
                'prescription_reference': ['resource', 'prescription', 'reference'],
                'insurance': ['resource', 'insurance'],
                'item': ['resource', 'item'],
                'total_value': ['resource', 'total', 'value'],
                'total_currency': ['resource', 'total' 'currency']
            }
        },
        'Condition': {
            'table_meta': {
                'table_name': 'conditions',
            },
            'json_schema': {
                'id': ['fullUrl'],
                'request_method': ['request', 'method'],
                'resource_type': ['resource', 'resourceType'],
                'resource_id': ['resource', 'id'],
                'clinical_status': ['resource', 'clinicalStatus', 'coding'],
                'verification_status': ['resource', 'verificationStatus', 'coding'],
                'category': ['resource', 'category'],
                'code_coding': ['resource', 'code', 'coding'],
                'code': ['resource', 'code', 'text'],
                'subject_reference': ['resource', 'subject', 'reference'],
                'encounter_reference': ['resource', 'encounter', 'reference'],
                'onset_date_time': ['resource', 'onsetDateTime'],
                'recorded_date': ['resource', 'recordedDate']
            }
        },
        'Device': {
            'table_meta': {
                'table_name': 'devices',
            },
            'json_schema': {
                'id': ['fullUrl'],
                'request_method': ['request', 'method'],
                'resource_type': ['resource', 'resourceType'],
                'resource_id': ['resource', 'id'],
                'udi_carrier': ['resource', 'udiCarrier'],
                'status': ['resource', 'status'],
                'distinct_identifier': ['resource', 'distinctIdentifier'],
                'manufacture_date': ['resource', 'manufactureDate'],
                'expiration_date': ['resource', 'expirationDate'],
                'lot_number': ['resource', 'lotNumber'],
                'serial_number': ['resource', 'serialNumber'],
                'device_name': ['resource', 'deviceName'],
                'type_coding': ['resource', 'type', 'coding'],
                'patient_reference': ['resource', 'patient', 'reference']
            }
        },
        'DiagnosticReport': {
            'table_meta': {
                'table_name': 'diagnostic_reports',
            },
            'json_schema': {
                'id': ['fullUrl'],
                'request_method': ['request', 'method'],
                'resource_type': ['resource', 'resourceType'],
                'resource_id': ['resource', 'id'],
                'status': ['resource', 'status'],
                'category': ['resource', 'category'],
                'code_coding': ['resource', 'code', 'coding'],
                'code': ['resource', 'code', 'text'],
                'subject_reference': ['resource', 'subject', 'reference'],
                'encounter_reference': ['resource', 'encounter', 'reference'],
                'effective_date_time': ['resource', 'effectiveDateTime'],
                'issued_date_time': ['resource', 'issued'],
                'performer': ['resource', 'performer'],
                'result': ['resource', 'result'],
                'presented_form': ['resource', 'presentedForm']
            }
        },
        'DocumentReference': {
            'table_meta': {
                'table_name': 'document_references',
            },
            'json_schema': {
                'id': ['fullUrl'],
                'request_method': ['request', 'method'],
                'resource_type': ['resource', 'resourceType'],
                'resource_id': ['resource', 'id'],
                'status': ['resource', 'status'],
                'type_coding': ['resource', 'type', 'coding'],
                'category': ['resource', 'category'],
                'subject_reference': ['resource', 'subject', 'reference'],
                'date': ['resource', 'date'],
                'author': ['resource', 'author'],
                'custodian_reference': ['resource', 'custodian', 'reference'],
                'custodian': ['resource', 'custodian', 'display'],
                'content': ['resource', 'content'],
                'context_encounter': ['resource', 'context', 'encounter'],
                'context_period_start': ['resource', 'context', 'period', 'start'],
                'context_period_end': ['resource', 'context', 'period', 'end']
            }
        },
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
                'type': ['resource', 'type'],
                'subject_reference': ['resource', 'subject', 'reference'],
                'subject': ['resource', 'subject', 'display'],
                'participant': ['resource', 'participant'],
                'period_start': ['resource', 'period', 'start'],
                'period_end': ['resource', 'period', 'end'],
                'reason_code': ['resource', 'reasonCode'],
                'hospitalization_discharge_disposition_coding': ['resource', 'hospitalization', 'dischargeDisposition', 'coding'],
                'hospitalization_discharge_disposition': ['resource', 'hospitalization', 'dischargeDisposition', 'text'],
                'location': ['resource', 'location'],
                'service_provider_reference': ['resource', 'serviceProvider', 'reference'],
                'service_provider': ['resource', 'serviceProvider', 'display']
            }
        },
        'ExplanationOfBenefit': {
            'table_meta': {
                'table_name': 'explanation_of_benefits',
            },
            'json_schema': {
                'id': ['fullUrl'],
                'request_method': ['request', 'method'],
                'resource_type': ['resource', 'resourceType'],
                'resource_id': ['resource', 'id'],
                'contained': ['resource', 'contained'],
                'identifier': ['resource', 'identifier'],
                'status': ['resource', 'status'],
                'type_coding': ['resource', 'type', 'coding'],
                'use': ['resource', 'use'],
                'patient_reference': ['resource', 'patient', 'reference'],
                'billable_period_start': ['resource', 'billablePeriod', 'start'],
                'billable_period_end': ['resource', 'billablePeriod', 'end'],
                'created': ['resource', 'created'],
                'insurer': ['resource', 'insurer', 'display'],
                'provider_reference': ['resource', 'provider', 'reference'],
                'referral_reference': ['resource', 'referral', 'reference'],
                'facility_reference': ['resource', 'facility', 'reference'],
                'facility': ['resource', 'facility', 'display'],
                'claim_reference': ['resource', 'claim', 'reference'],
                'outcome': ['resource', 'outcome'],
                'outcome': ['resource', 'careTeam'],
                'diagnosis': ['resource', 'diagnosis'],
                'insurance': ['resource', 'insurance'],
                'item': ['resource', 'item'],
                'total': ['resource', 'total'],
                'payment_amount_value': ['resource', 'payment' 'amount', 'value'],
                'payment_amount_currency': ['resource', 'payment', 'amount', 'currency']
            }
        },
        'ImagingStudy': {
            'table_meta': {
                'table_name': 'imaging_studies',
            },
            'json_schema': {
                'id': ['fullUrl'],
                'request_method': ['request', 'method'],
                'resource_type': ['resource', 'resourceType'],
                'resource_id': ['resource', 'id'],
                'identifier': ['resource', 'identifier'],
                'status': ['resource', 'status'],
                'subject_reference': ['resource', 'subject', 'reference'],
                'encounter_reference': ['resource', 'encounter', 'reference'],
                'started_date_time': ['resource', 'started'],
                'num_series': ['resource', 'numberOfSeries'],
                'num_instances': ['resource', 'numberOfInstances'],
                'procedure': ['resource', 'procedureCode'],
                'location_reference': ['resource', 'location', 'reference'],
                'location': ['resource', 'location', 'display'],
                'series': ['resource', 'series']
            }
        },
        'Immunization': {
            'table_meta': {
                'table_name': 'immunizations',
            },
            'json_schema': {
                'id': ['fullUrl'],
                'request_method': ['request', 'method'],
                'resource_type': ['resource', 'resourceType'],
                'resource_id': ['resource', 'id'],
                'status': ['resource', 'status'],
                'vaccine_code': ['resource', 'vaccineCode', 'coding'],
                'vaccine': ['resource', 'vaccineCode', 'text'],
                'patient_reference': ['resource', 'patient', 'reference'],
                'encounter_reference': ['resource', 'encounter', 'reference'],
                'occurence_date_time': ['resource', 'occurrenceDateTime'],
                'primary_source': ['resource', 'primarySource'],
                'location_reference': ['resource', 'location', 'reference'],
                'location': ['resource', 'location', 'display']
            }
        },
        'Medication': {
            'table_meta': {
                'table_name': 'medications',
            },
            'json_schema': {
                'id': ['fullUrl'],
                'request_method': ['request', 'method'],
                'resource_type': ['resource', 'resourceType'],
                'resource_id': ['resource', 'id'],
                'code_coding': ['resource', 'code', 'coding'],
                'status': ['resource', 'status']
            }
        },
        'MedicationAdministration': {
            'table_meta': {
                'table_name': 'medicaiton_administrations',
            },
            'json_schema': {
                'id': ['fullUrl'],
                'request_method': ['request', 'method'],
                'resource_type': ['resource', 'resourceType'],
                'resource_id': ['resource', 'id'],
                'medication_code': ['resource', 'medicationCodeableConcept', 'coding'],
                'medication': ['resource', 'medicationCodeableConcept', 'text'],
                'subject_reference': ['resource', 'subject', 'reference'],
                'context_reference': ['resource', 'context', 'reference'],
                'effective_date_time': ['resource', 'effectiveDateTime'],
                'reason_reference': ['resource', 'reasonReference'],
                'dosage_value': ['resource', 'dosage', 'dose', 'value'],
                'dosage_rate': ['resource', 'dosage', 'rateQuantity', 'value']
            }
        },
        'MedicationRequest': {
            'table_meta': {
                'table_name': 'medication_requests',
            },
            'json_schema': {
                'id': ['fullUrl'],
                'request_method': ['request', 'method'],
                'resource_type': ['resource', 'resourceType'],
                'resource_id': ['resource', 'id'],
                'status': ['resource', 'status'],
                'intent': ['resource', 'intent'],
                'medication_reference': ['resource', 'medicationReference', 'reference'],
                'medication_coding': ['resource', 'medication', 'coding'],
                'medication': ['resource', 'medication', 'text'],
                'subject_reference': ['resource', 'subject', 'reference'],
                'encounter_reference': ['resource', 'encounter', 'reference'],
                'authored_on': ['resource', 'authoredOn'],
                'requester_reference': ['resource', 'requester', 'reference'],
                'requester': ['resource', 'requester', 'display'],
                'reason_reference': ['resource', 'reasonReference'],
                'dosage_instruction': ['resource', 'dosageInstruction'],
            }
        },
        'Observation': {
            'table_meta': {
                'table_name': 'observations',
            },
            'json_schema': {
                'id': ['fullUrl'],
                'request_method': ['request', 'method'],
                'resource_type': ['resource', 'resourceType'],
                'resource_id': ['resource', 'id'],
                'status': ['resource', 'status'],
                'category': ['resource', 'category'],
                'code_coding': ['resource', 'code', 'coding'],
                'subject_reference': ['resource', 'subject', 'reference'],
                'encounter_reference': ['resource', 'encounter', 'reference'],
                'effective_date_time': ['resource', 'effectiveDateTime'],
                'issued_date_time': ['resource', 'issued'],
                'component': ['resource', 'component'],
                'value_code': ['resource', 'valueCodeableConcept', 'coding'],
                'value_text': ['resource', 'valueCodeableConcept', 'text'],
                'value': ['resource', 'valueQuantity', 'value'],
                'unit': ['resource', 'valueQuantity', 'unit']
            }
        },
        'Patient': {
            'table_meta': {
                'table_name': 'patients',
            },
            'json_schema': {
                'id': ['fullUrl'],
                'request_method': ['request', 'method'],
                'resource_type': ['resource', 'resourceType'],
                'resource_id': ['resource', 'id'],
                'extension': ['resource', 'extension'],
                'identifier': ['resource', 'identifier'],
                'name': ['resource', 'name'],
                'contact': ['resource', 'telecom'],
                'gender': ['resource', 'gender'],
                'birth_date': ['resource', 'birthDate'],
                'deceased_date_time': ['resource', 'deceasedDateTime'],
                'address': ['resource', 'address'],
                'marital_status_coding': ['resource', 'maritalStatus', 'coding'],
                'marital_status': ['resource', 'maritalStatus', 'text'],
                'multiple_birth': ['resource', 'multipleBirthBoolean'],
                'multiple_birth_amount': ['resource', 'multipleBirthInteger'],
                'communication': ['resource', 'communication']
            }
        },
        'Procedure': {
            'table_meta': {
                'table_name': 'procedures',
            },
            'json_schema': {
                'id': ['fullUrl'],
                'request_method': ['request', 'method'],
                'resource_type': ['resource', 'resourceType'],
                'resource_id': ['resource', 'id'],
                'status': ['resource', 'status'],
                'code_coding': ['resource', 'code', 'coding'],
                'code': ['resource', 'code', 'text'],
                'subject_reference': ['resource', 'subject', 'reference'],
                'encounter_reference': ['resource', 'encounter', 'reference'],
                'performed_period_start': ['resource', 'performedPeriod', 'start'],
                'performed_period_end': ['resource', 'performedPeriod', 'end'],
                'location_reference': ['resource', 'location', 'reference'],
                'location': ['resource', 'location', 'display'],
                'reason_reference': ['resource', 'reasonReference'],
            }
        },
        'Provenance': {
            'table_meta': {
                'table_name': 'provenances',
            },
            'json_schema': {
                'id': ['fullUrl'],
                'request_method': ['request', 'method'],
                'resource_type': ['resource', 'resourceType'],
                'resource_id': ['resource', 'id'],
                'target': ['resource', 'target'],
                'recorded_date': ['resource', 'recorded'],
                'agent': ['resource', 'agent']
            }
        },
        'SupplyDelivery': {
            'table_meta': {
                'table_name': 'supply_deliveries',
            },
            'json_schema': {
                'id': ['fullUrl'],
                'request_method': ['request', 'method'],
                'resource_type': ['resource', 'resourceType'],
                'resource_id': ['resource', 'id'],
                'patient_reference': ['resource', 'patient', 'reference'],
                'type_coding': ['resource', 'type', 'coding'],
                'supplied_quantity': ['resource', 'suppliedItem', 'quantity', 'value'],
                'item_code': ['resource', 'itemCodeableConcept', 'coding'],
                'item': ['resource', 'itemCodeableConcept', 'text'],
                'occurence_date_time': ['resource', 'occurrenceDateTime']
            }
        }
    }