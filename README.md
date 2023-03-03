# FHIR Transaction Bundle Data Loader
This program loads FHIR Transaction Bundle data to resource tables in a postgreSQL database called 'fhir_database'.

## Demo

![result](https://github.com/abuhasan12/exa-data-eng-assessment/blob/main/demo/result.png)

![data](https://github.com/abuhasan12/exa-data-eng-assessment/blob/main/demo/data.png)

## To Run using Pip

* Installation:
```Command Line
$ pip install fhir-load
```
* To run:
```Command Line
$ fhir-load --path <path/to/data/directory> --host <postgresql-host> --port <postgresql-port> --user <postgresql-user> --password <postgresql-password> [--database <postgresql-database>]
```
If you are using localhost for postgreSQL, use the 'localhost' in place of 'postgresql-host'

## To Run on Docker

* Run the following command:
```Command Line
$ docker run --network=host -v '<path/to/data/directory>':'/fhir-load/data' fhir-load --host <postgresql-host> --port <postgresql-port> --user <postgresql-user> --password <postgresql-password>
```
* If you want logging:
```Command Line
$ docker run --network=host -v '<path/to/data/directory>':'/fhir-load/data' -v 'path/to/logs/directory':'/fhir-load/logs' abuh12/fhir-load --host <postgresql-host> --port <postgresql-port> --user <postgresql-user> --password <postgresql-password>
```
If you are using localhost for postgreSQL, use the IP address in place of 'postgresql-host'

## To Run from clone

* If you have cloned the repo, ensure you add the repository to the python path:
```Command Line
$ export PYTHON PATH="path/to/repo"
```
* From inside the repo on your terminal run (make sure you have psycopg2 installed):
```Command Line
$ python -m fhir-load --path <path/to/data/directory> --host <postgresql-host> --port <postgresql-port> --user <postgresql-user> --password <postgresql-password> [--database <postgresql-database>]
```
If you are using localhost for postgreSQL, use the 'localhost' in place of 'postgresql-host'

## Next Steps

Will add support for more resources.
Currently only the following resources are supported:

* CarePlan
* Claim
* Condition
* DiagnosticReport
* DocumentReference
* Encounter
* ExplanationOfBenefit
* MedicationRequest
* Patient
* Procedure