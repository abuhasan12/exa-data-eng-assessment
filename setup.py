from setuptools import setup, find_packages

setup(
    name='fhir-load',
    version='1.0',
    description='This program loads FHIR Transaction Bundle data to resource tables in a postgreSQL database called fhir_database.',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'fhir-load=src.fhir_load:main'
        ]
    },
    install_requires=[
        'psycopg2==2.9.5'
    ],
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
)