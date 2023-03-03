
"""
Load function upload_resource() batch inserts rows into a table.
The function receives the number of columns to create the value_placeholders in the SQL query string.
"""


import os
import logging
import datetime

import psycopg2.errors
import psycopg2.extensions


errors = False


def upload_resources(conn: psycopg2.extensions.connection, table_name: str, num_cols: int, rows: list):
    """
    Inserts the batch of rows to the table referenced using a dynamically formatted SQL string.
    
    :param conn:
        Database connection object.
    :param table_name:
        The table name to insert the records to.
    :param num_cols:
        The number of columns of the table.
    :param rows:
        The list of rows to insert into the table.
    """
    cursor = conn.cursor()

    # Create number of value_placeholders from the number of columns.
    value_placeholders = ','.join(["'{}'"] *num_cols)

    # Iterate over the rows to insert.
    for row in rows:
        query = f"INSERT INTO {table_name} VALUES ({value_placeholders})".format(*row)

        try:
            cursor.execute(query)
        except psycopg2.errors.UniqueViolation as e:
            global errors
            if not errors:
                errors = True
                log_file = os.path.join('logs', datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S.log"))
                logging.basicConfig(filename=log_file, level=logging.ERROR)
            logging.error(f"Could not insert row to {table_name} due to error:\n{e}")
            conn.commit()
        except Exception as e:
            raise Exception(f"Could not execute query:\n{query}\nError:\n{e}")

    # Commit query executions.        
    conn.commit()
    
    cursor.close()