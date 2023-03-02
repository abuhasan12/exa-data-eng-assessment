import sys

def upload_resources(conn, table_name, number_of_cols, rows):
    cursor = conn.cursor()
    placeholders = ','.join(["'{}'"] *number_of_cols)
    for row in rows:
        query = f"INSERT INTO {table_name} VALUES ({placeholders})".format(*row)
        try:
            cursor.execute(query)
        except Exception as e:
            print(e)
            print(query)
            sys.exit()
    conn.commit()
    cursor.close()