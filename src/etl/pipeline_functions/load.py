def upload_resources(conn, resource_type, rows):
    cursor = conn.cursor()
    if resource_type == 'Encounter':
        for row in rows:
            query = "INSERT INTO encounters VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')".format(*row)
            cursor.execute(query)
        conn.commit()
    cursor.close()