def get_table_names(connection, database_name):
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{database_name}'
    """)
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return tables


def get_table_schema(connection, database_name, table_name):
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{database_name}'
        AND table_name = '{table_name}'
        ORDER BY ordinal_position
    """)
    columns = cursor.fetchall()
    cursor.close()
    return columns