############################################################################
#Program Filename: connection.py
#Author: Mason Allen
#Created Date: 4/28/2026
#Last Updated Date: 4/28/2026
#Description: Use mysql.connector cursor to query db schema information
############################################################################

def get_table_names(connection, database_name):
############################################################################
#Function Name: get_table_names
#Description: this function selects and returns all tables in the database 
#             schema
#Parameters: connection --> MySQL db connection for queries and imports
#            database_name --> "lsr_testing_database"
#Return Values: tables --> array containing all database tables
############################################################################

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
############################################################################
#Function Name: get_table_schema
#Description: this function selects and returns all column names and their 
#             datatypes in a specific table in the database 
#Parameters: connection --> MySQL db connection for queries and imports
#            database_name --> "lsr_testing_database"
#            table_name --> table to analyze
#Return Values: columns --> array containing all columns and thier respective
#                           datatypes from the table
############################################################################

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