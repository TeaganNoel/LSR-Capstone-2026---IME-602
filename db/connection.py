############################################################################
#Program Filename: connection.py
#Author: Mason Allen
#Created Date: 4/28/2026
#Last Updated Date: 4/28/2026
#Description: Use mysql.connector to establish an admin connection with db
############################################################################

import mysql.connector

def get_connection():
############################################################################
#Function Name: get_connection
#Description: Use mysql.connector to establish an admin connection with db
#Parameters: none
#Return Values: connection --> MySQL db connection for queries and imports
############################################################################

    return mysql.connector.connect(
        user='root',
        password='Bikegofast!777',
        host='localhost',
        database='lsr_testing_database'
    )