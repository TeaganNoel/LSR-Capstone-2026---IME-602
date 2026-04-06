import mysql.connector

def get_connection():
    return mysql.connector.connect(
        user='root',
        password='Bikegofast!777',
        host='localhost',
        database='lsr_testing_database'
    )