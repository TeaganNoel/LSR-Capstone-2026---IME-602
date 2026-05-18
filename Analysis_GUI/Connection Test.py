"""
Simple MySQL Remote Access Test
Queries testID = 999999999 and prints the result.
Database is accesible if no errors occur and kernal does not time out
"""

import mysql.connector
import pandas as pd

server_connect = mysql.connector.connect(
    user='lsr_remote',
    password='LSR2026!',
    host='100.108.209.100',   # THE BEAST's IP
    port=3306,
    database='lsr_testing_database'
)

print("Connected to Mason's database remotely.\n")

TEST_ID = 1

query = """
    SELECT *
    FROM test
    WHERE testID = %s
"""

df = pd.read_sql(query, server_connect, params=[TEST_ID])

if df.empty:
    print(f"No data found for testID {TEST_ID}.")
else:
    print(f"Data for testID {TEST_ID}:\n")
    print(df.to_string(index=False))

print("\nDone.")
