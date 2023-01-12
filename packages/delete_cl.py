import psycopg2
import pandas as pd

conn = psycopg2.connect(
    host="localhost",
    user="root",
    password="password",
    database="mydb"
)
# Create a cursor object
cur = conn.cursor()
cur.execute("ALTER TABLE sample_pos DROP COLUMN word;")
cur.execute("ALTER TABLE word DROP COLUMN pos;")
conn.commit()
cur.close()
conn.close()

