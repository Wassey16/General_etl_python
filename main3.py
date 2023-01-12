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

# Fetch the column values
cur.execute("SELECT id, pos FROM sample_pos")
column_values = cur.fetchall()

# Convert the column values to a DataFrame
df = pd.DataFrame(column_values, columns=["id_pos", "pos"])

# Close the cursor and connection
cur.close()
conn.close()


conn = psycopg2.connect(
    host="localhost",
    user="root",
    password="password",
    database="mydb"
)

# Create a cursor object
cur = conn.cursor()

# Fetch the column values
cur.execute("SELECT * FROM word")
column_values = cur.fetchall()

# Convert the column values to a DataFrame
df2 = pd.DataFrame(column_values, columns=["id_word", "word", 'pos'])

# Close the cursor and connection
cur.close()
conn.close()
merged_df = df.merge(df2, on='pos')
merged_df = merged_df[['id_word','id_pos']]
merged_df.to_csv('new2.csv', index=False)

conn = psycopg2.connect(
    host="localhost",
    database="mydb",
    user="root",
    password="password"
)
cur = conn.cursor()

# Create table
cur.execute('''CREATE TABLE word_pos (
  id_word INTEGER REFERENCES word(id),
  id_pos INTEGER REFERENCES sample_pos(id)
);''')

# Write the DataFrame to the table
for _, row in merged_df.iterrows():
    cur.execute("INSERT INTO word_pos(id_word, id_pos) VALUES (%s, %s)", (int(row['id_word']), int(row['id_pos'])))

# Save the changes
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()
