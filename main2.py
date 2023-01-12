import pandas as pd
import psycopg2

column_names = ['word', 'PoS', 'l1', 'l2', 'l3', 'l4', 'l5', 'l6']
data = pd.read_csv('alaa.vert', sep='\t', names=column_names, skiprows=4)
df=data[['word','PoS']]
df = df.dropna(subset=['PoS'])
x = df.drop_duplicates(subset='word', keep='first')

conn = psycopg2.connect(
    host="localhost",
    database="mydb",
    user="root",
    password="password"
)
cur = conn.cursor()

# Create table
cur.execute("CREATE TABLE word(id serial primary key, word varchar, pos varchar);")

# Write the DataFrame to the table
for _, row in x.iterrows():
    cur.execute("INSERT INTO word(word, pos) VALUES (%s, %s)", (row['word'], row['PoS']))

# Save the changes
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()
