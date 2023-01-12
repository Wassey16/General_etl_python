import pandas as pd
import psycopg2
column_names = ['word', 'PoS', 'l1', 'l2', 'l3', 'l4', 'l5', 'l6']
data = pd.read_csv('alaa.vert', sep='\t', names=column_names, skiprows=4)
df=data[['word','PoS']]
df = df.dropna(subset=['PoS'])
x = df.drop_duplicates(subset='PoS', keep='first')
x.to_csv('data.csv', index=False)
y=pd.read_csv('data.csv')
y['category'] = ""
y['type'] = ""
y['person'] = ""
y['gen'] = ""
y['num'] = ""
y['neclass'] = ""
y['nesubclass'] = ""
y['degree'] = ""
y['possessorpers'] = ""
y['possessornum'] = ""
y['case'] = ""
y['polite'] = ""
y['mood'] = ""
y['tense'] = ""
y['non_positional'] = ""
print(y)

def parse_pos(row):
    pos = row['PoS']
    if pos[0] == 'A': # adjective
        row['category'] = pos[0]
        row['type'] = pos[1]
        row['degree'] = pos[2]
        row['gen'] = pos[3]
        row['num'] = pos[4]
        row['possessorpers'] = pos[5]
        row['possessornum'] = pos[6]
    elif pos[0] == 'C': # conjunction
        row['category'] = pos[0]
        row['type'] = pos[1]
    elif pos[0] == 'D': # determiner
        row['category'] = pos[0]
        row['type'] = pos[1]
        row['person'] = pos[2]
        row['gen'] = pos[3]
        row['num'] = pos[4]
        row['possessornum'] = pos[5]
    elif pos[0] == 'N': # noun
        row['category'] = pos[0]
        row['type'] = pos[1]
        row['gen'] = pos[2]
        row['num'] = pos[3]
        row['neclass'] = pos[4]
        row['nesubclass'] = pos[5]
        row['degree'] = pos[6]
    elif pos[0] == 'P': # pronoun
        row['category'] = pos[0]
        row['type'] = pos[1]
        row['person'] = pos[2]
        row['gen'] = pos[3]
        row['num'] = pos[4]
        row['case'] = pos[5]
        row['polite'] = pos[6]
    elif pos[0] == 'R': # adverb
        row['category'] = pos[0]
        row['type'] = pos[1]
    elif pos[0] == 'S': # adposition
        row['category'] = pos[0]
        row['type'] = pos[1]
    elif pos[0] == 'V': # verb
        row['category'] = pos[0]
        row['type'] = pos[1]
        row['mood'] = pos[2]
        row['tense'] = pos[3]
        row['person'] = pos[4]
        row['num'] = pos[5]
        row['gen'] = pos[6]
    elif pos[0] == 'Z': # adposition
        row['category'] = pos
    elif pos[0] == 'W': # adposition
        row['category'] = pos[0]
    elif pos[0] == 'I': # adposition
        row['category'] = pos[0]
        
    else:
        row['non_positional'] = pos

# Apply the function to each row
y.apply(parse_pos, axis=1)
print(y)
y.to_csv('new.csv')


conn = psycopg2.connect(
    host="localhost",
    database="mydb",
    user="root",
    password="password"
)
cursor = conn.cursor()

# Create table
cursor.execute("""CREATE TABLE pos
(
	id				SERIAL	NOT	NULL PRIMARY KEY,
​
	category		CHAR	NOT	NULL,
	type			CHAR,
	degree			CHAR,
	gen				CHAR,
	num				CHAR,
	possessorpers	CHAR,
	possessornum	CHAR,
	person			CHAR,
	neclass			CHAR,
	nesubclass		CHAR,
	'case'			CHAR,
	polite			CHAR,
	mood			CHAR,
	tense			CHAR,
	punctenclose	CHAR
);
""")

conn.commit()
cursor.close()
cursor = conn.cursor()

# Insert rows into the table
for i, row in y.iterrows():
    cursor.execute("""
        INSERT INTO sample_pos (word, PoS, category, type, person, gen, num, neclass, nesubclass, degree, possessorpers, possessornum, 'case', polite, mood, tense, non_positional)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (row['word'], row['PoS'], row['category'], row['type'], row['person'], row['gen'], row['num'], row['neclass'], row['nesubclass'], row['degree'], row['possessorpers'], row['possessornum'], row['case'], row['polite'], row['mood'], row['tense'], row['non_positional']))
conn.commit()
cursor.close()
conn.close()



z = df.drop_duplicates(subset='word', keep='first')

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
for _, row in z.iterrows():
    cur.execute("INSERT INTO word(word, pos) VALUES (%s, %s)", (row['word'], row['PoS']))

# Save the changes
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()

# Create a new DataFrame with the word, POS code, and a unique ID
y['id'] = y.index + 1  # adding ID
pos_codes = y.drop_duplicates(subset='PoS', keep='first')
pos_codes['pos_id'] = pos_codes.index + 1
pos_codes = pos_codes[['PoS','pos_id']]

df_rel = y.merge(pos_codes, on='PoS')
df_rel = df_rel[['id','pos_id']]

# Write the new DataFrame to the new table in the database
print(df_rel)
# Save the changes
