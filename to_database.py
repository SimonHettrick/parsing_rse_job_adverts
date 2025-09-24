import csv
import pandas as pd
import sqlite3
import sys

args=sys.argv[1:]

# Ensure user has passed two arguments to use as filenames

if len(args) != 2:
    raise ValueError('Must pass 2 values to script (the named_processed_jobs csv and the location of the output db)')

conn = sqlite3.connect(args[1])

create_table="""CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY,
    filename TEXT NOT NULL, 
    job_title TEXT NOT NULL, 
    start_date DATE, 
    salary FLOAT,
    role TEXT,
    organisation TEXT,
    location TEXT,
    contains_data_scien BOOLEAN,
    contains_data_engineer BOOLEAN,
    contains_software_develop BOOLEAN,
    contains_software_engineer BOOLEAN,
    contains_research_engineer BOOLEAN,
    contains_bioinformatic BOOLEAN
);"""

cursor = conn.cursor()
cursor.execute(create_table)
conn.commit()

with open(args[0],'r') as f_in:
    df = pd.read_csv(f_in)
    df = df.rename(columns={
        'job title':'job_title',
        'date':'start_date',
        'data scien':'contains_data_scien',
        'data engineer':'contains_data_engineer',
        'software develop':'contains_software_develop',
        'software engineer':'contains_software_engineer',
        'research engineer':'contains_research_engineer',
        'bioinformatic':'contains_bioinformatic',
    })
    df = df.drop('year', axis=1)
    df.to_sql('jobs', conn, if_exists='replace', index=False)
    conn.commit()