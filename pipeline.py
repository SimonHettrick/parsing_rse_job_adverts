#!/usr/bin/env python
# encoding: utf-8

import os
import pathlib
import shutil
import sqlite3
import sys
import tarfile
import time
from datetime import datetime

from libs import find_jobs, parse_csv
import pandas as pd
import tempfile

# Default values for datastore and resultspath when not specified at command line

DATASTORES = ['./job_ads/']
RESULTSPATH = './results/'

# DATASTORES can be overridden by passing a list of directories as arguments when
# running this script on the command line, e.g. 
#  > python jobs_to_csv.py ./job_ads_simon ./job_ads_edinburgh

# Add a list of job titles that you want to search for here; stems such as 'scien' will catch 'science', 'scientist' etc
jobs_of_interest = ['data scien', 'data engineer', 'software develop', 'software engineer', 'research engineer', 'bioinformatic']

# Add a list of job titles that you're not interested in here
avoid_jobs = [ 'fellow', 'lecturer', 'student', 'tutor', 'profess']

in_args=sys.argv

if len(in_args)<3:

    pass

else:

    # Dont change these values, these are for the command-line override

    DATASTORES = in_args[1:]

# ---------------------------------------------------

def main():
    """
    Main function to run program
    """

    # ===== Prep =====

    # Logging
    now = datetime.now()

    flndate = now.strftime("%Y-%m-%d")
    logdate = now.strftime('%d/%m/%Y %H.%M.%S')
    logfile = open(RESULTSPATH + 'pipeline_log_'+flndate+'.txt', 'w')

    # Set up dict to store dfs of raw data
    dfs = {}

    # ===== Convert raw job htmls to csv =====
    for datastore in DATASTORES:

        start_time = time.time()

        # Get filenames of all available jobs
        list_of_adverts = parse_csv.find_files(datastore)

        logfile.write('Analysed datastore: ' + datastore + '\n \n')
        logfile.write('Date and time: ' + str(logdate) + '\n \n')
        logfile.write('There were ' + str(len(list_of_adverts)) + ' job adverts reviewed in the sample' + '\n \n')

        # Parse jobs html and read into df
        dfs[datastore] = parse_csv.read_html(list_of_adverts)

        # Logging
        logfile.write('There were ' + str(len(dfs[datastore])) + ' job adverts were parsed into the data file' + '\n')

        n_invalid = sum((dfs[datastore]['date'] == '') & (dfs[datastore]['job title'] == ''))

        logfile.write(' - ' +str(n_invalid) + ' were missing date and/or title data\n\n')

        parse_csv.export_to_csv(dfs[datastore], RESULTSPATH, '1_processed_jobs_'+datastore.replace('/','_')+'_'+flndate, False)

        print("--- Processed html files in %s to csv ---" % datastore)
        print("--- %s seconds ---" % round((time.time() - start_time),1))
        logfile.write('Processing took ' + str(round((time.time() - start_time),1)) + 's\n')


    # ===== Merge resultant datasets =====

    start_time = time.time()
    df = dfs[DATASTORES[0]]
    df['source'] = DATASTORES[0]

    for datastore in DATASTORES[1:]:
        new_df = dfs[datastore]
        ids_present_in_base = df['filename'].unique()
        new_records = new_df.loc[~new_df['filename'].isin(ids_present_in_base)]
        new_records['source']=datastore
        df = pd.concat((df, new_records))

    logfile.write('Merged jobs list has a length of %i\n' % len(df))

    parse_csv.export_to_csv(df, RESULTSPATH, '1_merged_jobs_'+flndate, False)

    print('Merged dataset with %i jobs saved to "%s"' % (len(df), '2_merged_jobs_'+flndate+'.csv') )
    logfile.write('Merged file saved to %s\n\n' % '2_merged_jobs_'+flndate+'.csv')
    logfile.write('Processing took %fs' % (time.time() - start_time) )


    # ===== Add new files to tar =====

    # Get the valid years
    df=df.loc[df.year!='']
    valid_years = df['year'].unique()
    
    for year in valid_years:
        y_df = df.loc[df['year'] == year]

        with tempfile.TemporaryDirectory() as td:

            tdir = str(pathlib.Path(td))+'/'

            # Extract current contents of tarfile (if exists)
            try:
                tar = tarfile.open(RESULTSPATH+'jobs_'+str(year)+'.tar.gz', 'r|gz')
                tar.extractall(tdir)
                tar.close()

            except FileNotFoundError:
                print('No existing tar found')
        
            # Copy all new files to the temp directory
            for _, job in y_df.iterrows():
                shutil.copyfile(job['source']+job['filename'], tdir+job['filename'])

            # Add the temp directory in its entirety to the new replacement tar
            tar = tarfile.open(RESULTSPATH+'jobs_'+str(year)+'.tar.gz', 'w|gz')    
            tar.add(tdir, recursive=True, arcname='')
            tar.close()

    # ===== Add new files to database =====

    conn = sqlite3.connect(RESULTSPATH+'jobs.sqlite3')

    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY,
            filename TEXT NOT NULL, 
            job_title TEXT NOT NULL, 
            start_date DATE,
            salary FLOAT,
            role TEXT,
            organisation TEXT,
            location TEXT,
            source TEXT,
            contains_data_scien BOOLEAN,
            contains_data_engineer BOOLEAN,
            contains_software_develop BOOLEAN,
            contains_software_engineer BOOLEAN,
            contains_research_engineer BOOLEAN,
            contains_bioinformatic BOOLEAN
        );
    """)
    conn.commit()

    # Fetch any results already in the db, remove from data to be added
    prev_files = pd.DataFrame(conn.execute("SELECT filename FROM jobs").fetchall())
    try:
        prev_records = prev_files[0].tolist()
    except KeyError:
        prev_records = []

    db_df = df.loc[~df['filename'].isin(prev_records)]

    # Reformat the raw df to be compatible with the df
    db_df = db_df.rename(columns={
        'job title':'job_title',
        'date':'start_date',
        'data scien':'contains_data_scien',
        'data engineer':'contains_data_engineer',
        'software develop':'contains_software_develop',
        'software engineer':'contains_software_engineer',
        'research engineer':'contains_research_engineer',
        'bioinformatic':'contains_bioinformatic',
    })
    db_df.drop(['year'], axis=1, inplace=True)
    db_df.to_sql('jobs', conn, if_exists='append', index=False)
    conn.commit()

    with open(RESULTSPATH+'jobs.sqlite3.stats', 'w') as fstats:

        fstats.write('Field name,Valid Values,Invalid Values\n')
        for column in db_df.columns:
            cursor.execute(f'SELECT * FROM jobs WHERE {column} IS NOT NULL')
            isntnull=str(len(cursor.fetchall()))
            cursor.execute(f'SELECT * FROM jobs WHERE {column} IS NULL')
            isnull=str(len(cursor.fetchall()))
            fstats.write(', '.join([column, isntnull, isnull])+'\n')

    # ===== Find jobs =====

    start_time = time.time()

    # Logging
    logfile.write('Analysing merged jobs list')
    # Get parsed job advert data
    print('Extracting date information...')
    df['date']= pd.to_datetime(df['date'],format='mixed')

    # Logging
    logfile.write('There were ' + str(len(df)) + ' parsed job adverts' + '\n \n')

    df = find_jobs.clean_job_titles(df)
    # Logging
    logfile.write('There are ' + str(len(df)) + ' jobs with job titles' + '\n \n')

    # Enrich data by searching job titles finding roles of interest
    df = find_jobs.find_jobs(df, jobs_of_interest)
    # Get dates working and sort by date
    #df = date_and_sort(df)
    logfile.write('There are ' + str(len(df)) + ' jobs with a full complement of data' + '\n \n')

    # Get number of jobs per year
    jobs_per_year_dict = find_jobs.jobs_per_year(df)
    # Export just the data of interest
    df_interest=find_jobs.enhance(df, jobs_of_interest, avoid_jobs)

    # Calculate a summary of the data
    df_summ = find_jobs.summary_of_job_num(df_interest, jobs_per_year_dict)

    # Make plots and gather stats based on the data summary
    find_jobs.plot_job_summary(df,df_interest,df_summ,RESULTSPATH,flndate)
    find_jobs.get_and_plot_salaries(df_interest,RESULTSPATH,flndate,df2=df)

    # Export data
    parse_csv.export_to_csv(df, RESULTSPATH, '2_named_processed_jobs_'+flndate, False)
    logfile.write('There are ' + str(len(df_interest)) + ' jobs with the job title of interest' + '\n \n')
    # Export enhanced data
    parse_csv.export_to_csv(df_interest, RESULTSPATH, '3_identified_jobs_'+flndate, False)

    # Export data
    parse_csv.export_to_csv(df_summ, RESULTSPATH, '4_summary_identified_jobs_'+flndate, False)

    print("--- %s seconds ---" % round((time.time() - start_time),1))
    logfile.write('Processing took ' + str(round((time.time() - start_time),1)) + '\n')

    logfile.close()


if __name__ == '__main__':
    main()
