#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Library of functions to deal with extracting the contents of html files and storing them in sqlite
and tarfiles.
"""

from datetime import datetime
import pathlib
import shutil
import sqlite3
import tarfile
import tempfile

from libs import parse_jobs
import numpy as np
import pandas as pd
import settings

def tar_job_ads(df):
    valid_years = df['year'].unique()
    valid_years = valid_years[~np.isnan(valid_years)]

    for year in valid_years:
        y_df = df.loc[df['year'] == year]

        with tempfile.TemporaryDirectory() as td:

            tdir = str(pathlib.Path(td))+'/'

            # Extract current contents of tarfile (if exists)
            try:
                with tarfile.open(settings.TARPATH+'jobs_'+str(year)+'.tar.gz', 'r|gz') as tar:
                    tar.extractall(tdir)

            except FileNotFoundError:
                print('No existing tar found')

            # Copy all new files to the temp directory
            for _, job in y_df.iterrows():
                shutil.copyfile(str(job['source'])+job['filename'], tdir+job['filename'])

            # Add the temp directory in its entirety to the new replacement tar
            with tarfile.open(settings.TARPATH+'jobs_'+str(year)+'.tar.gz', 'w|gz') as tar:
                tar.add(tdir, recursive=True, arcname='')


    with tempfile.TemporaryDirectory() as td:

        y_df = df[df['year'].isna()]

        tdir = str(pathlib.Path(td))+'/'

        # Extract current contents of tarfile (if exists)
        try:
            with tarfile.open(settings.TARPATH+'jobs_nullyear.tar.gz', 'r|gz') as tar:
                tar.extractall(tdir)

        except FileNotFoundError:
            print('No existing tar found')

        # Copy all new files to the temp directory
        for _, job in y_df.iterrows():
            shutil.copyfile(str(job['source'])+job['filename'], tdir+job['filename'])

        # Add the temp directory in its entirety to the new replacement tar
        with tarfile.open(settings.TARPATH+'jobs_nullyear.tar.gz', 'w|gz') as tar:
            tar.add(tdir, recursive=True, arcname='')


def parse_from_raw(datastores, logfile, start_time, no_tar=False):
    """
    Extracts all job listing html files from a number of given directories, sorts them by year and
    appends them to (or creates) tarfiles to reduce storage space, parses the html for each job to
    extract a number of useful data parameters and appends these to (or creates) a database of these
    values.  Ignores duplicates when appending to tarfiles or the db.  NOTE: does NOT delete the
    original html files.

    :params:
    - a list of strings, each of which is the full path to a directory containing a set of job
        listing html files.  If multiple paths are provided and contain duplicate jobs (by
        filename), the version of the html file at the path earlier in the list will be prioritised
        over the file at the path later in the list.
    - an open file in write mode to use for logging
    - a datetime object to use as the start of this script for logging purposes

    :return: nothing, creates or appends to a sqlite3 instance and tarfiles for each year present in
        the data
    """

    #flndate = start_time.strftime("%Y-%m-%d")
    logdate = start_time.strftime('%d/%m/%Y %H.%M.%S')

    # Set up dict to store dfs of raw data
    dfs = {}

    # ===== Convert raw job htmls to csv =====
    for datastore in datastores:

        # Get filenames of all available jobs
        list_of_adverts = parse_jobs.find_files(datastore)

        logfile.write('Analysed datastore: ' + datastore + '\n \n')
        logfile.write('Date and time: ' + str(logdate) + '\n \n')
        logfile.write(f'There were {len(list_of_adverts)} job adverts reviewed in the sample.\n \n')

        # Parse jobs html and read into df
        parsed_df = parse_jobs.read_html(list_of_adverts)

        if parsed_df.empty:
            continue

        dfs[datastore] = parsed_df

        # Logging
        logfile.write(
            f'There were {len(dfs[datastore])} job adverts were parsed into the data file\n'
        )

        n_invalid = sum((dfs[datastore]['placed_on'] == '') & (dfs[datastore]['job_title'] == ''))

        logfile.write(f' - {n_invalid} were missing date and/or title data\n\n')

        print(f"--- {datetime.now() - start_time} seconds ---")
        logfile.write('Processing took ' + str(datetime.now() - start_time) + 's\n')


    # ===== Merge resultant datasets =====

    df = dfs[datastores[0]]
    df['source'] = datastores[0]

    for datastore in datastores[1:]:
        if not datastore in dfs:
            continue
        new_df = dfs[datastore]
        ids_present_in_base = df['filename'].unique()
        new_records = new_df[~new_df['filename'].isin(ids_present_in_base)]
        new_records['source']=datastore
        df = pd.concat((df, new_records))

    logfile.write(f'Merged jobs list has a length of {len(df)}\n')

    logfile.write(f'Processing took {datetime.now() - start_time}')


    # ===== Add new files to tar =====

    # Get the valid years
    df['year'] = pd.DatetimeIndex(df['placed_on']).year        # pylint: disable=no-member

    if not no_tar:
        tar_job_ads(df)

    # ===== Add new files to database =====

    conn = sqlite3.connect(settings.DB_LOCATION)

    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY,
            filename TEXT NOT NULL, 
            job_title TEXT,
            job_title_parsed TEXT,
            description TEXT,
            description_parsed TEXT,
            description_word_count INTEGER,
            contract_type TEXT, 
            placed_on DATE,
            closes_on DATE,
            salary_min FLOAT,
            salary_max FLOAT,
            role TEXT,
            hours TEXT,
            job_ref TEXT,
            organisation TEXT,
            location_string TEXT,
            city TEXT,
            region TEXT,
            country TEXT,
            latitude FLOAT,
            longitude FLOAT,
            source TEXT
        );
    """)
    conn.commit()

    # Fetch any results already in the db, remove from data to be added
    prev_files = pd.DataFrame(conn.execute("SELECT filename FROM jobs").fetchall())
    try:
        prev_records = prev_files[0].tolist()
    except KeyError:
        prev_records = []

    ####### DEBUG #######

    with open('loc_strings.txt','w') as f:
        u = df['location_string'].unique()
        for i in u:
            if i is not None:
                f.write(i+'\n')

    #######       #######

    db_df = df.loc[~df['filename'].isin(prev_records)]
    db_df = db_df.replace('', None)

    # Reformat db to be compatible with sql
    db_df.drop(['year'], axis=1, inplace=True)

    db_df.to_sql('jobs', conn, if_exists='append', index=False)
    conn.commit()

    with open(settings.DB_LOCATION+'.stats', 'w', encoding='utf-8') as fstats:

        fstats.write('Field name,Valid Values,Invalid Values\n')
        for column in db_df.columns:
            cursor.execute(f'SELECT * FROM jobs WHERE {column} IS NOT NULL')
            isntnull=str(len(cursor.fetchall()))
            cursor.execute(f'SELECT * FROM jobs WHERE {column} IS NULL')
            isnull=str(len(cursor.fetchall()))
            fstats.write(', '.join([column, isntnull, isnull])+'\n')

    return df
