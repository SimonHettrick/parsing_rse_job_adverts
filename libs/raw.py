from datetime import datetime
import pathlib
import shutil
import sqlite3
import tarfile
import tempfile

from libs import parse_csv
import pandas as pd
import settings


def parse_from_raw(datastores, logfile, start_time):

    flndate = start_time.strftime("%Y-%m-%d")
    logdate = start_time.strftime('%d/%m/%Y %H.%M.%S')

    # Set up dict to store dfs of raw data
    dfs = {}

    # ===== Convert raw job htmls to csv =====
    for datastore in datastores:

        # Get filenames of all available jobs
        list_of_adverts = parse_csv.find_files(datastore)

        logfile.write('Analysed datastore: ' + datastore + '\n \n')
        logfile.write('Date and time: ' + str(logdate) + '\n \n')
        logfile.write(f'There were {len(list_of_adverts)} job adverts reviewed in the sample.\n \n')

        # Parse jobs html and read into df
        parsed_df = parse_csv.read_html(list_of_adverts)

        if parsed_df.empty:
            continue

        dfs[datastore] = parsed_df

        # Logging
        logfile.write(f'There were {len(dfs[datastore])} job adverts were parsed into the data file\n')

        n_invalid = sum((dfs[datastore]['placed_on'] == '') & (dfs[datastore]['job_title'] == ''))

        logfile.write(f' - {n_invalid} were missing date and/or title data\n\n')

        parse_csv.export_to_csv(
            dfs[datastore],
            settings.RESULTSPATH,
            '1_processed_jobs_'+datastore.replace('/','_')+'_'+flndate,
            False,
        )

        print(f"--- Processed html files in {datastore} to csv ---")
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

    parse_csv.export_to_csv(df, settings.RESULTSPATH, '1_merged_jobs_'+flndate, False)

    print(f'Merged dataset with {len(df)} jobs saved to "2_merged_jobs_{flndate}.csv"')
    logfile.write(f'Merged file saved to 2_merged_jobs_{flndate}.csv\n\n')
    logfile.write(f'Processing took {datetime.now() - start_time}')


    # ===== Add new files to tar =====

    # Get the valid years
    df['year'] = pd.DatetimeIndex(df['placed_on']).year
    valid_years = df['year'].unique()

    for year in valid_years:
        y_df = df.loc[df['year'] == year]

        with tempfile.TemporaryDirectory() as td:

            tdir = str(pathlib.Path(td))+'/'

            # Extract current contents of tarfile (if exists)
            try:
                with tarfile.open(settings.RESULTSPATH+'jobs_'+str(year)+'.tar.gz', 'r|gz') as tar:
                    tar.extractall(tdir)

            except FileNotFoundError:
                print('No existing tar found')

            # Copy all new files to the temp directory
            for _, job in y_df.iterrows():
                shutil.copyfile(str(job['source'])+job['filename'], tdir+job['filename'])

            # Add the temp directory in its entirety to the new replacement tar
            with tarfile.open(settings.RESULTSPATH+'jobs_'+str(year)+'.tar.gz', 'w|gz') as tar:
                tar.add(tdir, recursive=True, arcname='')

    # ===== Add new files to database =====

    conn = sqlite3.connect(settings.DB_LOCATION)

    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY,
            filename TEXT NOT NULL, 
            job_title TEXT,
            contract_type TEXT, 
            placed_on DATE,
            closes_on DATE,
            salary_min FLOAT,
            salary_max FLOAT,
            role TEXT,
            hours TEXT,
            job_ref TEXT,
            organisation TEXT,
            location TEXT,
            country TEXT,
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

    db_df = df.loc[~df['filename'].isin(prev_records)]
    db_df = db_df.replace('', None)
    # Reformat the raw df to be compatible with the db

    db_df.drop(['year'], axis=1, inplace=True)
    db_df.to_sql('jobs', conn, if_exists='append', index=False)
    conn.commit()

    with open(settings.DB_LOCATION+'.stats', 'w') as fstats:

        fstats.write('Field name,Valid Values,Invalid Values\n')
        for column in db_df.columns:
            cursor.execute(f'SELECT * FROM jobs WHERE {column} IS NOT NULL')
            isntnull=str(len(cursor.fetchall()))
            cursor.execute(f'SELECT * FROM jobs WHERE {column} IS NULL')
            isnull=str(len(cursor.fetchall()))
            fstats.write(', '.join([column, isntnull, isnull])+'\n')

    return df
