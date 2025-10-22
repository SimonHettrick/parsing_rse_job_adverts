#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
A full stack pipeline to scrape job postings from the web, package them locally in tarfiles,
parse their information, store the resultant information in a database, and perform some
basic analysis on the results.
"""

import sys
import sqlite3
import time
from datetime import datetime

from libs import find_jobs, raw, scrape_jobs
import pandas as pd
import settings

# Default values for datastore and resultspath when not specified at command line

datastores = settings.DEFAULT_DATASTORES
RESULTSPATH = settings.RESULTSPATH

# DATASTORES can be overridden by passing a list of directories as arguments when
# running this script on the command line, e.g.
#  > python jobs_to_csv.py ./job_ads_simon ./job_ads_edinburgh

in_args=sys.argv
if '--test' in in_args:
    datastores = settings.TEST_DATASTORES

# ---------------------------------------------------

def main(run_time, logfile):
    """
    Main function to run program

    :params: a datetime object to use as the 'start time' throughout the pipeline, and
            an opened file in write mode to use as a log file
    """

    # ===== Prep =====

    # Logging
    flndate = run_time.strftime("%Y-%m-%d")

    if '--scrape' in in_args:
        logfile.write('Scraping new jobs:')
        scrape_jobs.scrape()

    if not '--from-db' in in_args:
        raw.parse_from_raw(
            datastores=datastores,
            logfile=logfile,
            start_time=run_time,
        )

    with sqlite3.connect(settings.DB_LOCATION) as conn:

        df = pd.read_sql_query("SELECT * FROM jobs", conn)


    # ===== Annotate database =====

    df['year'] = pd.DatetimeIndex(df['placed_on']).year                 # pylint: disable=no-member
    df['salary_min'] = pd.to_numeric(df['salary_min'])
    df['salary_max'] = pd.to_numeric(df['salary_max'])

    # ===== Find jobs =====

    start_time = time.time()

    # Logging
    logfile.write('Analysing merged jobs list')
    # Get parsed job advert data
    print('Extracting date information...')
    df['placed_on']= pd.to_datetime(df['placed_on'],format='mixed', errors='coerce')
    df['closes_on']= pd.to_datetime(df['closes_on'],format='mixed', errors='coerce')

    # Logging
    logfile.write('There were ' + str(len(df)) + ' parsed job adverts' + '\n \n')

    df = find_jobs.clean_job_titles(df)
    # Logging
    logfile.write('There are ' + str(len(df)) + ' jobs with job titles' + '\n \n')

    # Enrich data by searching job titles finding roles of interest
    df = find_jobs.find_jobs(df)
    # Get dates working and sort by date
    #df = date_and_sort(df)
    logfile.write('There are ' + str(len(df)) + ' jobs with a full complement of data' + '\n \n')

    # Get number of jobs per year
    jobs_per_year_dict = find_jobs.jobs_per_year(df)
    # Export just the data of interest
    df_interest=find_jobs.enhance(df)

    # Calculate a summary of the data
    df_summ = find_jobs.summary_of_job_num(df_interest, jobs_per_year_dict)

    # Make plots and gather stats based on the data summary
    find_jobs.plot_job_summary(df, df_interest, df_summ,settings.RESULTSPATH, flndate)
    find_jobs.get_and_plot_salaries(df_interest, settings.RESULTSPATH, flndate, df2=df)

    print(f"--- {round((time.time() - start_time),1)} seconds ---")
    logfile.write('Processing took ' + str(round((time.time() - start_time),1)) + '\n')

    logfile.close()


if __name__ == '__main__':

    now = datetime.now()

    fdate = now.strftime("%Y-%m-%d")
    with open(settings.RESULTSPATH + 'pipeline_log_'+fdate+'.txt', 'w') as lfile:
        main(run_time=now, logfile=lfile)
