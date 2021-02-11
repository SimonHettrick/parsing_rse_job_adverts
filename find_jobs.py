#!/usr/bin/env python
# encoding: utf-8

import pandas as pd
import numpy as np
import re
import time
from datetime import datetime


RESULTSPATH = './results/'
RESULTSFILENAME = './1_processed_jobs.csv'


def import_csv_to_df(location, filename):
    """
    Imports a csv file into a Pandas dataframe
    :params: an csv file and a filename from that file
    :return: a df
    """

    return pd.read_csv(location + filename)


def export_to_csv(df, location, filename, index_write):
    """
    Exports a df to a csv file
    :params: a df and a location in which to save it
    :return: nothing, saves a csv
    """

    return df.to_csv(location + filename + '.csv', index=index_write)


def clean_job_titles(df):

    # Clean rows that have missing data
    print(len(df))
    df.dropna(subset=['job title'], inplace=True)

    # Clean rows where the parser could not find the data
    print(len(df))

    df = df[(df!=0).any(axis=1)]

    print(len(df))
    return df


def date_and_sort(df):

    df = df[df['date']!='no_data']
    df['date'] = pd.to_datetime(df['date'])
    df.sort_values(by=['date'], inplace=True, ascending=True)

    return df


def find_jobs(df):
    """
    Searches the job titles to find titles of interest. Also conducts some logic so that "Research Software Engineer"
    and "Software Engineer" aren't both flagged in the same job advert (RSE takes precendence here).
    :param df: the parsed info from the job adverts
    :return: a df with additional cols identifying rows of interest
    """

    # These find
    df['research software engineer'] = np.where(df['job title'].str.contains('research software engineer'), True, False)


    #df['impact manager'] = np.where(df['job title'].str.contains('impact manager'), True, False)
    #df['software developer'] = np.where(df['job title'].str.contains('software developer'), True, False)
    #df['software engineer'] = np.where(df['job title'].str.contains('software engineer'), True, False)
    #df['refined software engineer'] = np.where((df['software engineer'] == True) & (df[job_title] == False), True, False)

    return df

def enhance(df):

    df_interest = df[df['research software engineer']==True]

    return df_interest


def main():
    """
    Main function to run program
    """

    start_time = time.time()

    # Logging
    file = open(RESULTSPATH + 'find_jobs_log.txt', 'w')
    logdate = datetime.now().strftime('%d/%m/%Y %H.%M.%S')
    file.write('Date and time: ' + str(logdate) + '\n \n')


    # Get parsed job advert data
    df = import_csv_to_df(RESULTSPATH, RESULTSFILENAME)

    # Logging
    file.write('There were ' + str(len(df)) + ' parsed job adverts' + '\n \n')

    df = clean_job_titles(df)

    # Logging
    file.write('There are ' + str(len(df)) + ' jobs with job titles' + '\n \n')

    # Enrich data by searching job titles finding roles of interest
    df = find_jobs(df)

    # Get dates working and sort by date
    df = date_and_sort(df)

    # Export data
    export_to_csv(df, RESULTSPATH, '2_named_processed_jobs', False)

    # Export just the data of interest
    df_interest=enhance(df)
    # Logging
    file.write('There are ' + str(len(df_interest)) + ' jobs with the job title of interest' + '\n \n')
    # Export enhanced data
    export_to_csv(df_interest, RESULTSPATH, '3_identified_jobs', False)

    print("--- %s seconds ---" % round((time.time() - start_time)'s',1))
    file.write('Processing took ' + str(round((time.time() - start_time) + 's',1)) + '\n')

    file.close()

if __name__ == '__main__':
    main()
