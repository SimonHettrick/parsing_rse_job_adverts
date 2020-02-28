#!/usr/bin/env python
# encoding: utf-8

import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
import glob
import os
import re
import time
from datetime import datetime

RESULTSPATH = './results/'
TITLE_TO_SEARCH = 'impact officer'
DATANAME = 'processed_jobs.csv'


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

def check_for_results_dir(resultsdir, jobtitle):
    """
    Check if a dir for the results already exists and, if not, creates one
    :param resultsdir: the dir in which the results should exist
    :param jobtitle: the job title which will be used to create a subdir
    :return: the resulting dirname that comes from joining results dir and jobtitle
    """
    dirname = resultsdir + jobtitle

    if not os.path.exists(dirname):
        os.makedirs(dirname)

    return dirname


def drop_bad_rows(df):

    # Drop all missing job titles
    df = df[df['job title']!='none']

    # Drop all missing dates
    df = df[df['date'] != '0']

    # Drop all dates whose format doesn't allow easy extraction of the year
    df = df[df['year'].astype(str).str[0] == '2']

    # Drop all job categories
    df = df[df['date'] != '0']

    df = df[(df['role'] == '0') | (df['role'] == 'Academic or Research') | (df['role'] == 'Professional or Managerial') | (df['role'] == 'Technical')]

    return df

def analyse_results(df):

    #jobs_list = ['rse',	'software developer', 'software engineer']
    jobs_list = [TITLE_TO_SEARCH]

    years_list = df['year'].unique()

    all_results = []

    for current_year in years_list:
        first_temp_df = df[df['year'] == current_year]
        results = [current_year]
        for current_job in jobs_list:
            print(current_job)
            second_temp_df = first_temp_df[first_temp_df[current_job] == True]
            #print(second_temp_df)
            results.append(len(second_temp_df))
        all_results.append(results)

    # Use the following to name the coming df cols
    jobs_list.insert(0,'year')

    results_df = pd.DataFrame.from_records(all_results)
    results_df.columns = jobs_list
    results_df.sort_values(by=['year'], inplace=True)

    return results_df


def main():
    """
    Main function to run program
    """

    start_time = time.time()

    # Check for dir for results and if it doesn't exist, create it
    resultsdir = check_for_results_dir(RESULTSPATH, TITLE_TO_SEARCH)
    # Add a forward slash so I can use dirname as a path
    resultsdir = resultsdir + '/'

    print(resultsdir + DATANAME)

    df = import_csv_to_df(resultsdir, DATANAME)

    # Logging
    file = open(resultsdir + TITLE_TO_SEARCH + '_cleaning_log.txt', 'w')
    logdate = datetime.now().strftime('%d/%m/%Y %H.%M.%S')
    file.write('Date and time: ' + str(logdate) + '\n \n')


    before_length = len(df)
    df = drop_bad_rows(df)
    after_length = len(df)

    file.write('There were: ' + str(before_length) + 'adverts before cleaning\n')
    file.write('And ' + str(after_length) + 'adverts after cleaning\n')
    file.write('This means: ' + str(before_length - after_length) + 'adverts were missing information (such as the title'
                                                                    'or date that would prevent them from being processed \n')

    print('Before cleaning: ' + str(before_length))
    print('After cleaning: ' + str(after_length))

    export_to_csv(df, './', 'cleaned parsed jobs', False)

    results_df = analyse_results(df)

    print(results_df)

    export_to_csv(results_df, './', 'summary of analysis', False)

    print("--- %s seconds ---" % (time.time() - start_time))

if __name__ == '__main__':
    main()