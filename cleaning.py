#!/usr/bin/env python
# encoding: utf-8

import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
import glob
import os
import re
import time

DATA = 'processed_jobs_test.csv'

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

def drop_bad_rows(df):

    # Drop all missing job titles
    df = df[df['job title']!='none']

    # Drop all missing dates
    df = df[df['date'] != '0']

    # Drop all dates whose format doesn't allow easy extraction of the year
    df = df[df['year'].astype(str).str[0] == '2']

    return df

def analyse_results(df):

    jobs_list = ['rse',	'software developer', 'software engineer']

    years_list = df['year'].unique()

    all_results = []

    for current_year in years_list:
        first_temp_df = df[df['year'] == current_year]
        results = [current_year]
        for current_job in jobs_list:
            second_temp_df = first_temp_df[first_temp_df[current_job] == True]
            #print(second_temp_df)
            results.append(len(second_temp_df))
        all_results.append(results)

    # Use the following to name the coming df cols
    jobs_list.insert(0,'year')

    results_df = pd.DataFrame.from_records(all_results)
    results_df.columns = jobs_list
    results_df.sort_values(by=['year'], inplace=True)


    print(results_df)
    return


def main():
    """
    Main function to run program
    """

    start_time = time.time()

    df = import_csv_to_df('./', DATA)

    print(len(df))
    df = drop_bad_rows(df)
    print(len(df))

    export_to_csv(df, './', 'cleaned parsed jobs', False)

    analyse_results(df)

    print("--- %s seconds ---" % (time.time() - start_time))

if __name__ == '__main__':
    main()