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

    # Clean rows that have missing title data
    df.dropna(subset=['job title'], inplace=True)

    return df


def date_and_sort(df):
    """
    Drops all the rows that lack date data, then converts the date col to datetime and sorts the data by date
    :param df: all the parsed job advert data
    :return: the same df, but with the date data cleaned and sorted
    """

    df = df[df['date']!='no_data']
    df['date'] = pd.to_datetime(df['date'])
    df.sort_values(by=['date'], inplace=True, ascending=True)

    return df


def jobs_per_year(df):
    """
    Finds number of job adverts per year so we can work out percentages later
    :param df: all the parsed job advert data
    :return: a dict of year and number of adverts available
    """

    jobs_per_year_dict = df.value_counts(subset='year').to_dict()

    return jobs_per_year_dict


def find_jobs(df, jobs_of_interest):
    """
    Searches the job titles to find titles of interest.
    :param df: the parsed info from the job adverts
    :return: a df with additional cols identifying rows of interest
    """

    for current_job in jobs_of_interest:
        df[current_job] = np.where(df['job title'].str.contains(current_job), True, False)

    # These find the rows where the job title matches the search term and creates a new column marked as True

    #df['research software engineer'] = np.where(df['job title'].str.contains('research software engineer'), True, False)
    #df['software developer'] = np.where(df['job title'].str.contains('software developer'), True, False)
    #df['software engineer'] = np.where(df['job title'].str.contains('software engineer'), True, False)
    #df['research engineer'] = np.where(df['job title'].str.contains('research engineer'), True, False)

    return df


def enhance(df, jobs_of_interest, avoid_jobs):

    # Create a column which identifies rows which include any of the jobs of interest
    for current_job in jobs_of_interest:
        mask = df[current_job]==True
        df.loc[mask, 'any_job'] = True

    # THIS IS ONLY NEEDED FOR RSE and SE job columns. Obviously, all SE will be listed in the RSE column too, so
    # we need to remove this duplication
    mask = df['research software engineer']==True
    df.loc[mask, 'software engineer'] = False

    # Flag jobs that are not of interest and remove them
    for not_job in avoid_jobs:
        df['not_job'] = np.where(df['job title'].str.contains(not_job), True, False)
        # The any_job col AND "NOT of not_job" will result in True only for those jobs that include
        # terms from the jobs_of_interest list and do not include terms from the avoid_jobs list
        df['keep_job'] = df['any_job'] & ~df['not_job']
        # Limit the df to only those jobs of interest
        df = df[df['keep_job']==True]

    return df


def summary_of_job_num(df_interest, jobs_per_year_dict):

    found_jobs_per_year_dict = df_interest.value_counts(subset='year').to_dict()

    year_list = []
    num_all_list = []
    num_rse_list = []
    percent_list = []

    for key in found_jobs_per_year_dict:
        year_list.append(key)
        num_rse_list.append(found_jobs_per_year_dict[key])
        num_all_list.append(jobs_per_year_dict[key])
        percent_list.append(round((found_jobs_per_year_dict[key]/jobs_per_year_dict[key])*100,3))

    df_summ = pd.DataFrame()
    df_summ['year'] = year_list
    df_summ['number all jobs'] = num_all_list
    df_summ['number rse jobs'] = num_rse_list
    df_summ['percentage rse jobs'] = percent_list

    df_summ.sort_values(by=['year'], inplace=True, ascending=True)

    return df_summ


def main():
    """
    Main function to run program
    """

    # Add a list of job titles that you want to search for here
    jobs_of_interest = ['research software engineer', 'software developer', 'software engineer', 'research engineer', 'bioinformatician']

    # Add a list of job titles that you're not interested in here
    avoid_jobs = ['lecturer', 'fellow', 'student']

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
    df = find_jobs(df, jobs_of_interest)

    # Get dates working and sort by date
    #df = date_and_sort(df)
    file.write('There are ' + str(len(df)) + ' jobs with a full complement of data' + '\n \n')

    # Get number of jobs per year
    jobs_per_year_dict = jobs_per_year(df)

    # Export just the data of interest
    df_interest=enhance(df, jobs_of_interest, avoid_jobs)

    # Calculate a summary of the data
    df_summ = summary_of_job_num(df_interest, jobs_per_year_dict)

    # Export data
    export_to_csv(df, RESULTSPATH, '2_named_processed_jobs', False)

    # Logging
    file.write('There are ' + str(len(df_interest)) + ' jobs with the job title of interest' + '\n \n')
    # Export enhanced data
    export_to_csv(df_interest, RESULTSPATH, '3_identified_jobs', False)

    # Export data
    export_to_csv(df_summ, RESULTSPATH, '4_summary_identified_jobs', False)

    print("--- %s seconds ---" % round((time.time() - start_time),1))
    file.write('Processing took ' + str(round((time.time() - start_time),1)) + '\n')

    file.close()

if __name__ == '__main__':
    main()
