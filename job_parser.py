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

DATASTORE = './JobsAcUk/'
RESULTSPATH = './results/'
TITLE_TO_SEARCH = 'technician'

def find_files():
    """
    Goes through the DATASTORE directory and collects names of all the files
    :return: a list of all files
    """

    list_of_adverts = glob.glob(DATASTORE + '*')

    return list_of_adverts


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


def read_html(list_of_adverts):
    """
    Goes through the list of files in the DATASTORE dir, finds the ones related to job adverts
    using a regex, pulls out the <h1> tag, because that's generally the job title, then stores all
    the job titles in a column of a dataframe
    :param list_of_adverts: a list of the job advert filenames
    :return: a df with a column of job titles from the filenames
    """

    big_data_list = []

    # Setting up html tag remover
    clean_tag = re.compile('<.*?>')
    clean_lb = re.compile('\n')

    for current_ad in list_of_adverts:
        data = []
        filename = os.path.basename(current_ad)
        data.append(filename)

        # Check if the file is one of the job adverts (which have
        # a set patern of filename
        if re.match(r'\w\w\w\d\d\d', filename):

            with open(current_ad, "r") as f:
                contents = f.read()
                advert = BeautifulSoup(contents, 'lxml')
                advert_title = str(advert.h1).lower()
                # Remove html
                advert_title = re.sub(clean_tag, '', advert_title)
                data.append(advert_title)

                try:
                    date = advert.find('td', text='Placed on:').find_next_sibling('td').text
                except:
                    date = 0
                    pass

                try:
                    try_date = advert.find('th', text='Placed On:').find_next_sibling('td').text
                    # Only replace the date if the previous date is zero (i.e. don't overwrite
                    # a valid date from the last 'try'
                    if date == 0:
                        date = try_date
                except:
                    pass

                try:
                    role = advert.find('p', text='Type / Role:').find_next_sibling('p').text
                    role = re.sub(clean_lb, '', role)
                except:
                    role = 0
                    pass

                try:
                    try_role = advert.find('p', text='Type / Role:').find_next('a').text
                    # Only replace the role if the previous role is zero (i.e. don't overwrite
                    # a valid date from the last 'try'
                    if role == 0:
                        role = try_role
                except:
                    pass

                try:
                    try_role = advert.find('b', text='Type / Role:').find_next('div', {'class':'j-form-input ie-11-width'}).find_next('input').attrs['value']
                    # Only replace the role if the previous role is zero (i.e. don't overwrite
                    # a valid date from the last 'try'
                    if role == 0:
                        role = try_role
                except:
                    pass

                try:
                    try_role = advert.find('p', text='Type / Role:').find_next('div', {'class':'j-form-input ie-11-width'}).find_next('input').attrs['value']
                    # Only replace the role if the previous role is zero (i.e. don't overwrite
                    # a valid date from the last 'try'
                    if role == 0:
                        role = try_role
                except:
                    pass

                data.append(date)
                # Get the year for a separate column
                data.append(str(date)[-4:])
                data.append(role)

        big_data_list.append(data)

    df = pd.DataFrame.from_records(big_data_list)
    df.columns = ['filename', 'job title', 'date', 'year', 'role']

    return df


def find_rse(df, job_title):
    """
    Searches the job titles to find titles of interest. Also conducts some logic so that "Research Software Engineer"
    and "Software Engineer" aren't both flagged in the same job advert (RSE takes precendence here).
    :param df: the parsed info from the job adverts
    :return: a df with additional cols identifying rows of interest
    """

    # These find
    df['impact'] = np.where(df['job title'].str.contains('impact'), True, False)
    df[job_title] = np.where(df['job title'].str.contains(job_title), True, False)

    #df['impact manager'] = np.where(df['job title'].str.contains('impact manager'), True, False)
    #df['software developer'] = np.where(df['job title'].str.contains('software developer'), True, False)
    #df['software engineer'] = np.where(df['job title'].str.contains('software engineer'), True, False)
    #df['refined software engineer'] = np.where((df['software engineer'] == True) & (df[job_title] == False), True, False)

    return df


def main():
    """
    Main function to run program
    """

    start_time = time.time()

    # Check for dir for results and if it doesn't exist, create it
    resultsdir = check_for_results_dir(RESULTSPATH, TITLE_TO_SEARCH)
    # Add a forward slash so I can use dirname as a path
    resultsdir = resultsdir + '/'

    # Get filenames of all available jobs
    list_of_adverts = find_files()

    # Parse jobs html and read into df
    df = read_html(list_of_adverts)

    # Search the job titles and add cols to df that indicate where the title exists
    df = find_rse(df, TITLE_TO_SEARCH)

    # Logging
    file = open(resultsdir + TITLE_TO_SEARCH + '_job_parser_log.txt', 'w')
    logdate = datetime.now().strftime('%d/%m/%Y %H.%M.%S')
    file.write('Date and time: ' + str(logdate) + '\n \n')
    file.write('Looking at the job title: ' + TITLE_TO_SEARCH + '\n')
    file.write('There are ' + str(len(df)) + ' job adverts in the sample' + '\n \n')

    # Useful info to see whether the analysis has worked
    print(len(df))
    print(df[TITLE_TO_SEARCH].value_counts())

    export_to_csv(df, resultsdir, 'processed_jobs', False)

    print("--- %s seconds ---" % round((time.time() - start_time),1))
    file.write('Processing took ' + str(round((time.time() - start_time),1)) + '\n')

    file.close()

if __name__ == '__main__':
    main()
