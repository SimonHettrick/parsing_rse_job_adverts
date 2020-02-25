#!/usr/bin/env python
# encoding: utf-8

import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
import glob
import os
import re

DATASTORE = './testjobs/'


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
    clean = re.compile('<.*?>')

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
                advert_title = re.sub(clean, '', advert_title)
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

                data.append(date)

        big_data_list.append(data)

    df = pd.DataFrame.from_records(big_data_list)
    df.columns = ['filename', 'job title', 'date']

    return df


def find_rse(df):

    df['rse'] = np.where(df['job title'].str.contains('research software engineer'), 'True', 'False')

#    df['rse'] = False
#    df['rse'][df['job title'].str.contains('research software engineer')] = 'True'

    return df


def main():
    """
    Main function to run program
    """

    list_of_adverts = find_files()

    df = read_html(list_of_adverts)

    df = find_rse(df)

    #print(df)
    #print(len(df))
    print(df['rse'].value_counts())

    export_to_csv(df, './', 'processed_jobs', False)

if __name__ == '__main__':
    main()
