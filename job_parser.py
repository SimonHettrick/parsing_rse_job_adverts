#!/usr/bin/env python
# encoding: utf-8

import pandas as pd
from bs4 import BeautifulSoup
import glob
import os
import re

DATASTORE = './testjobs/'


def find_files():

    list_of_adverts = glob.glob(DATASTORE + '*')

    return list_of_adverts


def import_csv_to_df(location, filename):
    """
    Imports a csv file into a Pandas dataframe
    :params: an xls file and a sheetname from that file
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

    for current_add in list_of_adverts:
        filename = os.path.basename(current_add)

        # Check if the file is one of the job adverts (which have
        # a set patern of filename
        if re.match(r'\w\w\w\d\d\d', filename):

            with open(current_add, "r") as f:
                contents = f.read()
                advert = BeautifulSoup(contents, 'lxml')
                print(advert.h1)

    return


def main():
    """
    Main function to run program
    """

    list_of_adverts = find_files()

    job_titles = read_html(list_of_adverts)

if __name__ == '__main__':
    main()
