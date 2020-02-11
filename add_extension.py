#!/usr/bin/env python
# encoding: utf-8

import pandas as pd
from bs4 import BeautifulSoup
import glob

DATASTORE = './testjobs/'


def find_files():

    list_of_adverts = glob.glob(DATASTORE + '*')

    return


def main():
    """
    Main function to run program
    """

    find_files()

if __name__ == '__main__':
    main()
