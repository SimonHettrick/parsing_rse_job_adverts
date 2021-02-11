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

DATASTORE = './ALL_JOBS_18-01-2021/'
RESULTSPATH = './results/'

def find_files():
    """
    Goes through the DATASTORE directory and collects names of all the files
    :return: a list of all files
    """

    list_of_adverts = glob.glob(DATASTORE + '*')

    return list_of_adverts


def export_to_csv(df, location, filename, index_write):
    """
    Exports a df to a csv file
    :params: a df and a location in which to save it
    :return: nothing, saves a csv
    """

    return df.to_csv(location + filename + '.csv', index=index_write)


def read_html(list_of_adverts):
    """
    Goes through the list of job adverts in the DATASTORE dir, extracts the data I need and adds it to a df
    :param list_of_adverts: a list of the job advert filenames
    :return: a df with a data extracted from job adverts (titles, start date, location, etc)
    """


    def find_title(advert):
        """
        Find the title from the job advert
        :param advert: the beatiful soup parsed version of an advert
        :return: a job title
        """
        try:
            title = advert.find('h1').text
            if len(title) == 0:
                title = 'no_data'
        except:
            title = 'no_data'
            pass

        if title != 'no_data':
            title = re.sub(clean_lb, '', title)
            title = title.lower()

        return title


    def find_date(advert):
        """
        Find the date on which the advert was placed
        :param advert: the beautiful soup parsed version of an advert
        :return: a date on which the advert was placed
        """
        try:
            date = advert.find('td', text='Placed on:').find_next_sibling('td').text
        except:
            date = 'no_data'
            pass

        try:
            try_date = advert.find('th', text='Placed On:').find_next_sibling('td').text
            # Only replace the date if the previous date is zero (i.e. don't overwrite
            # a valid date from the last 'try'
            if date == 'no_data':
                date = try_date
        except:
            pass

        return date


    def find_role(advert):
        """
        Find the role (i.e. the job family) in the advert
        :param advert: the beatiful soup parsed version of an advert
        :return: the role from the advert
        """
        try:
            role = advert.find('p', text='Type / Role:').find_next_sibling('p').text
            role = re.sub(clean_lb, '', role)
        except:
            role = 'no_data'
            pass

        try:
            try_role = advert.find('p', text='Type / Role:').find_next('a').text
            # Only replace the role if the previous role is zero (i.e. don't overwrite
            # a valid date from the last 'try'
            if role == 'no_data':
                role = try_role
        except:
            pass

        try:
            try_role = \
            advert.find('b', text='Type / Role:').find_next('div', {'class': 'j-form-input ie-11-width'}).find_next(
                'input').attrs['value']
            # Only replace the role if the previous role is zero (i.e. don't overwrite
            # a valid date from the last 'try'
            if role == 'no_data':
                role = try_role
        except:
            pass

        try:
            try_role = \
            advert.find('p', text='Type / Role:').find_next('div', {'class': 'j-form-input ie-11-width'}).find_next(
                'input').attrs['value']
            # Only replace the role if the previous role is zero (i.e. don't overwrite
            # a valid date from the last 'try'
            if role == 'no_data':
                role = try_role
        except:
            pass

        if role !='no_data':
            role = re.sub(clean_lb, '', role)
            role = role.lower()

        return role


    def find_organisation(advert):
        """
        Find the organisation (i.e. the university where the job is based) in the advert
        :param advert: the beatiful soup parsed version of an advert
        :return: the organisation from the advert
        """
        try:
            organisation = advert.find('h3').text.split('-',1)[0]
        except:
            organisation = 'no_data'
            pass

        if organisation !='no_data':
            organisation = re.sub(clean_lb, '', organisation)
            organisation = organisation.lower()

        return organisation


    def find_location(advert):
        """
        Find the location (i.e. the city where the job is based) in the advert
        :param advert: the beatiful soup parsed version of an advert
        :return: the location from the advert
        """
        try:
            location = advert.find('td', text='Location:').find_next_sibling('td').text
        except:
            location = 'no_data'
            pass

        try:
            try_location = advert.find('th', text='Location:').find_next_sibling('td').text
            if location == 'no_data':
                location = try_location
        except:
            pass

        if location !='no_data':
            location = re.sub(clean_lb, '', location)
            location = location.lower()
            location = location.strip()

        return location

    big_data_list = []

    # Setting up annoying text remover
    clean_lb = re.compile('\n')

    # Set up a counter to print on screen and assure me that everything's working
    sanity_counter=0

    # Go through all the ads and extract the data I need
    for current_ad in list_of_adverts:
        sanity_counter+=1
        data = []
        filename = os.path.basename(current_ad)
        data.append(filename)

        # Check if the file is one of the job adverts (which have
        # a set patern of filename
        if re.match(r'\w\w\w\d\d\d', filename):

            with open(current_ad, "r") as f:
                contents = f.read()
                advert = BeautifulSoup(contents, 'lxml')

                #Extract info I want
                title = find_title(advert)
                date = find_date(advert)
                # Extract year directly from date variable
                year = str(date)[-4:]
                role = find_role(advert)
                organisation = find_organisation(advert)
                location = find_location(advert)

                # Add the info to the data list
                data.append(title)
                data.append(date)
                data.append(year)
                data.append(role)
                data.append(organisation)
                data.append(location)

        # Add data to a list of lists which will later be transformed into a df
        big_data_list.append(data)

        # Not drowning but waving output for my sanity
        print('Processed ' + str(sanity_counter) + ' jobs', end='\r')

    df = pd.DataFrame.from_records(big_data_list)
    df.columns = ['filename', 'job title', 'date', 'year', 'role', 'organisation', 'location']

    return df


def main():
    """
    Main function to run program
    """

    start_time = time.time()

    # Get filenames of all available jobs
    list_of_adverts = find_files()

    # Logging
    file = open(RESULTSPATH + 'job_parser_log.txt', 'w')
    logdate = datetime.now().strftime('%d/%m/%Y %H.%M.%S')
    file.write('Date and time: ' + str(logdate) + '\n \n')
    file.write('There were ' + str(len(list_of_adverts)) + ' job adverts reviewed in the sample' + '\n \n')

    # Parse jobs html and read into df
    df = read_html(list_of_adverts)

    # Logging
    file.write('There were ' + str(len(df)) + ' job adverts were parsed into the data file' + '\n \n')

    export_to_csv(df, RESULTSPATH, '1_processed_jobs', False)

    print("--- %s seconds ---" % round((time.time() - start_time),1))
    file.write('Processing took ' + str(round((time.time() - start_time),1)) + '\n')

    file.close()

if __name__ == '__main__':
    main()
