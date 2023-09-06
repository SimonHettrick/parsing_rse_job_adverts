#!/usr/bin/env python
# encoding: utf-8

import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
import glob
import os
import re
import sys
import time
from datetime import datetime
from collections import OrderedDict


# Default values for datastore and resultspath when not specified at command line

DATASTORE = './job_ads/'
RESULTSPATH = './results/'

# DATASTORE and RESULTSPATH can be overridden by passing these arguments when running this script on the command line,
# e.g. 
#  > python jobs_to_csv.py ./job_ads_simon ./simon_results

in_args=sys.argv

if len(in_args)<3:

    pass

else:

    # Dont change these values, these are for the command-line override

    DATASTORE = in_args[1]
    RESULTSPATH = in_args[2]


# ---------------------------------------------------

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
                title = ''
        except:
            title = ''
            pass

        if title != '':
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
            date = ''
            pass

        try:
            try_date = advert.find('th', text='Placed On:').find_next_sibling('td').text
            # Only replace the date if the previous date is '' (i.e. don't overwrite
            # a valid date from the last 'try'
            if date == '':
                date = try_date
        except:
            pass

        return date


    def find_role(advert):
        """
        Find the role (i.e. the job family) in the advert
        :param advert: the beautiful soup parsed version of an advert
        :return: the role from the advert
        """
        try:
            role = advert.find('p', text='Type / Role:').find_next_sibling('p').text
            role = re.sub(clean_lb, '', role)
        except:
            role = ''
            pass

        try:
            try_role = advert.find('p', text='Type / Role:').find_next('a').text
            # Only replace the role if the previous role is zero (i.e. don't overwrite
            # a valid date from the last 'try'
            if role == '':
                role = try_role
        except:
            pass

        try:
            try_role = \
            advert.find('b', text='Type / Role:').find_next('div', {'class': 'j-form-input ie-11-width'}).find_next(
                'input').attrs['value']
            # Only replace the role if the previous role is zero (i.e. don't overwrite
            # a valid date from the last 'try'
            if role == '':
                role = try_role
        except:
            pass

        try:
            try_role = \
            advert.find('p', text='Type / Role:').find_next('div', {'class': 'j-form-input ie-11-width'}).find_next(
                'input').attrs['value']
            # Only replace the role if the previous role is zero (i.e. don't overwrite
            # a valid date from the last 'try'
            if role == '':
                role = try_role
        except:
            pass

        if role !='':
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
            organisation = ''
            pass

        if organisation !='':
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
            location = ''
            pass

        try:
            try_location = advert.find('th', text='Location:').find_next_sibling('td').text
            if location == '':
                location = try_location
        except:
            pass

        if location !='':
            location = re.sub(clean_lb, '', location)
            location = location.lower()
            location = location.strip()

        return location


    def find_salary(advert):
        """
        Find the salary
        :param advert: the beautiful soup parsed version of an advert
        :return: a text field describing salary
        """
        try:
            salary = advert.find('th', text='Salary:').find_next_sibling('td').text
        except:
            return ''

        # Remove carriage returns, tabs, brackets,slashes and commas
        salary_string = salary.replace('\n', ' ').replace('\t', ' ').replace(',', '').replace('(',' ').replace(')',' ')

        # Remove spaces either side of dashes and slashes to better locate salary ranges,
        # convert slashes into dashes so they will be treated the same (e.g. 10000-30000 and
        # 10000/30000 will both be treated as 20000).
        salary_string = salary_string.replace('- ','-').replace(' -','-')
        salary_string = salary_string.replace(' /','-').replace('/ ','-').replace('/','-')

        # Define function to search for and extract salaries from a string when given an
        # arbitrary currency code or symbol to search for

        def extract_values_by_currency(salary_string,currency_symbol,conversion=1):

            salary_strings = salary_string.split(currency_symbol)[1:]
            salaries=[]

            # For each value appearing after that symbol...
            for salary in salary_strings:

                # Get numeric value immediately after currency sign
                salary=salary.strip().split(' ')

                # Remove any 'per annum' denotation that wasnt space-separated
                salary_cleaned=salary[0].replace('pa','').replace('PA','').replace('p.a.','').replace('per','')

                # Remove various other symbols, interpret 'xxxxx+' as just 'xxxxx'
                salary_cleaned=salary_cleaned.replace('+','').replace('*','').replace(';','')

                # Remove trailing -s (these happen when salaries are given as e.g. £30000-£40000, so both ends
                # of the range will already be encapsulated and trailing - can be ignored)
                salary_cleaned=salary_cleaned.strip('-')
                
                # Turn '40k' back into '40000', etc
                salary_cleaned=salary_cleaned.replace('k','000').replace('K','000')

                # Deal with ranges; deal with low value now, append other value onto the end of the loop list for later
                if '-' in salary_cleaned:
                    sc_split=salary_cleaned.split('-')
                    salary_cleaned=sc_split[0]
                    salary_strings.append(sc_split[1])

                # If it still cant be parsed, throw it out

                try:
                    salary_value=float(salary_cleaned)
                except:
                    continue

                # Convert to GBP

                salary_gbp=salary_value*conversion

                # Do not save small numbers which relate to grades or hourly pay

                if salary_gbp<=12000:
                    continue

                # If there's a huge salary, something has probably gone wrong, so remove these too

                if salary_gbp>500000:
                    continue

                salaries.append(salary_gbp)

            # After all the fireworks, check we actually got some sane salary values out, else return ''

            if len(salaries)==0:
                return ''

            else:
                return np.mean(salaries)

        # Create a dictionary of currencies to scan for with their conversion rates

        # Format: tuple of symbols, conversion rate from currency to GBP  They will be looked for in this order,
        # so keep USD near the bottom so '$' doesnt trigger for 'AUS $', for example

        # Currencies based on interatively looking through unparseable files to see what could scoop more values.
        # Exchange rates from xe.com in Sep 2023

        currencies=OrderedDict()
        currencies[('£','GBP')]=1
        currencies[('€','EUR')]=0.85
        currencies[('SEK')]=0.07
        currencies[('DKK')]=0.11
        currencies[('CHF')]=0.90
        currencies[('MOP')]=0.098 # Macau
        currencies[('RMB')]=0.11
        currencies[('JPY')]=0.0054
        currencies[('A$','AUD$','AUD $','AUD')]=0.51
        currencies[('CAD$','CAD $','CAD')]=0.58
        currencies[('HKD$','HK $','HKD')]=0.10
        currencies[('NZD$','NZD $','NZD')]=0.47
        currencies[('S$','SGD$','SGD $','SGD')]=0.58
        currencies[('Col$','COP')]=0.00019
        currencies[('USD$','USD','$')]=0.79

        # Run the currency scanner for all currencies listed

        for currency in currencies:
            conversion=currencies[currency]
            for symbol in currency:
                if symbol in currency:
                    salary=extract_values_by_currency(salary_string,symbol,conversion)

                    # If salary succesfully found, return it and dont run the rest of the tests

                    if salary!='':
                        return salary
                else:
                    continue

        # If no symbols yielded sane results, return empty string

        return ''


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
                salary = find_salary(advert)

                # Extract year directly from date variable (there's two forms of date, hence the if)
                if date=='':
                    year = ''
                elif '-' in date:
                    year = str(date)[:4]
                else:
                    year = str(date)[-4:]

                role = find_role(advert)
                organisation = find_organisation(advert)
                location = find_location(advert)

                # Add the info to the data list
                data.append(title)
                data.append(date)
                data.append(year)
                data.append(salary)
                data.append(role)
                data.append(organisation)
                data.append(location)

        # Add data to a list of lists which will later be transformed into a df
        big_data_list.append(data)

        # Not drowning but waving output for my sanity
        print('Processed ' + str(sanity_counter) + ' jobs', end='\r')

    df = pd.DataFrame.from_records(big_data_list)
    df.columns = ['filename', 'job title', 'date', 'year', 'salary', 'role', 'organisation', 'location']

    return df


def main():
    """
    Main function to run program
    """

    start_time = time.time()

    # Get filenames of all available jobs
    list_of_adverts = find_files()

    now = datetime.now()
    flndate = now.strftime("%Y-%m-%d")
    logdate = now.strftime('%d/%m/%Y %H.%M.%S')

    # Logging
    logfile = open(RESULTSPATH + 'job_parser_log_'+flndate+'.txt', 'w')

    logfile.write('Date and time: ' + str(logdate) + '\n \n')
    logfile.write('There were ' + str(len(list_of_adverts)) + ' job adverts reviewed in the sample' + '\n \n')

    # Parse jobs html and read into df
    df = read_html(list_of_adverts)

    # Logging
    logfile.write('There were ' + str(len(df)) + ' job adverts were parsed into the data file' + '\n')

    n_invalid = sum((df['date'] == '') & (df['job title'] == ''))

    logfile.write(' - ' +str(n_invalid) + ' were missing date and/or title data\n\n')

    export_to_csv(df, RESULTSPATH, '1_processed_jobs_'+flndate, False)

    print("--- %s seconds ---" % round((time.time() - start_time),1))
    logfile.write('Processing took ' + str(round((time.time() - start_time),1)) + 's\n')

    logfile.close()

if __name__ == '__main__':
    main()
