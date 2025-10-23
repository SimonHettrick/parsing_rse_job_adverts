#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Library of functions to deal with parsing values of interest out of the raw html
"""

import glob
import json
import os
import re
from collections import OrderedDict

from bs4 import BeautifulSoup
import numpy as np
import pandas as pd

def find_files(location):
    """
    Goes through the DATASTORE directory and collects names of all the files
    :return: a list of all files
    """

    list_of_adverts = glob.glob(location + '/*')

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
    Goes through the list of job adverts in the DATASTORE dir, extracts the data I need and adds it
    to a df
    
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
                title = None
        except AttributeError:
            title = None

        if title is not None:
            title = re.sub(clean_lb, '', title)
            title = title.lower()

        return title


    def find_description(advert):
        """
        Find the description from the job advert
        :param advert: the beatiful soup parsed version of an advert
        :return: a job description
        """
        try:
            dlist = advert.find('div', id='job-description').contents
            description = ''.join(map(str, dlist))

        except AttributeError:
            locscript = advert.find('script', type="application/ld+json")
            if not locscript:
                return None
            parsed_script = json.loads(locscript.string)
            try:
                description = parsed_script['description']
            except KeyError:
                return None


        description = re.sub(clean_lb, '', description)
        description = description.lower()

        return description


    def find_role(advert):
        """
        Find the role (i.e. the job family) in the advert
        :param advert: the beautiful soup parsed version of an advert
        :return: the role from the advert
        """
        try:
            role = advert.find('p', string='Type / Role:').find_next_sibling('p').text
            role = re.sub(clean_lb, '', role)
        except AttributeError:
            role = None

        try:
            try_role = advert.find('p', string='Type / Role:').find_next('a').text
            # Only replace the role if the previous role is zero (i.e. don't overwrite
            # a valid date from the last 'try'
            if role in (None, '', 'Ok') and try_role not in (None, '', 'Ok'):
                role = try_role
        except (AttributeError, IndexError):
            pass

        try:
            try_role = \
            advert.find('b', string='Type / Role:').find_next(
                'div', {'class': 'j-form-input ie-11-width'}).find_next('input').attrs['value']
            # Only replace the role if the previous role is zero (i.e. don't overwrite
            # a valid date from the last 'try'
            if role in (None, '', 'Ok') and try_role not in (None, '', 'Ok'):
                role = try_role
        except (AttributeError, IndexError):
            pass

        try:
            try_role = \
            advert.find('p', string='Type / Role:').find_next(
                'div', {'class': 'j-form-input ie-11-width'}).find_next('input').attrs['value']
            # Only replace the role if the previous role is zero (i.e. don't overwrite
            # a valid date from the last 'try'
            if role in (None, '', 'Ok') and try_role not in (None, '', 'Ok'):
                role = try_role
        except (AttributeError, IndexError):
            pass

        if role not in (None, ''):
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
        except AttributeError:
            organisation = None

        if organisation not in ('', None):
            organisation = re.sub(clean_lb, '', organisation)
            organisation = organisation.lower()

        return organisation


    def find_generic(advert, search_strings):
        """
        Find the location (i.e. the city where the job is based) in the advert
        :param advert: the beatiful soup parsed version of an advert
        :return: the location from the advert
        """
        for search_string in search_strings:
            for ctype in ('td', 'th'):
                try:
                    value = advert.find(ctype, string=search_string).find_next_sibling('td').text
                    if value not in ('', None):
                        break
                except AttributeError:
                    value=None
            if value not in ('', None):
                break

        if value not in ('', None):
            value = re.sub(clean_lb, '', value)
            value = value.lower()
            value = value.strip()

        return value


    def find_date(advert, search_strings):
        """
        Find a date labelled with any of the labels passed as search_strings
        :param advert: the beautiful soup parsed version of an advert
        :param search_strings: a list of strings (from high to low priority)
                                to look for when searching for the required
                                date.
        :return: a date matching the relevant
        """

        for search_string in search_strings:
            try:
                date = advert.find('td', string=search_string).find_next_sibling('td').text
                date = date.replace('th','').replace('1st','1')
                date = date.replace('2nd','2').replace('3rd','3')
                return date
            except AttributeError:
                pass

            # Only replace the date if the previous date is '' (i.e. don't overwrite
            # a valid date from the last 'try'
            try:
                date = advert.find('th', string=search_string).find_next_sibling('td').text
                date = date.replace('th','').replace('1st','1')
                date = date.replace('2nd','2').replace('3rd','3')
                return date
            except AttributeError:
                pass

        return None


    def find_loc_data(advert):
        """
        Find the country where the job is based
        :param advert: the beatiful soup parsed version of an advert
        :return: the location from the advert
        """
        locscript = advert.find('script', type="application/ld+json")
        if not locscript:
            return None, None, None

        parsed_script = json.loads(locscript.string)
        try:
            city = parsed_script['jobLocation'][0]['address']['addressLocality']
        except (KeyError, IndexError):
            city = None

        try:
            region = parsed_script['jobLocation'][0]['address']['addressRegion']
        except (KeyError, IndexError):
            region = None

        try:
            country = parsed_script['jobLocation'][0]['address']['addressCountry']
        except (KeyError, IndexError):
            country = None

        return city, region, country


    def find_salary(advert):
        """
        Find the salary
        :param advert: the beautiful soup parsed version of an advert
        :return: a text field describing salary
        """
        try:
            salary = advert.find('th', string='Salary:').find_next_sibling('td').text
        except AttributeError:
            try:
                salary = advert.find('th', string='Funding amount:').find_next_sibling('td').text
            except AttributeError:
                return None, None

        # Remove carriage returns, tabs, brackets,slashes and commas
        salary_string = salary.replace('\n', ' ').replace('\t', ' ').replace(',', '').replace(
            '(',' ').replace(')',' ')

        # Remove spaces either side of dashes and slashes to better locate salary ranges,
        # convert slashes into dashes so they will be treated the same (e.g. 10000-30000 and
        # 10000/30000 will both be treated as min 10000, max 30000).
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
                salary_cleaned=salary[0].replace('pa','').replace('PA','').replace(
                    'p.a.','').replace('per','')

                # Remove various other symbols, interpret 'xxxxx+' as just 'xxxxx'
                salary_cleaned=salary_cleaned.replace('+','').replace('*','').replace(';','')

                # Remove trailing -s (these happen when salaries are given as e.g. £30000-£40000,
                # so both ends of the range will already be encapsulated and trailing - can be
                # ignored)
                salary_cleaned=salary_cleaned.strip('-')

                # Turn '40k' back into '40000', etc
                salary_cleaned=salary_cleaned.replace('k','000').replace('K','000')

                # Deal with ranges; deal with low value now, append other value onto the end of the
                # loop list for later
                if '-' in salary_cleaned:
                    sc_split=salary_cleaned.split('-')
                    salary_cleaned=sc_split[0]
                    salary_strings.append(sc_split[1])

                # If it still cant be parsed, throw it out

                try:
                    salary_value=float(salary_cleaned)
                except ValueError:
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

            # After all the fireworks, check we actually got sane salary values out, else return ''

            if len(salaries)==0:
                return None, None

            return np.min(salaries), np.max(salaries)

        # Create a dictionary of currencies to scan for with their conversion rates

        # Format: tuple of symbols, conversion rate from currency to GBP  They will be looked for
        # in this order, so keep USD near the bottom so '$' doesnt trigger for 'AUS $', for example

        # Currencies based on interatively looking through unparseable files to see what could
        # scoop more values.

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
                    min_salary, max_salary=extract_values_by_currency(
                        salary_string,symbol,conversion)

                    # If salary succesfully found, return it and dont run the rest of the tests

                    if min_salary!='':
                        return min_salary, max_salary
                else:
                    continue

        # If no symbols yielded sane results, return empty string

        return None, None


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

            with open(current_ad, "r", encoding="utf-8") as f:
                contents = f.read()
                advert = BeautifulSoup(contents, 'lxml')

                #Extract info I want
                title = find_title(advert)
                description = find_description(advert)
                contract_type = find_generic(advert, ['Contract Type:'])
                placed_on = find_date(advert, ['Placed On:'])
                closes_on = find_date(advert, ['Closes:', 'Expires:'])
                salary_min, salary_max = find_salary(advert)
                role = find_role(advert)
                hours = find_generic(advert, ['Hours:'])
                job_ref = find_generic(advert, ['Job Ref:', 'Reference:'])
                organisation = find_organisation(advert)
                location_string = find_generic(advert, ['Location:'])
                city, region, country = find_loc_data(advert)

                # Add the info to the data list
                data.append(title)
                data.append(description)
                data.append(contract_type)
                data.append(placed_on)
                data.append(closes_on)
                data.append(salary_min)
                data.append(salary_max)
                data.append(role)
                data.append(hours)
                data.append(job_ref)
                data.append(organisation)
                data.append(location_string)
                data.append(city)
                data.append(region)
                data.append(country)

        # Add data to a list of lists which will later be transformed into a df
        big_data_list.append(data)

        # Not drowning but waving output for my sanity
        print('Processed ' + str(sanity_counter) + ' jobs', end='\r')

    df = pd.DataFrame.from_records(big_data_list)

    try:
        df.columns = [
            'filename',
            'job_title',
            'description',
            'contract_type',
            'placed_on',
            'closes_on',
            'salary_min',
            'salary_max',
            'role',
            'hours',
            'job_ref',
            'organisation',
            'location_string',
            'city',
            'region',
            'country',
        ]
    except ValueError:
        print('--- folder is empty, skipping ---')

    return df
