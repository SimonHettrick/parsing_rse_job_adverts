#!/usr/bin/env python
# encoding: utf-8

from matplotlib import pyplot as plt
import numpy as np
from .parse_csv import export_to_csv

import pandas as pd
import settings

def clean_job_titles(df):

    # Clean rows that have missing title data
    df.dropna(subset=['job_title'], inplace=True)

    return df


def date_and_sort(df):
    """
    Drops all the rows that lack date data, then converts the date col to datetime and sorts the data by date
    :param df: all the parsed job advert data
    :return: the same df, but with the date data cleaned and sorted
    """

    df = df[df['start_date']!='no_data']
    df['start_date'] = pd.to_datetime(df['start_date'])
    df.sort_values(by=['start_date'], inplace=True, ascending=True)

    return df


def jobs_per_year(df):
    """
    Finds number of job adverts per year so we can work out percentages later
    :param df: all the parsed job advert data
    :return: a dict of year and number of adverts available
    """

    jobs_per_year_dict = df.value_counts(subset='year').to_dict()

    return jobs_per_year_dict


def find_jobs(df):
    """
    Searches the job titles to find titles of interest.
    :param df: the parsed info from the job adverts
    :return: a df with additional cols identifying rows of interest
    """

    for current_job in settings.jobs_of_interest:
        df[current_job] = np.where(df['job_title'].str.contains(current_job), True, False)

    return df


def enhance(df_original):

    # Create copt of original dataset to work on rather than manipulating the original

    df=df_original.copy()

    # Create a column which identifies rows which include any of the jobs of interest

    for current_job in settings.jobs_of_interest:
        mask = df[current_job] == True
        df.loc[mask, 'any_job'] = True

    # Flag jobs that are not of interest and remove them
    for not_job in settings.avoid_jobs:
        df.loc[:,'not_job'] = np.where(df['job_title'].str.contains(not_job), True, False)
        # The any_job col AND "NOT of not_job" will result in True only for those jobs that include
        # terms from the jobs_of_interest list and do not include terms from the avoid_jobs list
        df.loc[:,'keep_job'] = df['any_job'] & ~df['not_job']
        # Limit the df to only those jobs of interest
        bad_jobs = df.loc[df['keep_job'] == False]
        df.drop(bad_jobs.index,inplace=True)

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

    df_summ.sort_values(by = ['year'], inplace = True, ascending = True)

    return df_summ


def plot_job_summary(raw_data,interest_data,summary,resultspath,filedate):

    export_to_csv(summary, resultspath, 'jobs_by_year_'+filedate, False)

    plt.figure()
    summary.plot('year','number rse jobs',style = 'x-')
    #plt.xlim(summary['year'].min(),summary['year'].max())
    plt.grid(False)
    plt.savefig(resultspath + 'rse_jobs_per_year_' + filedate + '.png')
    plt.close()

    plt.figure()
    summary.plot('year','number all jobs',style = 'x-')
    #plt.xlim(summary['year'].min(),summary['year'].max())
    plt.grid(False)
    plt.savefig(resultspath + 'all_jobs_per_year_' + filedate + '.png')
    plt.close()

    datespan = (raw_data['start_date'].max()-raw_data['start_date'].min()).days

    plt.figure()
    ax = plt.axes()
    raw_data.hist('start_date',bins = datespan // 28,ax=ax)
    plt.grid(False)
    plt.savefig(resultspath + 'all_jobs_per_week_' + filedate + '.png')
    plt.close()

    plt.figure()
    ax = plt.axes()
    interest_data.hist('start_date',bins = datespan // 28,ax=ax)
    plt.grid(False)
    plt.savefig(resultspath + 'rse_jobs_per_week_' + filedate + '.png')
    plt.close()


def get_and_plot_salaries(df,resultspath,filedate,df2=None):

    # Fetch the minimum and maximum years in the dataset

    year_set=set(df['year'])
    min_year=int(min(year_set))
    max_year=int(max(year_set))

    years=range(min_year,max_year+1)

    # Store four values for plotting: mean salary (stored in 'salary'), clipped mean salary (mean
    # of the salaries after removing values outside the IQR, to remove outlier influence), max salary
    # and min salary.

    df_data={
        'salaries':[],
        'clipped_salaries':[],
        'max_salaries':[],
        'min_salaries':[],
    }

    # Create slices of the dataset per year in the range

    for year in years:
        df_slice=df[df['year'] == year]

        # Remove all jobs with no salary data

        df_slice=df_slice.dropna(subset = ['salary'])

        # If no jobs were present in a given year, append NaNs

        if len(df_slice)==0:
            df_data['salaries'].append(np.nan)
            df_data['clipped_salaries'].append(np.nan)
            df_data['max_salaries'].append(np.nan)
            df_data['min_salaries'].append(np.nan)

        else:

            # Calculate interquartile range for clipped_salaries

            Q1 = df_slice['salary'].quantile(0.25)
            Q3 = df_slice['salary'].quantile(0.75)

            # Remove values outside the IQR for clipped_salaries

            iq_df_slice = df_slice.query('@Q1 <= salary <= @Q3')

            # Append mean, clipped mean, max and min salaries to the lists

            df_data['salaries'].append(np.mean(df_slice['salary']))
            df_data['clipped_salaries'].append(np.mean(iq_df_slice['salary']))
            df_data['max_salaries'].append(np.max(df_slice['salary']))
            df_data['min_salaries'].append(np.min(df_slice['salary']))

    # Repeat the process for the second dataset if given

    if df2 is not None:
        df2_data={
            'salaries':[],
            'clipped_salaries':[],
            'max_salaries':[],
            'min_salaries':[],
        }

        for year in years:
            df_slice=df2[df2['year'] == year]
            df_slice=df_slice.dropna(subset = ['salary'])

            if len(df_slice)==0:
                df2_data['salaries'].append(np.nan)
                df2_data['clipped_salaries'].append(np.nan)
                df2_data['max_salaries'].append(np.nan)
                df2_data['min_salaries'].append(np.nan)
            
            else:
                Q1 = df_slice['salary'].quantile(0.25)
                Q3 = df_slice['salary'].quantile(0.75)

                iq_df_slice = df_slice.query('@Q1 <= salary <= @Q3')

                df2_data['salaries'].append(np.mean(df_slice['salary']))
                df2_data['clipped_salaries'].append(np.mean(iq_df_slice['salary']))
                df2_data['max_salaries'].append(np.max(df_slice['salary']))
                df2_data['min_salaries'].append(np.min(df_slice['salary']))

    # Define the plotter for compactness' sake

    def plot_salaries(data_label, title, filename):

        plt.figure()
        plt.title(title)
        plt.plot(years,df_data[data_label],label='RSE Jobs')
        plt.xlabel('Year')
        plt.ylabel('Salary (£/yr)')
        if df2 is not None:
            plt.plot(years,df2_data[data_label],label='All Jobs')
            plt.legend()
        plt.savefig(resultspath + filename + '_' + filedate + '.png')

    plot_salaries('salaries','Mean Salaries','rse_salary_per_year')
    plot_salaries('clipped_salaries','Mean Salaries','rse_salary_per_year_clipped')
    plot_salaries('max_salaries','Max Salaries','max_rse_salary_per_year')
    plot_salaries('min_salaries','Min Salaries','min_rse_salary_per_year')
