#!/usr/bin/env python
# encoding: utf-8

import pandas as pd
from matplotlib import pyplot as plt
import numpy as np
import time
from glob import glob
from datetime import datetime
import sys


RESULTSPATH = './results/'
OUTRESULTSPATH = './results/'
MERGEDFILE_ROOT = '1_processed_merged_jobs'
RESULTSFILE_ROOT = '1_processed_jobs'

# Add a list of job titles that you want to search for here; stems such as 'scien' will catch 'science', 'scientist' etc
jobs_of_interest = ['data scien', 'data engineer', 'software develop', 'software engineer', 'research engineer', 'bioinformatic']

# Add a list of job titles that you're not interested in here
avoid_jobs = [ 'fellow', 'lecturer', 'student', 'tutor', 'profess']


if len(sys.argv)>1:

    RESULTSPATH=''
    RESULTSFILENAME=sys.argv[1]
    RESULTSDATE=datetime.now().strftime("%Y-%m-%d")

else:

    # Fetch list of viable parsed csv files
    single_csvs=glob(RESULTSPATH+RESULTSFILE_ROOT+'_*.csv')
    merged_csvs=glob(RESULTSPATH+MERGEDFILE_ROOT+'_*.csv')

    parsed_csvs = single_csvs + merged_csvs

    # Automatically fetch the most recent csv, prioritising merged files
    parsed_csvs.sort()
    RESULTSFILENAME = parsed_csvs[-1].split('/')[-1]

    # Uncomment the line below to override this and manually pick an input file
    #RESULTSFILENAME = './processed_jobs_2000-01-01.csv'

    # Fetch the RESULTSDATE from the results filename
    RESULTSDATE = RESULTSFILENAME[-14:-4]

    print( 'Found parsed job data data at', RESULTSFILENAME )

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


def enhance(df_original, jobs_of_interest, avoid_jobs):

    # Create copt of original dataset to work on rather than manipulating the original

    df=df_original.copy()

    # Create a column which identifies rows which include any of the jobs of interest

    for current_job in jobs_of_interest:
        mask = df[current_job]==True
        df.loc[mask, 'any_job'] = True

    # Flag jobs that are not of interest and remove them
    for not_job in avoid_jobs:
        df.loc[:,'not_job'] = np.where(df['job title'].str.contains(not_job), True, False)
        # The any_job col AND "NOT of not_job" will result in True only for those jobs that include
        # terms from the jobs_of_interest list and do not include terms from the avoid_jobs list
        df.loc[:,'keep_job'] = df['any_job'] & ~df['not_job']
        # Limit the df to only those jobs of interest
        bad_jobs = df.loc[df['keep_job']==False]
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


def plot_job_summary(raw_data,interest_data,summary):

    export_to_csv(summary, OUTRESULTSPATH, 'jobs_by_year'+RESULTSDATE, False)

    plt.figure()
    summary.plot('year','number rse jobs',style = 'x-')
    #plt.xlim(summary['year'].min(),summary['year'].max())
    plt.grid(False)
    plt.savefig(OUTRESULTSPATH + 'rse_jobs_per_year_' + RESULTSDATE + '.png')
    plt.close()

    plt.figure()
    summary.plot('year','number all jobs',style = 'x-')
    #plt.xlim(summary['year'].min(),summary['year'].max())
    plt.grid(False)
    plt.savefig(OUTRESULTSPATH + 'all_jobs_per_year_' + RESULTSDATE + '.png')
    plt.close()

    datespan = (raw_data['date'].max()-raw_data['date'].min()).days

    plt.figure()
    ax = plt.axes()
    raw_data.hist('date',bins = datespan // 28,ax=ax)
    plt.grid(False)
    plt.savefig(OUTRESULTSPATH + 'all_jobs_per_week_' + RESULTSDATE + '.png')
    plt.close()

    plt.figure()
    ax = plt.axes()
    interest_data.hist('date',bins = datespan // 28,ax=ax)
    plt.grid(False)
    plt.savefig(OUTRESULTSPATH + 'rse_jobs_per_week_' + RESULTSDATE + '.png')
    plt.close()


def get_and_plot_salaries(df,df2=None):

    # Fetch the minimum and maximum years in the dataset

    year_set=set(df['year'])
    min_year=round(min(year_set))
    max_year=round(max(year_set))

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
        plt.savefig(OUTRESULTSPATH + filename + '_' + RESULTSDATE + '.png')

    plot_salaries('salaries','Mean Salaries','rse_salary_per_year')
    plot_salaries('clipped_salaries','Mean Salaries','rse_salary_per_year_clipped')
    plot_salaries('max_salaries','Max Salaries','max_rse_salary_per_year')
    plot_salaries('min_salaries','Min Salaries','min_rse_salary_per_year')

#    plt.figure()
#    plt.title('Mean Salaries')
#    plt.plot(years,salaries,label='RSE Jobs')
#    if df2 is not None:
#        plt.plot(years,salaries2,label='All Jobs')
#       plt.legend()
#    plt.savefig(OUTRESULTSPATH + 'rse_salary_per_year_' + RESULTSDATE + '.png')

#    plt.figure()
#    plt.title('Max Salaries')
#    plt.plot(years,max_salaries,label='RSE Jobs')
#    if df2 is not None:
#        print('===MAX===')
#        print(df2.loc[df2['salary'].idxmax()])
#        plt.plot(years,max_salaries2,label='All Jobs')
#        plt.legend()
#    plt.savefig(OUTRESULTSPATH + 'max_rse_salary_per_year_' + RESULTSDATE + '.png')

#    plt.figure()
#    plt.title('Min salaries')
#    plt.plot(years,min_salaries,label='RSE Jobs')
#    if df2 is not None:
#        print('===MIN===')
#        print(df2.loc[df2['salary'].idxmin()])
#        plt.plot(years,min_salaries2,label='All Jobs')
#        plt.legend()
#    plt.savefig(OUTRESULTSPATH + 'min_rse_salary_per_year_' + RESULTSDATE + '.png')


def main():
    """
    Main function to run program
    """

    start_time = time.time()

    # Logging
    file = open(OUTRESULTSPATH + 'find_jobs_log.txt', 'w')
    logdate = datetime.now().strftime('%d/%m/%Y %H.%M.%S')
    file.write('Date and time: ' + str(logdate) + '\n \n')
    file.write('Analysing job list in "'+RESULTSFILENAME+'"')
    # Get parsed job advert data
    df = import_csv_to_df(RESULTSPATH, RESULTSFILENAME)
    # Convert date column to datetime objects
    print('Extracting date information...')
    df['date']= pd.to_datetime(df['date'],format='mixed')

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

    # Make plots based on the data summary

    plot_job_summary(df,df_interest,df_summ)

    # Collect salary stats on the data on a per-year basis

    get_and_plot_salaries(df_interest,df)

    # Export data
    export_to_csv(df, OUTRESULTSPATH, '2_named_processed_jobs_'+RESULTSDATE, False)

    # Logging
    file.write('There are ' + str(len(df_interest)) + ' jobs with the job title of interest' + '\n \n')
    # Export enhanced data
    export_to_csv(df_interest, OUTRESULTSPATH, '3_identified_jobs_'+RESULTSDATE, False)

    # Export data
    export_to_csv(df_summ, OUTRESULTSPATH, '4_summary_identified_jobs_'+RESULTSDATE, False)

    print("--- %s seconds ---" % round((time.time() - start_time),1))
    file.write('Processing took ' + str(round((time.time() - start_time),1)) + '\n')

    file.close()

if __name__ == '__main__':
    main()
