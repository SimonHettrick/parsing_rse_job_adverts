from datetime import datetime
from matplotlib import pyplot as plt
import pandas as pd
import sys
import time

# Takes two csv files produced with 'jobs_to_csv.py' and compares the jobs present in each set, before
# creating a new csv merging the data from both inputs (prioritising the information in file1 for jobs
# present in both infiles.
#
# Call as 'python dataset_merger.py file1 file2' where file1 and file2 are relative paths to the csvs
# to be merged.  Saves its output csv in RESULTSPATH directory as RESULTSNAME, and also saves plots to
# the same directory showing number of jobs present in file 1 but not in file 2 (and vice versa) as a
# function of year.

start_time=time.time()

RESULTSPATH = './results/'
RESULTSNAME = '0_merged_processed_joblist.csv'

args=sys.argv[1:]

# Ensure user has passed two arguments to use as filenames

if len(args) != 2:
	raise ValueError('Must pass 2 values to script (names of the 2 parsed job csvs to be compared)')

def get_differences(df1, df2):

	# Given two dataframes, return the lists of filenames present in only one set or the other

	# Create sets of the job IDs in each unfiltered frame
	fileset1=set(df1['filename'])
	fileset2=set(df2['filename'])

	#   Find all filenames in set 1 but not in set 2

	uniques1 = fileset1.difference(fileset2)

	#   Find all filenames in set 2 but not in set 1

	uniques2 = fileset2.difference(fileset1)

	#   Return both lists

	return uniques1, uniques2


def compare_jobs(df1, df2):

	# Given two dataframes, return the number of filenames present in only one set or the other

	# Get lists of filenames unique to each dataset
	uniques1, uniques2 = get_differences(df1, df2)

	#   Find number of filenames in set 1 but not in set 2

	difference1 = len(uniques1)

	#   Find number of filenames in set 2 but not in set 1

	difference2 = len(uniques2)

	#   Return both values

	return difference1, difference2


def main(file1, file2):

	# Initialise logfile
	logfile = open(RESULTSPATH+'comparer_log.txt', 'w')
	logdate = datetime.now().strftime('%d/%m/%Y %H.%M.%S')
	logfile.write('Date and time: %s\n\n' % logdate)
	logfile.write('Finding difference between jobs listed in "%s" and "%s"\n\n' % (file1,file2))

    # Load csv files into dataframes
	print('Loading datasets...')
	df1=pd.read_csv(file1)
	df2=pd.read_csv(file2)

	# Removing bad data (any jobs without a name or year)

	df1.dropna(subset=['filename','job title','year'], inplace=True)
	df2.dropna(subset=['filename','job title','year'], inplace=True)

	# Fetch the number of jobs present in each set but absent from the other
	difference1,difference2=compare_jobs(df1,df2)

	print('Total jobs in csv1 missing from csv2: ',difference1)
	logfile.write('Jobs present in "%s" but absent from "%s": %i\n' % (file1,file2,difference1))

	print('Total jobs in csv2 missing from csv1: ',difference2)
	logfile.write('Jobs present in "%s" but absent from "%s": %i\n\n' % (file2,file1,difference2))

	print('Calculating differences by year...')

	# Find earliest and latest years in dataset to set up the loop

	year_set=set(df1.dropna(subset = ['year'])['year']) | set(df2.dropna(subset = ['year'])['year'])
	min_year=round(min(year_set))
	max_year=round(max(year_set))

	year_range=range(min_year, max_year+1)

	logfile.write('Calculating difference by year between %i and %i.\n\n' % (min_year,max_year))

	# Repeat the calculation for which jobs are only present in one set, but filter by year

	in_1_not_2 = []
	in_2_not_1 = []

	for year in year_range:

		# Filter frames on year=current year to get working df slices

		sub_df1=df1[df1['year'] == year]
		sub_df2=df2[df2['year'] == year]

		# Fetch diff jobs from each subframe

		s_diff1,s_diff2=compare_jobs(sub_df1, sub_df2)

		# add to arrays to be plotted on a yearly basis

		in_1_not_2.append(s_diff1)
		in_2_not_1.append(s_diff2)

		logfile.write('%s:\n' % year)
		logfile.write('In file 1 but not in 2: %i\n' % s_diff1)
		logfile.write('In file 2 but not in 1: %i\n\n' % s_diff2)

	# Prepare plots

	plt.figure()
	plt.title('Jobs in %s but not in %s' % (file1, file2))
	plt.plot(year_range, in_1_not_2)
	plt.savefig(RESULTSPATH + 'in_1_not_2.png')
	plt.close()

	plt.figure()
	plt.title('Jobs in %s but not in %s' % (file2, file1))
	plt.plot(year_range, in_2_not_1)
	plt.savefig(RESULTSPATH + 'in_2_not_1.png')
	plt.close()

	# Filter dataset 2 to only include those not present in 1

	_, uniques2 = get_differences(df1, df2)
	df2_extras=df2[df2['filename'].isin(uniques2)]

	# Merge datasets

	print('Merging datasets...')
	merged_df = pd.concat([df1, df2_extras])

	len_merged = len(merged_df)
	logfile.write('Merged jobs list has a length of %i\n' % len_merged)

	outfilename = RESULTSPATH + RESULTSNAME
	merged_df.to_csv(outfilename, index=False)

	print('Merged dataset with %i jobs saved to "%s"' % (len_merged, outfilename) )
	logfile.write('Merged file saved to %s\n\n' % outfilename)
	logfile.write('Processing took %fs' % (time.time() - start_time) )

	logfile.close()

main(*args)