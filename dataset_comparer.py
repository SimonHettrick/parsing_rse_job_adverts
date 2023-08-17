import sys
import pandas as pd

args=sys.argv[1:]

# Ensure user has passed two arguments to use as filenames

if len(args)!=2:
	raise ValueError('Must pass 2 values to script (names of the 2 parsed job csvs to be compared)')

def compare_filesets(dataset1,dataset2):

	# Given two sets of filenames, return the number of filenames present in only one set or the other

	#   Find all filenames in set 1 but not in set 2

	difference1=len(dataset1.difference(dataset2))

	#   Find all filenames in set 2 but not in set 1

	difference2=len(dataset2.difference(dataset1))

	#   Return both values

	return difference1, difference2

def main(file1,file2):

	print('Loading datasets...')
	df1=pd.read_csv(file1)
	df2=pd.read_csv(file2)

	fileset1=set(df1['filename'])
	fileset2=set(df2['filename'])

	difference1,difference2=compare_filesets(fileset1,fileset2)

	print(difference1, difference2)

main(*args)