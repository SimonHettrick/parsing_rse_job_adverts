# Takes a csv files produced with 'dataset_merger.py', corresponding to a list of unique job info from
# 2 different sources with the source indicated for each, and the locations of the two folders containing
# the raw data for each of those sources.  Using the basic analysis already performed in the dataset_merger,
# repackages the large amount of raw job ad html files, ignores duplicates, sorts the ads by date and
# creates a single tar file for each year containing all ads from either source corresponding to that
# year.

#
# Call as 'python job_tarrer.py mergefile dir1 dir2' where mergefile is the relative path to the merged csv
# and dir1 and dir2 are the relative paths to the directories containing raw html job info for source 1
# and source 2 respectively.  Saves its output tarfiles in TARPATH directory as jobs_XXXX, where XXXX is
# the relevant year.

import pandas as pd
import sys
import tarfile
import time

start_time=time.time()

TARPATH = './tarred_job_data/'

args=sys.argv[1:]

# Ensure user has passed two arguments to use as filenames

if len(args) != 3:
    raise ValueError('Must pass 3 values to script (the merged data csv and two raw data directories)')

def main(mergefile, dir1, dir2):
    print('Loading merged dataset...')
    df = pd.read_csv(mergefile)
    
    # Get the valid years
    df['year'] = df['year'].astype(int)
    valid_years = df['year'].unique()
    
    for year in valid_years:
        y_df = df.loc[df['year'] == year]
        print('Creating tar for '+str(year))
        tar = tarfile.open(TARPATH+'jobs_'+str(year)+'.tar.gz', 'w|gz')
        
        for _, job in y_df.iterrows():
            if job['source']==1:
                source_dir = dir1
            else:
                source_dir = dir2
                
            tar.add(source_dir+job['filename'], arcname=job['filename'])
            
        tar.close()

main(*args)
