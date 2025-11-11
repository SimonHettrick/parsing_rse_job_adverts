#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
A full stack pipeline to scrape job postings from the web, package them locally in tarfiles,
parse their information, store the resultant information in a database, and perform some
basic analysis on the results.
"""

import sys
from datetime import datetime

from libs import raw, scrape_jobs
import settings

# Default values for datastore and resultspath when not specified at command line

datastores = settings.DEFAULT_DATASTORES

# DATASTORES can be overridden by passing a list of directories as arguments when
# running this script on the command line, e.g.
#  > python jobs_to_csv.py ./job_ads_simon ./job_ads_edinburgh

in_args=sys.argv
if '--test' in in_args:
    datastores = settings.TEST_DATASTORES

# ---------------------------------------------------

def main(run_time, logfile):
    """
    Main function to run program

    :params: a datetime object to use as the 'start time' throughout the pipeline, and
            an opened file in write mode to use as a log file
    """

    # ===== Prep =====

    if (not '--no-scrape' in in_args) and (not '--from-db' in in_args):
        logfile.write('Scraping new jobs:')
        scrape_jobs.scrape()

    raw.parse_from_raw(
        datastores=datastores,
        logfile=logfile,
        start_time=run_time,
    )

    logfile.close()


if __name__ == '__main__':

    now = datetime.now()

    fdate = now.strftime("%Y-%m-%d")
    with open('pipeline_log_'+fdate+'.txt', 'w', encoding='utf-8') as lfile:
        main(run_time=now, logfile=lfile)
