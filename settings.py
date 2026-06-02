#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
A collection of settings which can be altered to change the function of the pipeline.
"""

import os

# === Scraper settings ===

# The repository where the scraped job html files will be stored
SCRAPE_DATASTORE = './scraped_jobs/'

# The maximum number of jobs to scrape per attempt
NUM_JOBS = 10000

# The base URL of the job repository to be scraped
BASE_URL = "http://www.jobs.ac.uk"

# The full URL used to access the current list of job listings
FULL_URL = f"{BASE_URL}/search/?keywords=*&sort=re&s=1&pageSize={NUM_JOBS}"

# A list of 'locations' to screen out when processing job location information
IGNORE_LOCATIONS = (
    'hybrid',
    'remote',
    'remote/on-site',
    'work from home',
    'hybrid/on-site',
    'hybrid/remote',
    'field based',
    'home based',
    'home-based',
    'home',
    'homebase',
    'home-based / online',
    'online',
)

# === API settings ===

GOOGLE_GEOCODE_URL = 'https://maps.googleapis.com/maps/api/geocode/json'

# The API key to use to poll the Google Places API
GOOGLE_PLACES_KEY = os.environ.get('PLACES_API_KEY', None)

# === Local storage settings ===

# A list of directories to look for job html files in
DEFAULT_DATASTORES = ['./job_ads/soton/', './job_ads/edin/', SCRAPE_DATASTORE]

# A list of directories to look for job html files in when running with the --test flag
TEST_DATASTORES = ['./test_job_ads/soton/', './test_job_ads/edin/', SCRAPE_DATASTORE]

# The path to the location where tarfiles are to be created
TARPATH = './tarred_jobs/'

# The full path of the database file where the parsed job data is to be stored
DB_LOCATION = './db/jobs.sqlite'


# === Frontend settings ===

# A list of words to ignore when generating ANY WordClouds.  Stopwords defined in
# `wordloud.STOPWORDS` will also be ignored.
ADDITIONAL_STOPWORDS = [
    'will',
    'must',
    'us',
    'well',
    'http',
    'https',
    'embl',
    'please',
    'may',
]

# A threshold (defined as a ratio of total counts) below which a category should not be plotted
# when creating a histogram, instead collecting all of these results into a single 'other'
# category.
HISTOGRAM_CUTOFF = 0.05
