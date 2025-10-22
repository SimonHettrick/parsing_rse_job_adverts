# Job parsing

Bit of a complex background to this one. I set it up to look at RSE jobs from the jobs.ac.uk data, then expanded it to look at any type of jobs for some policy work I was doing for the Hidden REF. It's now mainly used for RSE jobs again.

# Setting Up

To set up the RSE job parsing pipeline, first create and activate a Python virtual environment and install `requirements.txt`.

In order to store the various data products created by the pipeline, you will need to set up a number of directories in the project root directory.  The names and locations of these directories can be changed in `settings.py`, but the recommended directories to set up (all under the `parsing_rse_job_adverts` directory) are as follows:
* `db/`
* `data_products/`
* `scraped_jobs/`
* `tarred_jobs/`

Then open `settings.py` to check or modify the settings for the pipeline.  In particular, make sure that `DEFAULT_DATASTORES` and `TEST_DATASTORES` are populated with lists of paths to folders that you wish to poll for job adverts.  If you intend to scrape new jobs as part of running the pipeline, make sure the final entry of this list is `SCRAPE_DATASTORE`.

# Running the Pipeline

- To be completed

To run the pipeline, simply run `python pipeline.py`.  This will run the pipeline script, which performs a number of steps:
1. Scrape data
2. Create tar files
3. Extract data from htmls
4. Create database
5. Perform aux analysis

# What's what

- To be completed
