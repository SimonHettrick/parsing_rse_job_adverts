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

To run the pipeline, simply run `python pipeline.py`.  This will run the pipeline script, which performs a number of steps:
1. Scrapes the data: Fetches the raw html of job adverts hosted at the URL set in settings.  The html files are stored locally in the directory set as `SCRAPE_DATASTORE` in `settings.py`.
2. Creates tar files: fetches every html file stored in any of the directories listed in `DEFAULT_DATASTORES` in `settings.py`, and performs some basic analysis on the contents to determine which year the job was posted in.  Each job's html file is then compressed and added to a tar file containing all jobs released in the same year.  The tar files are in the directory set at `TARPATH` in `settings.py`, and will be created if they do not already exist.  Note that the original html files are NOT deleted by the pipeline in order to prevent mishaps causing loss of data, and these files should be tidied up manually.
3. Extract data from job files: the html files for each job are parsed to extract a number of useful parameters, such as job title, job location and salary range.  This step produces no output.
4. Create database: using the values parsed in stage 3, add the new jobs to a sqlite3 database of jobs (or create the database if it does not exist).  The database is stored at the path provided as `DB_LOCATION` in `settings.py`.  Also creates a db logfile, listing the fields that were collected for the job data, and how many jobs have valid data for each field.
5. Perform auxilliary analysis: runs some scripts on the contents of the database to perform some basic analysis on the data, creating a number of plots and auxilliary data products.  These auxilliary products are stored in the directory provided for `DATA_PRODUCTS` in `settings.py`.

When adding data to the tarfiles and database, the pipeline knows to ignore duplicates by filename; i.e., if a filename is already present in the database or a given tarfile, that job file will be skipped when running the pipeline.  If an identical job (by filename) exists in multiple of the directories given in `DEFAULT_DATASTORES`, then the pipeline will prioritise versions of the file stored in earlier directories and ignore the same file stored in later ones.

## Optional flags

For convenience, the pipeline can also be invoked with a number of optional flags which modify its behaviour, i.e.:
```
python pipeline.py --flag1 --flag2
```

Valid flags are listed here:
* `--from-db` : skips Steps 1-4 of the pipeline and instead loads the current data from the db to perform Step 5.
* `--no-scrape` : skips Step 1 of the pipeline.
* `--test` : modifies Step 2 of the pipeline, to search the directories listed in `TEST_DATASTORES` in `settings.py` instead of `DEFAULT_DATASTORES`.  This allows you to maintain a small set of data to use when testing the pipeline, as running on a full dataset can take multiple hours.

# Querying the Database

The database can be queried using any standard SQL interface of your choice, e.g. DB Browser for SQLite.  By default, all data collected by the pipeline will exist in the database in a table named `jobs`.  For convenience, a number of template SQL queries are described below:

- Create a table of how many jobs are listed for each country, excluding any jobs where country information could not be parsed.
```
SELECT
    country, COUNT(country)
FROM jobs
WHERE country NOTNULL
GROUP BY country
```

- Create a table of how many jobs in each year had a title containing the substring "echnician"
```
SELECT
    CAST(SUBSTR(placed_on, LENGTH(placed_on) - 4) AS integer) AS yr, COUNT(CAST(SUBSTR(placed_on, LENGTH(placed_on) - 4) AS integer))
FROM jobs
WHERE job_title LIKE '%echnician%' AND yr NOTNULL
GROUP BY yr
```

- Create a table of job listings from 2019 ordered by minimum salary
```
SELECT
    job_title, salary_min, salary_max, CAST(SUBSTR(placed_on, LENGTH(placed_on) - 4) AS integer) AS yr
FROM jobs
WHERE salary_min NOTNULL AND salary_max NOTNULL AND yr IS 2019
ORDER BY salary_min DESC
```

# What's what

- [```pipeline.py```](pipeline.py) - The complete pipeline, as detailed in this README.
- [```settings.py```](settings.py) - The settings file where you can adjust parameters such as the location of inputs and outputs to the pipeline.
- [```libs/find_jobs.py```](libs/find_jobs.py) - A library of data-cleaning and analysis functions to extract data products from the database
- [```libs/parse_csv.py```](libs/parse_csv.py) - A library of functions to parse the raw job advert html files in order to extract parameters of interest.
- [```libs/raw.py```](libs/raw.py) - A library of functions dealing with converting raw html files into tarfile and database entries.
- [libs/scrape_jobs.py] - A library of functions dealing with scraping online job information websites to produce locally-stored html files.

