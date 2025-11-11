# Background

Bit of a complex background to this one. I set it up to look at RSE jobs from the jobs.ac.uk data, then expanded it to look at any type of jobs for some policy work I was doing for the Hidden REF. It's now mainly used for RSE jobs again.

# What's what

The job parser consists of two main components:

- [```pipeline.py```](pipeline.py) - The complete Python pipeline to scrape job adverts, process them, extract data and populate a `Sqlite` database.
- [```streamlit_app.py```](streamlit_app.py) - The Streamlit-based front-end; a browser app to perform as a human-friendly interface to interact with the database.

The behaviour of these components can be modified by editing the following settings files:

- [```settings.py```](settings.py) - The settings file where you can adjust parameters such as the location of inputs and outputs to the pipeline, and page layout options for the Streamlit app.

The Job Parser also consists of the following libraries:
- [```frontend/components.py```](frontend/components.py) - A library of components, plots and widgets which make up the Streamlit browser app.
- [```frontend/translators.py```](frontend/translators.py) - A collection of lists and dictionaries to deal with translating between backend ids for various components and front-end human-readable labels.
- [```libs/parse_csv.py```](libs/parse_csv.py) - A library of functions to parse the raw job advert html files in order to extract parameters of interest.
- [```libs/raw.py```](libs/raw.py) - A library of functions dealing with converting raw html files into tarfile and database entries.
- [```libs/scrape_jobs.py```](libs/scrape_jobs.py) - A library of functions dealing with scraping online job information websites to produce locally-stored html files.

# Setting Up

To set up the RSE job parser, first create and activate a Python virtual environment and install `requirements.txt`.

In order to store the various data products created by the pipeline, you will need to set up a number of directories in the project root directory.  The names and locations of these directories can be changed in `settings.py`, but the recommended directories to set up (all under the `parsing_rse_job_adverts` directory) are as follows:
* `db/`
* `scraped_jobs/`
* `tarred_jobs/`

Then open `settings.py` to check or modify the settings for the pipeline and front end.  In particular, make sure that `DEFAULT_DATASTORES` and `TEST_DATASTORES` are populated with lists of paths to folders that you wish to poll for job adverts.  If you intend to scrape new jobs as part of running the pipeline, make sure the final entry of this list is `SCRAPE_DATASTORE`.

# Running the Pipeline

To run the pipeline, simply run `python pipeline.py`.  This will run the pipeline script, which performs a number of steps:
1. Scrapes the data: Fetches the raw html of job adverts hosted at the URL set in settings.  The html files are stored locally in the directory set as `SCRAPE_DATASTORE` in `settings.py`.
2. Creates tar files: fetches every html file stored in any of the directories listed in `DEFAULT_DATASTORES` in `settings.py`, and performs some basic analysis on the contents to determine which year the job was posted in.  Each job's html file is then compressed and added to a tar file containing all jobs released in the same year.  The tar files are in the directory set at `TARPATH` in `settings.py`, and will be created if they do not already exist.  Note that the original html files are NOT deleted by the pipeline in order to prevent mishaps causing loss of data, and these files should be tidied up manually.
3. Extract data from job files: the html files for each job are parsed to extract a number of useful parameters, such as job title, job location and salary range.  This step produces no output.
4. Create database: using the values parsed in stage 3, add the new jobs to a sqlite3 database of jobs (or create the database if it does not exist).  The database is stored at the path provided as `DB_LOCATION` in `settings.py`.  Also creates a db logfile, listing the fields that were collected for the job data, and how many jobs have valid data for each field.

When adding data to the tarfiles and database, the pipeline knows to ignore duplicates by filename; i.e., if a filename is already present in the database or a given tarfile, that job file will be skipped when running the pipeline.  If an identical job (by filename) exists in multiple of the directories given in `DEFAULT_DATASTORES`, then the pipeline will prioritise versions of the file stored in earlier directories and ignore the same file stored in later ones.

## Optional flags

For convenience, the pipeline can also be invoked with a number of optional flags which modify its behaviour, i.e.:
```
python pipeline.py --flag1 --flag2
```

Valid flags are listed here:
* `--no-scrape` : skips Step 1 of the pipeline.
* `--test` : modifies Step 2 of the pipeline, to search the directories listed in `TEST_DATASTORES` in `settings.py` instead of `DEFAULT_DATASTORES`.  This allows you to maintain a small set of data to use when testing the pipeline, as running on a full dataset can take multiple hours.

# Using the Frontend

To run the Streamlit Frontend, simply run `streamlit run streamlit_app.py`.  This will run the server and, if possible, automatically open a web browser for you to interact with it.  By default, the Streamlit Server will be running at `localhost:8501`.

Using the Frontend consists of a number of steps:

1. Load the data: the Streamlit Frontend will automatically load and perform some pre-processing on the dataset when a running instance of it is accessed for the first time.  For large datasets this may take a few minutes, but the result of this is cached to ensure that further interaction with the app will not involve repeating this time-consuming step.  If you, however, wish to force a re-load of the data (if, e.g., the database has been updated while running the server), click the ellipsis icon in the top-right of the page and select 'clear cache'.
2. Select your filters: first choose the number of filters you wish to apply to the dataset (by default the maximum number of filters is 5 but this can be modified in [`settings.py`](settings.py)).  Construct the filters by selecting the parameter you wish to filter on (e.g. country, job title), the filter you wish to apply (e.g. 'contains substring', 'does not contain substring') and the parameter for the filter (e.g. the substring to be searched for).  Filters are ANDed together, so the resultant dataset will consist only of records which pass ALL filters supplied.
3. Select the data product you wish to create, or click the Download Data button to download a csv of the data given the currently applied filters.

# Querying the Database

The database can also be queried using any standard SQL interface of your choice, e.g. DB Browser for SQLite.  By default, all data collected by the pipeline will exist in the database in a table named `jobs`.  For convenience, a number of template SQL queries are described below:

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

