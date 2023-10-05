# Job parsing

Bit of a complex background to this one. I set it up to look at RSE jobs from the
jobs.ac.uk data, then expanded it to look at any type of jobs for some policy work
I was doing for the Hidden REF. It's now mainly used for RSE jobs again.

#What's what

* `jobs_to_csv.py`: takes a folder full of jobs.ac.uk adverts (which are stored as html files), parses them (job title, location, type of role, etc.) and saves them as a csv (`1_processed_jobs_YYYY-MM-DD.csv`, where YYYY-MM-DD is the date at the time of processing) for later processing.  Call as `python jobs_to_csv.py /PATH_TO_JOBS_FOLDER/ /PATH_TO_RESULTS_FOLDER/`
* `dataset_merger.py`: takes two files produced by `jobs_to_csv.py` and merges them, making sure no jobs are duplicated in the resultant merged file `1_processed_merged_jobs_YYYY-MM-DD.csv`.  Call as `dataset_merger.py /PATH_TO_PROCESSED_JOBS_FILE_1.csv /PATH_TO_PROCESSED_JOBS_FILE_2.csv`.  Places the results in `./results`.
* `find_jobs.py`: loads the data from a `1_processed_jobs_YYYY-MM-DD.csv` or a `1_processed_merged_jobs_YYYY-MM-DD.csv` file and looks for a specific job title (which is set in the 'find_jobs' module), then does some basic additions to produce further datafiles and flots.  Call as `find_jobs.py /PATH_TO_PROCESSED_JOBS_FILE.csv`; if run without an argument, finds the most recent `processed_merged_jobs` file in `./results` or, failing that, the most recent `processed_jobs` file.  Results are places in `./results`.  Further datafiles created are:
  * `2_named_processed_jobs_YYYY-MM-DD.csv`: The data from `1_processed_jobs_YYYY-MM-DD.csv` with
  extra column(s) used to identify jobs of interest and with any job that lacks a title removed
  from the data
  * `3_identified_jobs_YYYY-MM-DD.csv`: a subset of `2_named_processed_jobs_YYYY-MM-DD.csv` which
  contains only the jobs with the job titles of interest
  * `4_summary_identified_jobs_YYYY-MM-DD.csv`: breakdowns the number of jobs and the number of
  jobs of interest over the period of interest

# Relation to SSI Outcome Indicators

This is only relevant to people in the SSI who are looking into the outcome indicators
we started collecting in 2019 or so.

* `4_summary_identified_jobs.csv` is renamed `2_2_1 number of rse jobs being advertised.csv`
and becomes Outcome Indicator 2.2.1. 
* `3_identified_jobs.csv` is used to generate `1_2_2 Number of UK institutions employing RSEs in positions with
RSE-specific job titles.csv` by pulling out the unique institutions in each year.
Currently this is done via a spreadsheet, so I need to automate the process.
