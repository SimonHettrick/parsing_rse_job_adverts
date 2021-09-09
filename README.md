# Job parsing

Bit of a complex background to this one. I set it up to look at RSE jobs from the
jobs.ac.uk data, then expanded it to look at any type of jobs for some policy work
I was doing for the Hidden REF. It's now mainly used for RSE jobs again.

#What's what

* `jobs_to_csv.py`: takes a folder full of jobs.ac.uk adverts (which are stored as html
files), parses them (job title, location, type of role, etc.) and saves them as a csv
  (`1_processed_jobs.csv`) for later processing
* `find_jobs.py`: loads the data from `1_processed_jobs.csv` and looks for a specific
job title (which is set in the 'find_jobs' module), then does some basic additions to
produce:
  * `2_named_processed_jobs.csv`: The data from `1_processed_jobs.csv` with extra
  column(s) used to identify jobs of interest and with any job that lacks a title
  removed from the data
  * `3_identified_jobs.csv`: a subset of `2_named_processed_jobs.csv` which contains
  only the jobs with the jobs of interest
  * `4_summary_identified_jobs.csv`: breakdowns the number of jobs and the number of
  jobs of interest over the period of interest

# Relation to SSI Outcome Indicators

This is only relevant to people in the SSI who are looking into the outcome indicators
we started collecting in 2019 or so.

* `4_summary_identified_jobs.csv` is renamed `2_2_1 number of rse jobs being advertised.csv`
and becomes Outcome Indicator 2.2.1. 
* `3_identified_jobs.csv` is used to generate `1_2_2 Number of UK institutions employing RSEs in positions with
RSE-specific job titles.csv` by pulling out the unique institutions in each year.
Currently this is done via a spreadsheet, so I need to automate the process.
