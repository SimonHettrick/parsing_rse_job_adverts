# === Scraper settings ===

SCRAPE_DATASTORE = './scraped_jobs/'
NUM_JOBS = 10000
BASE_URL = "http://www.jobs.ac.uk"
FULL_URL = f"{BASE_URL}/search/?keywords=*&sort=re&s=1&pageSize={NUM_JOBS}"


# === Local storage settings ===

DEFAULT_DATASTORES = ['./job_ads/JobsAcUk/', './job_ads/JOBS_RAW/', SCRAPE_DATASTORE]
TEST_DATASTORES = [SCRAPE_DATASTORE]
DB_LOCATION = './db/jobs.sqlite'
RESULTSPATH = './results/'


# === Analysis settings ===

jobs_of_interest = [
    'data scien',
    'data engineer',
    'data steward',
    'software develop',
    'software engineer',
    'research engineer',
    'bioinformatic',
    'knowledge exchange',
]

avoid_jobs = [ 'fellow', 'lecturer', 'student', 'tutor', 'profess']
