'''Functions and dictionaries to convert backend identifiers to human-readable labels,
and to define whitelists/blacklists of parameters to be included/ignored in various
settings.'''

# A list of columns which should not be used for filters in the app
bad_filters = [
    'city',
    'job_title_parsed',
    'closes_on',
    'contract_type',
    'description_word_count',
    'filename',
    'hours',
    'id',
    'job_ref',
    'placed_on',
    'region',
    'role',
    'salary_min',
    'salary_max',
    'location_string',
    'source',
    'description_parsed',
    'year',
]

# Dicts to translate from human-readable column names to backend column ids (and vice versa)
human_to_id = {
    '--None--':None,
    'Country':'country',
    'Description':'description',
    'Job Title':'job_title',
    'Organisation':'organisation',
}

id_to_human = {v : k for k, v in human_to_id.items()}
