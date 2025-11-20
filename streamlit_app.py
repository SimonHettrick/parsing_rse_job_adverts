'''Streamlit app to provide a user-friendly frontend to access the dataset and create basic
data products'''

import sqlite3

import numpy as np
import pandas as pd
import streamlit as st

import settings
from frontend import components, translators

# === Set up Streamlit layout and header ===

# Basic page configuration
st.set_page_config(page_title='RSE Job Tracker', layout='wide', initial_sidebar_state='auto')

# Manually override internal streamlit function to parse pandas dframes for array;
#  the included function is cell-wise and hence extremely slow.
st.elements.lib.pandas_styler_utils._use_display_values = lambda df, style: df.astype(str)

# Title text
image_column, header_column = st.columns([1, 20], gap='small')

with header_column:
    st.header('RSE Job Tracker')

st.divider()

# === Pre-run Setup ===

@st.cache_resource
def extract_data():
    '''Extract the dataset from the db, perform some light processing and cache the result'''

    with sqlite3.connect(settings.DB_LOCATION) as conn:

        df = pd.read_sql_query("SELECT * FROM jobs", conn)

    # Drop unnecessary columns
    df = df.drop(['filename', 'source'], axis=1)

    # Fix column types
    df['placed_on'] = pd.to_datetime(df['placed_on'], format='mixed', errors='ignore')
    df['closes_on'] = pd.to_datetime(df['closes_on'], format='mixed', errors='ignore')

    # Annotate the db
    def get_year(date):
        try:
            return date.year
        except AttributeError:
            return None

    df['year'] = df['placed_on'].apply(get_year)

    return df

db = extract_data()

# === Set up page layout ===

#help_col_1, help_col_2 = st.columns(2, gap='medium')
filter_area = st.container()

# === Prepare input area ===

# Set up column containers for user input dropdowns
col_data, col_view = st.columns(2)

# Setting the maximum number of filters the user can request
MAX_FILTERS = 5

# Collect how many filters the user wants to apply
with col_data:
    n_filters = st.selectbox('Number of Filters', range(MAX_FILTERS+1))

# Skip the filter section entirely if 0 filters are requested
if n_filters > 0:

    with filter_area:

        # Set up subcolumns for filter dropdowns
        subcol_filterfield, subcol_filtertype, subcol_filtervalue = st.columns(3)
        st.divider()

    # For each filter requested:
    for f_key in range(n_filters):

        # Set a flag to ensure only the top filter has visible labels for its inputs
        l_vis = 'visible' if f_key == 0 else 'collapsed'

        # Collect the list of filterable fields by removing bad filter fields based on dtype

        filt_fields = [str(c) for c in db.columns]
        filt_fields = list(filter(
            lambda c : c not in translators.bad_filters,
            filt_fields
        ))
        filt_fields = [translators.id_to_human[c] for c in filt_fields]


        filt_fields.sort()

        with subcol_filterfield:

            # Collect user's selection of filter field
            filter_field = translators.human_to_id[st.selectbox(
                'Filter Field:',
                ['--None--']+filt_fields,
                label_visibility=l_vis,
                key=('f', f_key)
                # ^ generate unique keys for this and all widgets, required by streamlit backend
            )]

        if filter_field is None:

            # If no field selected, display non-interactable placeholder dropdowns
            subcol_filtertype.selectbox(
                'Filter Type:',
                [],
                label_visibility=l_vis, key=('t', f_key)
            )
            subcol_filtervalue.selectbox(
                'Filter Value:',
                [],
                label_visibility=l_vis, key=('v', f_key)
            )

        else:

            db = db.dropna(subset=[filter_field])
            # Collect the type of filter to perform
            filter_type = subcol_filtertype.selectbox('Filter Type:', [
                'Contains',
                'Does not Contain'
                ], label_visibility=l_vis, key=('t', f_key))

            # Deal with collecting input for 'contain' type filters

            filter_value = subcol_filtervalue.text_input(
                'Filter Value:',
                label_visibility=l_vis,
                key=('v', f_key)
            )

            # Check filter value isn't either blank or placeholder
            if filter_value not in ('--Select--', '', None):

                # Collect a boolean array for records with filter_fields equal to filter_value
                filter_bools = db[filter_field].str.contains(filter_value, case=False)

                # If 'not equal' mode active, negate the boolean field
                if 'Does not' in filter_type:
                    filter_bools = np.logical_not(filter_bools)

                # Filter the dataframe
                db = db.loc[filter_bools]

# === Plot / Display options: ===

# Create dropdown box for data project, collect user's input
with col_view:
    view = st.selectbox('Data Product:', [
        '--Select--',
        'General Stats',
        'Jobs per Year',
        'Jobs per Contract Type',
        'Title Word Cloud',
        'Description Word Cloud',
        'Organisation Word Cloud',
        'Density Map',
    ])

    components.download_button(db)


# === Plot creation: ===

if db.empty:
    st.markdown('*No data to plot!*')

# ==== Stats Page ====

elif view == 'General Stats':
    components.general_stats(db)

# ==== Description Word Cloud ====

elif view == 'Title Word Cloud':
    components.word_cloud(db, 'job_title_parsed')

# ==== Description Word Cloud ====

elif view == 'Description Word Cloud':
    components.word_cloud(db, 'description_parsed', wc_limit=100000)

# ==== Organisation Cloud ====

elif view == 'Organisation Word Cloud':
    components.word_cloud(db, 'organisation', stopwords=set(['university','of','the']))

# ==== Histogram by Year ====

elif view == 'Jobs per Year':
    components.histogram(db, 'year')

# ==== Histogram by Contract Type ====

elif view == 'Jobs per Contract Type':
    components.histogram(db, 'contract_type', groupother=True)

elif view == 'Density Map':
    components.density_map(db)
