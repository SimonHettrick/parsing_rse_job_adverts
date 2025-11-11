'''Definitions for components (e.g. plots, tables) for the frontend streamlit app'''

from wordcloud import WordCloud, STOPWORDS
import pandas as pd
import plotly.express as px
import streamlit as st

import settings

STOPWORDS.update(settings.ADDITIONAL_STOPWORDS)

def download_button(df, title='data'):
    '''A button which prepares and then downloads a modified version of the dataset, applyinh
    currently selected filters and removing sensitive information'''

    # Ignore description column and aux columns
    df = df.drop([
        'description',
        'description_parsed',
        'job_title_parsed',
        'year',
        'id',
        'location_string',
    ], axis=1)

    # Convert dataframe into csv string, convert this into bytestring
    df_bytes = bytes(df.to_csv(lineterminator='\n', index=False), encoding='utf-8')

    # Create download button, collect whether the user has clicked it
    downloaded = st.download_button('Download Data', df_bytes, file_name=title+'_data.csv')

    # Acknowledge download button click if the user has clicked it
    if downloaded:
        st.write('Thanks for downloading!')

def general_stats(df):
    '''A table to display various stats on the currently selected dataset'''

    stats_matrix = pd.DataFrame(
        {
            'Mean' : [
                df['salary_min'].mean(),
                df['salary_max'].mean(),
                df['description_word_count'].mean(),
            ],
            'Min' : [
                df['salary_min'].min(),
                df['salary_max'].min(),
                df['description_word_count'].min(),
            ],
            'Max' : [
                df['salary_min'].max(),
                df['salary_max'].max(),
                df['description_word_count'].max(),
            ],
        },
        index=[
            'Min salary',
            'Max salary',
            'Description Word Count',
        ]
    )

    st.table(stats_matrix)

def word_cloud(df, field, wc_limit=5000000, stopwords=None):
    '''Create a wordcloud of the most used words in a given field'''

    if stopwords is None:
        stopwords=STOPWORDS

    if len(df)>wc_limit:
        st.markdown(f'*Too much data to create word cloud! ({len(df)}/{wc_limit})*')

    else:
        df = df.dropna(subset=[field])

        counts = getattr(df,field).str.split().explode().value_counts()
        counts=counts.drop(labels=stopwords, errors='ignore')

        wc = WordCloud(width=2000, height=800, max_words=150, colormap="Dark2")
        img = wc.generate_from_frequencies(counts)
        fig = px.imshow(img)

        st.plotly_chart(fig)

def histogram(df, field, groupother=False):
    '''Create a histogram of value frequencies of a given frame in the df, grouping
    rare values into an 'other' column if requested'''

    df = df.dropna(subset=[field])
    counts = df[field].value_counts().sort_index()
    if groupother:
        cutoff=settings.HISTOGRAM_CUTOFF
        misc_counts = counts[counts<(counts.sum()*cutoff)]
        print(misc_counts.sum())
        counts = counts.drop(labels=misc_counts.keys(), errors='ignore')
        counts['other'] = misc_counts.sum()

        fig = px.bar(counts)

    else:
        fig = px.histogram(df, x=field)
    st.plotly_chart(fig)
