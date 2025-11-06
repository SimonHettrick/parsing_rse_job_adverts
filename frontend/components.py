from wordcloud import WordCloud
import plotly.express as px
import streamlit as st

def download_button(df, title='data'):

    # Ignore description column and aux columns
    df = df.drop([
        'description',
        'description_parsed',
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
    pass

def word_cloud(df, field, wc_limit=100000):

    if len(df)>wc_limit:
        st.markdown(f'*Too much data to create word cloud! ({len(df)}/{wc_limit})*')

    else:
        df = df.dropna(subset=[field])

        counts = getattr(df,field).str.split().explode().value_counts()

        wc = WordCloud(width=2000, height=800, max_words=150, colormap="Dark2")
        img = wc.generate_from_frequencies(counts)
        fig = px.imshow(img)

        st.plotly_chart(fig)

def histogram(df, field):
    df = df.dropna(subset=[field])

    fig = px.histogram(df, x=field)
    st.plotly_chart(fig)