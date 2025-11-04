def download_button(in_df, title='data'):

    # Convert dataframe into csv string, convert this into bytestring
    df_bytes = bytes(in_df.to_csv(lineterminator='\n', index=False), encoding='utf-8')

    # Create download button, collect whether the user has clicked it
    downloaded = st.download_button('Download Data', df_bytes, file_name=title+'_data.csv')

    # Acknowledge download button click if the user has clicked it
    if downloaded:
        st.write('Thanks for downloading!')