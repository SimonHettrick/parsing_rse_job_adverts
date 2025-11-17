import pandas as pd
import sqlite3

import settings

def enhance_location_data(df, api=False):

    # Set up places db if it doesnt exist, else load it as a df

    conn = sqlite3.connect(settings.DB_LOCATION)

    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS places (
            id INTEGER PRIMARY KEY,
            location_string TEXT, 
            city TEXT,
            region TEXT,
            country TEXT,
            latitude FLOAT,
            longitude FLOAT
        );
    """)
    conn.commit()

    places = pd.DataFrame(conn.execute("SELECT * FROM places").fetchall())

    def get_loc_data(row):
        #print(row['location_string'])
        return None

    df.apply(get_loc_data, axis=1)

    df['latitude']=None ###
    df['longitude']=None ###

    return df
