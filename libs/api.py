#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Library of functions to deal with communicating with external API services
"""

import sqlite3

import pandas as pd

import settings

def enhance_location_data(df, use_api=False):
    '''A function to attempt to enhance the location data of each job advert.  Performs the 
    following steps:

    1. Check if some enhanced data (city, region, country) already is present for the record.
        if so, create a location key from this data.  If not, use the raw 'location string' as
        the key.
    2. Check if information matching the location key already exists in the locally stored cache
        of location data.  If so, enrich the dataframe row with this data and proceed to the
        next row.
    3. If no matching information exists in the local location cache, fetch enriched location
        information from the Google Places API, using the location key as input.
    4. Add the new location information fetched from the Places API to the local location cache.
    5. Return the dataframe with additional enriched information.
    '''

    # Set up places db if it doesnt exist, else load it as a df

    conn = sqlite3.connect(settings.DB_LOCATION)

    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS places (
            id INTEGER PRIMARY KEY,
            location_key TEXT, 
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
        # The get_loc_data function, which parses location data, checks if information on that
        # location exists locally and, of not, calls the Places API to fetch info

        # Check if the location already has some enriched data
        location_key = ', '.join([k for k in row[['city','region','country']] if k is not None])
        if location_key == '':
            location_key = row['location_string']

        # Check if place already parsed in the db and, if so, return stored values to avoid
        # unnecessary API calls
        if not(places.empty) and (location_key in places['location_key']):
            stored_loc = places[places['location_key']==location_key]
            outputs = (
                stored_loc['city'].values[0],
                stored_loc['region'].values[0],
                stored_loc['country'].values[0],
                stored_loc['latitude'].values[0],
                stored_loc['longitude'].values[0],
            )
            return outputs

        # If no stored data and API disabled, do nothing
        if not use_api:
            outputs = (
                row['city'],
                row['region'],
                row['country'],
                None,
                None,
            )
            return outputs

        # API calling as a last resort
        raise NotImplementedError

    df[[
        'city',
        'region',
        'country',
        'latitude',
        'longitude',
    ]]=df.apply(get_loc_data, axis=1, result_type='expand')

    return df
