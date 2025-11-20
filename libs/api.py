#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Library of functions to deal with communicating with external API services
"""

import sqlite3

import pandas as pd
import requests

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

    global places
    places = pd.DataFrame(conn.execute("SELECT * FROM places").fetchall())
    places.rename(columns={
        0:'id',
        1:'location_key',
        2:'city',
        3:'region',
        4:'country',
        5:'latitude',
        6:'longitude'
    }, inplace=True)

    def get_loc_data(row):
        # The get_loc_data function, which parses location data, checks if information on that
        # location exists locally and, of not, calls the Places API to fetch info

        # Use Places as global (bad practice, I know...) to allow it to modify as loops progress
        global places

        # Check if the location already has some enriched data
        location_key = ', '.join([k for k in row[['city','region','country']] if k is not None])
        if not location_key:
            if row['location_string']:
                location_key = row['location_string']
            else:
                # If there's still no data, fail safely and leave enriched columns as none
                return (None, None, None, None, None)

        location_key = location_key.lower().strip()

        # Check if place already parsed in the db and, if so, return stored values to avoid
        # unnecessary API calls
        if not(places.empty) and (location_key in places['location_key'].values):
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
        response = requests.get(settings.GOOGLE_GEOCODE_URL, params={
            'address': location_key,
            'key': settings.GOOGLE_PLACES_KEY,
        }, timeout=10)
        print(f'API called for address: {location_key}')

        if response.status_code != 200:
            # If API call fails to execute, just return default info
            outputs = (
                row['city'],
                row['region'],
                row['country'],
                None,
                None,
            )
            return outputs

        # Setup default info to return if no replacements found in the API call
        city=row['city']
        region=row['region']
        country=row['country']
        latitude=None
        longitude=None



        if not response.json()['results']:
            # If no results found, return the default info but remember this place to prevent
            # calling the API on it next time it comes up
            pass

        else:
            # Otherwise, take the first response to be the correct one
            found_place = response.json()['results'][0]

            # Scan address components for useful information, if present
            for component in found_place['address_components']:
                if 'locality' in component['types']:
                    city = component['long_name']
                elif 'administrative_area_level_1' in component['types']:
                    region = component['short_name']
                elif 'country' in component['types']:
                    country = component['long_name']

            latlon = found_place['geometry']['location']
            latitude = latlon['lat']
            longitude = latlon['lng']

        new_place = pd.DataFrame({
            'id': None,
            'location_key': [location_key],
            'city': [city],
            'region': [region],
            'country': [country],
            'latitude': [latitude],
            'longitude': [longitude]}
        )

        places=pd.concat([places,new_place])

        outputs = (
            city,
            region,
            country,
            latitude,
            longitude,
        )

        return outputs

    df[[
        'city',
        'region',
        'country',
        'latitude',
        'longitude',
    ]]=df.apply(get_loc_data, axis=1, result_type='expand')

    # Drop all values with an ID before pushing back to the DB (as these ones were already
    # processed by SQL and hence are already present in the db

    if not places.empty:
        places = places[places['id'].isna()]
        places.to_sql('places', conn, if_exists='append', index=False)
        conn.commit()

    return df
