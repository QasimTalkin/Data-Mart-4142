import datetime as dt
import os
from datetime import timedelta
from pathlib import Path

import holidays
import pandas as pd
from sqlalchemy import create_engine

DB_HOST = 'localhost'
DB_PORT = 5432
DB = 'group_16'


def main():
    user = input('Enter pgadmin username: ')
    password = input('Enter pgadmin password: ')
    engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format(user, password, DB_HOST, DB_PORT, DB))

    print('----------HOUR----------')
    hour_df = create_hour_df()
    insert_hour(engine, hour_df)
    print('--------END HOUR--------')

    print('---------WEATHER--------')
    weather_df = create_weather_df()
    insert_weather(engine, weather_df)
    print('-------END WEATHER------')

    print('--------COLLISION-------')
    collision_df = create_collision_df()

    print('--------LOCATION--------')
    insert_location(engine, collision_df)
    print('------END LOCATION------')

    print('--------ACCIDENT--------')
    insert_accident(engine, collision_df)
    print('------END ACCIDENT------')
    print('------END COLLISION-----')

    print('----------FACT----------')
    insert_fact(engine)
    print('--------END FACT--------')


def create_weather_df():
    weather_csv = Path('weather_final.csv')
    if weather_csv.is_file():
        weather_df = pd.read_csv('weather_final.csv', dtype={'weather': str})
    else:
        # Filter relevant stations from the file (ON, hourly data still active 2014+)
        iter_stations = pd.read_csv('Station Inventory EN.csv', header=3, chunksize=1000)
        station_df = pd.concat([chunk[(chunk['Province'] == 'ONTARIO') & (chunk['HLY Last Year'] >= 2014)] for chunk in iter_stations], ignore_index=True)
        station_df = station_df.sort_values(by='Station ID').drop_duplicates('Name', keep='last')
        station_df = station_df[['Name', 'Latitude (Decimal Degrees)', 'Longitude (Decimal Degrees)']]  # Select columns
        station_df.columns = ['station_name', 'latitude', 'longitude']  # Rename columns
        station_names = station_df['station_name'].tolist()

        # Create one dataframe from all files
        weather_df = pd.DataFrame()
        files = ['ontario_1_1.csv', 'ontario_1_2.csv', 'ontario_2_1.csv', 'Ontario_2_2.csv', 'Ontario_3.csv', 'Ontario_4.csv']
        for f in files:
            print('Processing file: ' + f)
            # Get filtered dataframe for file
            reader = pd.read_csv(f, chunksize=1000)
            df = pd.concat([chunk[(chunk['Year'] >= 2014) & (chunk['X.U.FEFF..Station.Name.'].isin(station_names))] for chunk in reader])
            # Drop rows that have no weather data
            df.dropna(subset=df.columns[range(5, len(df.columns)-2)], how='all', inplace=True)
            # Combine final df with file df
            weather_df = pd.concat([weather_df, df], ignore_index=True)

        # Clean and finalize dataframe
        weather_df.drop(columns=list(weather_df.filter(regex='.Flag$')) + ['Year', 'Month', 'Day', 'Time', 'X.Province.'], inplace=True)
        weather_df.columns = [
            'date_time',
            'temp_c',
            'dew_point_temp_c',
            'rel_hum',
            'wind_dir_deg',
            'wind_spd_kmh',
            'visibility_km',
            'stn_press_kpa',
            'hmdx',
            'wind_chill',
            'weather',
            'station_name'
        ]
        weather_df = weather_df.merge(station_df, how='left')
        weather_df.sort_values(['station_name', 'date_time'], inplace=True)
        weather_df.to_csv('weather_final.csv', index=False)
    print('Weather Processed (Rows, Cols) : ', weather_df.shape)

    return weather_df


def insert_weather(engine, weather_df):
    connection = engine.connect()
    connection.execute('DROP TABLE IF EXISTS weather CASCADE')
    connection.execute('''CREATE TABLE weather(
        weather_key serial PRIMARY KEY,
        station_name text,
        longitude float8,
        latitude float8,
        temp_c float4,
        dew_point_temp_c float4,
        rel_hum int,
        wind_dir_deg int,
        wind_spd_kmh int,
        visibility_km float4,
        stn_press_kpa float4,
        hmdx int,
        wind_chill int,
        weather text,
        date_time timestamp,
        hour_id int)''')
    connection.close()
    weather_df.to_sql('weather', con=engine, if_exists='append', index=False)
    print('Weather table added to DB')

    connection = engine.connect()
    connection.execute('''UPDATE weather w
        SET hour_id = h.hour_key
        FROM hours h
        WHERE w.date_time::date = h.currentdate
        AND w.date_time::time = h.hour_start''')
    connection.close()
    print('Column hour_id updated in weather table')
    
    
def create_hour_df():
    hour_df = pd.DataFrame()
    hour_df['currentdate'] = pd.date_range(start='1/1/2014', end='01/01/2018', freq='H', closed='left')
    hour_df['day_of_week'] = hour_df['currentdate'].dt.day_name()  
    hour_df['month_of_year'] = hour_df['currentdate'].dt.month_name()
    hour_df['is_weekend'] = ((pd.DatetimeIndex(hour_df['currentdate']).dayofweek) // 5 == 1)
    #HOLIDAY FLAG
    # creating a blank series 
    Type_new_hol_flags = pd.Series([])
    Type_new_hol_name = pd.Series([])   
    #Canadian holidays. 
    ca_hols = holidays.CA(years =(2014, 2015, 2016, 2017))
    # running a for loop and assigning some values to series
    for index, row in hour_df.iterrows():
        hol_flag = getattr(row, "currentdate") in ca_hols
        if hol_flag:
            Type_new_hol_flags[index] = hol_flag
            Type_new_hol_name[index] = ca_hols.get(getattr(row, "currentdate"))
        else:
            Type_new_hol_flags[index] = hol_flag
            Type_new_hol_name[index] = None
    hour_df['current_year'] = pd.to_datetime(hour_df['currentdate']).apply(lambda x: x.strftime('%Y'))
    # inserting new column with values of holiday flag list made above         
    hour_df.insert(5, "is_holiday", Type_new_hol_flags)
    # inserting new column with values of holiday name list made above         
    hour_df.insert(6, "holiday_name", Type_new_hol_name)
    #hour start
    hour_df['hour_start'] = hour_df['currentdate'].apply(lambda x: x.strftime('%H:%M:%S'))
    #hour end = hour start + 1hr
    hour_df['hour_end'] = hour_df['currentdate']  + timedelta(hours=1)
    hour_df['hour_end'] = pd.to_datetime(hour_df['hour_end']).apply(lambda x: x.strftime('%H:%M:%S'))
    #clean date
    hour_df['currentdate'] = pd.to_datetime(hour_df['currentdate']).apply(lambda x: x.strftime('%Y-%m-%d'))
    hour_df.to_csv("hour_final.csv", index=False)
    print('Hour Table Processed (Rows, Cols) : ',  hour_df.shape)

    return hour_df


def insert_hour(engine, hour_df):
    connection = engine.connect()
    connection.execute('DROP TABLE IF EXISTS hours CASCADE')
    connection.execute('''CREATE TABLE hours(
        hour_key serial PRIMARY KEY,
        hour_start time,
        hour_end time,
        currentdate date,
        day_of_week text,
        month_of_year text,
        current_year int,
        is_weekend bool,
        is_holiday bool,
        holiday_name text)''')
    connection.close()
    hour_df.to_sql("hours", engine, index=False, if_exists='append') #replace or append or fail
    print('Hours table added to DB')


def create_collision_df():
    collision_df = pd.read_csv(os.path.abspath("h2017collisionsfinal.csv"), header = 0, encoding = 'ISO-8859-1')
    ac_16_df = pd.read_csv(os.path.abspath("2016collisionsfinal.csv"), header = 0, encoding = 'ISO-8859-1')
    collision_df = collision_df.append(ac_16_df, sort=False)
    ac_15_df = pd.read_csv(os.path.abspath("2015collisionsfinal.csv"), header = 0, encoding = 'ISO-8859-1')
    collision_df = collision_df.append(ac_15_df, sort=False)
    ac_14_df = pd.read_csv(os.path.abspath("2014collisionsfinal.csv"), header = 0, encoding = 'ISO-8859-1')
    collision_df = collision_df.append(ac_14_df, sort=False)

    # Create csv
    collision_df.to_csv("full_collision_final.csv", index=False)
    return collision_df


#accident flat file construction
def insert_accident(engine, collision_df):
    accident_columns = ['accident_time', 'environment', 'road_surface', 'traffic_control', 'impact_type', 'visibility', 'accident_date',
                        'street_name', 'intersection_1', 'intersection_2', 'longitude', 'latitude', 'neighborhood', 'is_fatal']
    accident_df = collision_df[['Time', 'Environment', 'Road_Surface', 'Traffic_Control', 'Impact_type', 'Light', 'Date',
                                'Location', 'Location', 'Location', 'lon', 'lat', 'Neighborhood', 'Collision_Classification']]
    accident_df.columns = accident_columns
    # Round to nearest hour
    accident_df.loc[:, 'adjusted_time'] =  pd.to_datetime(accident_df['accident_time']).apply(lambda x: x.round("H"))
    accident_df.loc[:, 'adjusted_time'] =  pd.to_datetime(accident_df['adjusted_time']).apply(lambda x: x.strftime('%H:%M:%S'))
    # Get location data for comparison
    accident_df.loc[:, 'street_name'] = accident_df['street_name'].apply(get_street_name)
    accident_df.loc[:, 'intersection_1'] = accident_df['intersection_1'].apply(get_intersection_1)
    accident_df.loc[:, 'intersection_2'] = accident_df['intersection_2'].apply(get_intersection_2)
    # Get is_fatal values
    accident_df.loc[:, 'is_fatal'] = accident_df['is_fatal'].apply(lambda x: True if '01' in x else False)
    accident_df.to_csv('accident_final.csv', index=False)
    print('Accident Processed (Rows, Cols) : ', accident_df.shape)
    connection = engine.connect()
    connection.execute('DROP TABLE IF EXISTS accidents CASCADE')
    connection.execute('''CREATE TABLE accidents(
        accident_key serial PRIMARY KEY,
        accident_time time,
        environment text,
        road_surface text,
        traffic_control text,
        visibility text,
        impact_type text,
        accident_date date,
        adjusted_time time,
        hour_id int,
        street_name text,
        intersection_1 text,
        intersection_2 text,
        longitude float8,
        latitude float8,
        neighborhood text,
        location_id int,
        is_fatal bool)''')
    connection.close()
    accident_df.to_sql("accidents", engine, index=False, if_exists='append') #replace or append or fail
    print('Accidents table added to DB')

    connection = engine.connect()
    connection.execute('''UPDATE accidents acc
        SET hour_id = h.hour_key
        FROM hours h
        WHERE acc.accident_date = h.currentdate
        AND acc.adjusted_time = h.hour_start''')
    print('Column hour_id updated in accidents table')
    connection.execute('''UPDATE accidents acc
        SET location_id = l.locale_key
        FROM locales l
        WHERE acc.street_name = l.street_name
        AND acc.intersection_1 IS NOT DISTINCT FROM l.intersection_1
        AND acc.intersection_2 IS NOT DISTINCT FROM l.intersection_2
        AND acc.longitude = l.longitude
        AND acc.latitude = l.latitude
        AND acc.neighborhood IS NOT DISTINCT FROM l.neighborhood''')
    print('Column location_id updated in accidents table')
    connection.close()


def insert_location(engine, collision_df):
    # Get relevant columns
    location_columns = ['street_name', 'intersection_1', 'intersection_2', 'longitude', 'latitude', 'neighborhood']
    location_df = collision_df[['Location', 'Location', 'Location', 'lon', 'lat', 'Neighborhood' ]]
    location_df.columns = location_columns
    # Separate location into street_name, intersection_1, intersection_2
    location_df.loc[:, 'street_name'] = location_df['street_name'].apply(get_street_name)
    location_df.loc[:, 'intersection_1'] = location_df['intersection_1'].apply(get_intersection_1)
    location_df.loc[:, 'intersection_2'] = location_df['intersection_2'].apply(get_intersection_2)
    # Remove duplicates
    location_df.drop_duplicates(inplace=True)
    location_df.to_csv("location_final.csv", index=False)
    print('Location Processed (Rows, Cols) : ', location_df.shape)
    # Drop and create locales table
    connection = engine.connect()
    connection.execute('DROP TABLE IF EXISTS locales CASCADE')
    connection.execute('''CREATE TABLE locales(
        locale_key serial PRIMARY KEY,
        street_name text,
        intersection_1 text,
        intersection_2 text,
        longitude float8,
        latitude float8,
        neighborhood text)''')
    connection.close()
    location_df.to_sql("locales", engine, index=False, if_exists='append')
    print('Location Table added to DB')


def get_street_name(location):
    if '@' in location:
        return location.split('@', 1)[1].strip()
    elif 'btwn' in location:
        return location.split('btwn', 1)[0].strip()
    else:
        return location.split('/')[0].strip()


def get_intersection_1(location):
    if '@' in location:
        return location.split('@', 1)[0].split('/', 1)[0].strip()
    elif 'btwn' in location:
        return location.split('btwn', 1)[1].split('&', 1)[0].strip()
    elif '/' in location:
        return location.split('/')[1].strip()
    else:
        return None


def get_intersection_2(location):
    if '@' in location:
        intersections = location.split('@', 1)[0].split('/', 1)
    elif 'btwn' in location:
        intersections = location.split('btwn', 1)[1].split('&', 1)
    elif '/' in location:
        intersections = location.split('/')[1:]
    else:
        intersections = []

    if len(intersections) >= 2:
        return intersections[1].strip()
    else:
        return None


def insert_fact(engine):
    connection = engine.connect()
    connection.execute('''CREATE TEMP TABLE distance AS
        SELECT s.accident_key, s.location_id, s.station_name, d.distance
        FROM (
            SELECT acc.accident_key, acc.location_id, w.station_name
            FROM accidents acc, weather w
            WHERE acc.hour_id = w.hour_id
        ) AS s
        JOIN (
            SELECT l.locale_key, w.station_name, (acos(sin(radians(l.latitude)) * sin(radians(w.latitude)) + cos(radians(l.latitude)) * cos(radians(w.latitude)) * cos(radians(w.longitude - l.longitude))) * 6371) AS distance
            FROM (
                SELECT DISTINCT ON (station_name) station_name, latitude, longitude
                FROM weather
            ) w, locales l
        ) AS d
        ON s.location_id = d.locale_key AND s.station_name = d.station_name''')
    print('Created temporary table for distances between accidents and stations')

    connection.execute('DROP TABLE IF EXISTS facts')
    connection.execute('''CREATE TABLE facts AS
        SELECT h.hour_key, l.locale_key, acc.accident_key, w.weather_key, acc.is_fatal, (l.intersection_2 IS NULL) AS is_intersection
        FROM hours h, accidents acc, locales l, weather w, (
            SELECT d2.accident_key, d2.station_name
            FROM (
                SELECT d.accident_key, min(d.distance) AS min_distance
                FROM distance d
                GROUP BY d.accident_key
            ) md
            JOIN distance d2 ON d2.accident_key = md.accident_key AND d2.distance = md.min_distance) near
        WHERE acc.hour_id = h.hour_key
        AND acc.location_id = l.locale_key
        AND h.hour_key = w.hour_id
        AND w.station_name = near.station_name
        AND near.accident_key = acc.accident_key''')
    connection.execute('''ALTER TABLE facts
        ADD CONSTRAINT hours_fkey FOREIGN KEY (hour_key) REFERENCES hours(hour_key),
        ADD CONSTRAINT locales_fkey FOREIGN KEY (locale_key) REFERENCES locales(locale_key),
        ADD CONSTRAINT accidents_fkey FOREIGN KEY (accident_key) REFERENCES accidents(accident_key),
        ADD CONSTRAINT weather_fkey FOREIGN KEY (weather_key) REFERENCES weather(weather_key)''')
    print('Fact Table added to DB')

    connection.execute('ALTER TABLE weather DROP COLUMN date_time, DROP COLUMN hour_id')
    connection.execute('DELETE FROM weather WHERE weather_key NOT IN (SELECT weather_key FROM facts)')
    connection.execute('''ALTER TABLE accidents DROP COLUMN accident_date, DROP COLUMN adjusted_time, DROP COLUMN hour_id,
        DROP COLUMN street_name, DROP COLUMN intersection_1, DROP COLUMN intersection_2, DROP COLUMN longitude,
        DROP COLUMN latitude, DROP COLUMN neighborhood, DROP COLUMN location_id, DROP COLUMN is_fatal''')
    print('Removed unneeded rows and columns')
    connection.close()


if __name__ == "__main__":
    main()
