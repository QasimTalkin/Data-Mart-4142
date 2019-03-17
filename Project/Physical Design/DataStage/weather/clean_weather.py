from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine


DB_HOST = 'localhost'
DB_PORT = 5432
DB = 'group_16'


def main():
    user = input('Enter pgadmin username: ')
    password = input('Enter pgadmin password: ')
    weather_csv = Path('weather.csv')
    if weather_csv.is_file():
        weather_df = pd.read_csv('weather.csv', dtype={'weather': str})
    else:
        weather_df = prepare_weather_data()
    insert_weather(weather_df, user, password)


def prepare_weather_data():
    # Filter relevant stations from the file (ON, hourly data still active 2014+)
    print('Getting station names...')
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
    weather_df.drop(columns=list(weather_df.filter(regex='.Flag$')) + ['X.Province.'], inplace=True)
    weather_df.columns = [
        'date_time',
        'year',
        'month',
        'day',
        'time',
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
    weather_df.to_csv('weather.csv', index=False)
    print('Created file: weather.csv')

    return weather_df


def insert_weather(weather_df, user, password):
    engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format(user, password, DB_HOST, DB_PORT, DB))
    print('Creating table...')
    connection = engine.connect()
    connection.execute('DROP TABLE IF EXISTS weather')
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
        weather text)''')
    connection.close()
    print('Inserting data...')
    # Data frame and csv currently have date_time, year, month, day, time
    # to compare with hour table (may change later)
    weather_df.iloc[:, 5:].to_sql('weather', con=engine, if_exists='append', index=False)


if __name__ == '__main__':
    main()
