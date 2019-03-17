import pandas
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
import datetime as dt
import numpy as np
import holidays


#Locally stored database initiation -> must be changed as per your setting.   
DB = "group_16"
DB_U = "data_admin_16"
DB_P = "password"
engine = ("postgresql+psycopg2://"+DB_U+":"+ DB_P + "@127.0.0.1:5432/"+ DB)
session = sessionmaker()
session.configure(bind=engine)


def main():    
   print("------------MAIN----------")
   '''
   sample_df = pandas.DataFrame()
   hour_df['current_date'] = pandas.date_range(start='1/1/2017', end='31/12/2017', freq='H')
   print("Hello", sample_df[:8])
   '''
   create_acc_df()
   create_hour_df()
   print("--------END MAIN----------")


def create_acc_df():
    ac_data_frame = pandas.read_csv(os.path.abspath("h2017collisionsfinal.csv"), header = 0, encoding = 'ISO-8859-1')
    ac_16_df = pandas.read_csv(os.path.abspath("2016collisionsfinal.csv"), header = 0, encoding = 'ISO-8859-1')
    ac_data_frame = ac_data_frame.append(ac_16_df, sort=False)
    ac_15_df = pandas.read_csv(os.path.abspath("2015collisionsfinal.csv"), header = 0, encoding = 'ISO-8859-1')
    ac_data_frame = ac_data_frame.append(ac_15_df, sort=False)
    ac_14_df = pandas.read_csv(os.path.abspath("2014collisionsfinal.csv"), header = 0, encoding = 'ISO-8859-1')
    ac_data_frame = ac_data_frame.append(ac_14_df, sort=False)

 #------------- create excel and instert into DB-------------   
    ac_data_frame.to_excel("full_collision_final.xlsx")
    insert_accident_tbl(ac_data_frame)
    insert_location_tbl(ac_data_frame)
#------------- create excel and insert into DB-------------    


#accident flat file construction
def insert_accident_tbl(ac_data_frame):
    accident_columns = ['accident_time', 'environment', 'road_surface', 'traffic_control', 'impact_type', 'visibility']
    accident_df = ac_data_frame[['Time', 'Environment', 'Road_Surface', 'Traffic_Control', 'Impact_type', 'Light' ]].copy()
    accident_df.columns = accident_columns
    #Round up to nearest hour
    accident_df['accident_time'] =  pandas.to_datetime(accident_df['accident_time']).apply(lambda x: x.round("H"))
    accident_df['accident_time'] =  pandas.to_datetime(accident_df['accident_time']).apply(lambda x: x.strftime('%H:%M:%S'))
    #key build up
    accident_df['accident_key'] = range(0, len(accident_df))
    print('Accident Processed (Rows, Cols) : ', accident_df.shape) 
    accident_df.to_sql("accidents", engine, index=False, if_exists='replace') #replace or append or fail
    print('Accident Table added to DB')
    accident_df.to_excel('accident_final.xlsx')


#adding data onto Db
def insert_location_tbl(ac_data_frame):
    location_columns = ['street_name', 'intersection_1', 'intersection_2', 'longitude', 'latitude', 'neighborhood']
    location_df = ac_data_frame[['Location', 'Location', 'Location', 'lon', 'lon', 'Neighborhood' ]].copy()
    location_df.columns = location_columns
    location_df['location_key'] = range(0, len(location_df))
    print('Location Processed (Rows, Cols) : ', location_df.shape)
    location_df.to_sql("locales", engine, index=False, if_exists='replace')
    print('Location Table added to DB')
    location_df.to_excel("location_final.xlsx")


def create_hour_df():
    hour_df = pandas.DataFrame()
    hour_df['currentdate'] = pandas.date_range(start='1/1/2014', end='31/12/2017', freq='H')
    hour_df['day_of_week'] = hour_df['currentdate'].dt.day_name()  
    hour_df['month_of_year'] = hour_df['currentdate'].dt.month_name()
    hour_df['is_weekend'] = ((pandas.DatetimeIndex(hour_df['currentdate']).dayofweek) // 5 == 1)
    #HOLIDAY FLAG
    # creating a blank series 
    Type_new_hol_flags = pandas.Series([])
    Type_new_hol_name = pandas.Series([])   
    #Canadian holidays. 
    ca_hols = holidays.CA(years =(2014, 2015, 2016, 2017))
    # holiday count 
    count = 0 
    # running a for loop and assigning some values to series
    for index, row in hour_df.iterrows():
        hol_flag = getattr(row, "currentdate") in ca_hols
        if hol_flag:
            Type_new_hol_flags[index]=hol_flag
            Type_new_hol_name[index] = ca_hols.get(getattr(row, "currentdate"))
        else:
            Type_new_hol_flags[index]=hol_flag
            Type_new_hol_name[index] = 'Not a holiday'
    hour_df['current_year'] = pandas.to_datetime(hour_df['currentdate']).apply(lambda x: x.strftime('%Y'))
    # inserting new column with values of holiday flag list made above         
    hour_df.insert(5, "is_holiday", Type_new_hol_flags)
    # inserting new column with values of holiday name list made above         
    hour_df.insert(6, "holiday_name", Type_new_hol_name)
    #hour start
    hour_df['hour_start'] = hour_df['currentdate'].apply(lambda x: x.strftime('%H:%M:%S'))
    #hour end = hour start + 1hr
    hour_df['hour_end'] = hour_df['currentdate']  + timedelta(hours=1)
    hour_df['hour_end'] = pandas.to_datetime(hour_df['hour_end']).apply(lambda x: x.strftime('%H:%M:%S'))
    #clean date
    hour_df['currentdate'] = pandas.to_datetime(hour_df['currentdate']).apply(lambda x: x.strftime('%Y-%m-%d'))
    #hour key build based on numbe of rows. 
    hour_df['hour_key'] = range(0, len(hour_df))  
    print('Hour Table Processed (Rows, Cols) : ',  hour_df.shape)
    hour_df.to_sql("hours", engine, index=False, if_exists='replace') #replace or append or fail
    print('Hour Table added to DB')
    hour_df.to_excel("hour_final.xlsx")

if __name__ == "__main__":
    main()
