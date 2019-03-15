import pandas
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


#Locally stored database initiation -> must be changed as per your setting.   
DB = "group_16"
DB_U = "data_admin_16"
DB_P = "password"
engine = ("postgresql+psycopg2://"+DB_U+":"+ DB_P + "@127.0.0.1:5432/"+ DB)
session = sessionmaker()
session.configure(bind=engine)

def main():
    
#Read the excel files

def create_acc_df():
    ac_data_frame = pandas.read_csv(os.path.abspath("h2017collisionsfinal.csv"), header = 0, encoding = 'ISO-8859-1')
    ac_16_df = pandas.read_csv(os.path.abspath("2016collisionsfinal.csv"), header = 0, encoding = 'ISO-8859-1')
    ac_data_frame = ac_data_frame.append(ac_16_df, sort=False)
    ac_15_df = pandas.read_csv(os.path.abspath("2015collisionsfinal.csv"), header = 0, encoding = 'ISO-8859-1')
    ac_data_frame = ac_data_frame.append(ac_15_df, sort=False)
    ac_14_df = pandas.read_csv(os.path.abspath("2014collisionsfinal.csv"), header = 0, encoding = 'ISO-8859-1')
    ac_data_frame = ac_data_frame.append(ac_14_df, sort=False)

 #------------- create excel and instert into DB-------------   
    #ac_data_frame.to_excel("collision_final.xlsx")
    insert_accident_tbl(ac_data_frame)
    #insert_location_tbl(ac_data_frame)
#------------- create excel and instert into DB-------------  
    
#accident flat file construction and adding data onto Db
def insert_accident_tbl(ac_data_frame):
    accident_columns = ['accident_time', 'environment', 'road_surface', 'traffic_control', 'impact_type', 'visibility']
    accident_df = ac_data_frame[['Time', 'Environment', 'Road_Surface', 'Traffic_Control', 'Impact_type', 'Light' ]].copy()
    accident_df.columns = accident_columns
    #Round up to nearest hour
    accident_df['accident_time'] =  pandas.to_datetime(accident_df['accident_time']).apply(lambda x: x.round("H"))
    accident_df['accident_time'] =  pandas.to_datetime(accident_df['accident_time']).apply(lambda x: x.strftime('%H:%M:%S'))
    #key build up
    accident_df['accident_key'] = range(0, len(accident_df))
    accident_df.to_sql("accidents", engine, index=False, if_exists='append') #replace or append or fail


#accident flat file construction and adding data onto Db
def insert_location_tbl(ac_data_frame):
    location_columns = ['street_name', 'intersection_1', 'intersection_2', 'longitude', 'latitude', 'neighborhood']
    location_df = ac_data_frame[['Location', 'Location', 'Location', 'Longitude', 'Latitude', 'Neighborhood' ]].copy()
    location_df.columns = location_columns
    location_df['location_key'] = range(0, len(location_df))
    print(location_df.shape)
    location_df.to_sql("locales", engine, index=False, if_exists='append')
  

if __name__ == "__main__":
    main()
