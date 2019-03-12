import pandas
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


#Locally stored database initiation -> must be changed as per your setting.   
DB = "group_16"
DB_U = "data_admin_16"
DB_P = "password"


#engine = create_engine('postgresql://'+DB_U+ ':' + DB_P + '@localhost/5432/'+ DB)
engine = ("postgresql+psycopg2://data_admin_16:password@127.0.0.1:5432/group_16")
session = sessionmaker()
session.configure(bind=engine)

def main():
#database engine.    


#Read the excel files
    ac_data_frame = pandas.read_excel(os.path.abspath("h2017collisionsfinal.xlsx"), header = 0)
    accident_tbl(ac_data_frame)

#accident flat file construction.
def accident_tbl(ac_data_frame):
    accident_columns = ['accident_time', 'environment', 'road_surface', 'traffic_control', 'impact_type', 'visibility']
    accident_df = ac_data_frame[['Time', 'Environment', 'Road_Surface', 'Traffic_Control', 'Impact_type', 'Light' ]].copy()
    accident_df.columns = accident_columns
    accident_df['accident_key'] = range(0, len(accident_df))
    accident_df.to_sql("accidents", engine, index=False, if_exists='append')
    
#adding data onto Db
    
if __name__ == "__main__":
    main()