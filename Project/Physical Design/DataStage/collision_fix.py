import pandas
import os

def main():
    ac_data_frame = pandas.read_excel(os.path.abspath("h2017collisionsfinal.xlsx"), header = 0)
    accident_tbl(ac_data_frame)
#accident flat file construction.    
def accident_tbl(ac_data_frame):
    accident_columns = ['accident_time', 'environment', 'road_surface', 'traffic_control', 'impact_type']
    accident_df = ac_data_frame[['Time', 'Environment', 'Road_Surface', 'Traffic_Control', 'Impact_type' ]].copy()
    accident_df.columns = accident_columns
    accident_df['accident_key'] = range(0, len(accident_df))
    print(accident_df)

if __name__ == "__main__":
    main()