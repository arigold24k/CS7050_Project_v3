import pandas as pd
import os
import configparser
import re

basepath = os.path.dirname(__file__)


config =configparser.ConfigParser()
config.sections()
config.read(os.path.join(basepath, 'config.ini'))
config.sections()

api_key = config['GOOGLE_KEY']['key']

def strip_street_from_location(x:str):

    x = x[:x.find('\n')]
    remove_house_number = re.sub(r'^\s?\d+\s+', '',x)
    remove_house_number = remove_house_number.strip()

    if re.search(r'^-', remove_house_number) or re.search(r'^\\', remove_house_number) or re.search(r'^/', remove_house_number):
        remove_house_number = remove_house_number[1:]
    return remove_house_number.strip()


def transform_data(data: pd.DataFrame) -> pd.DataFrame:
    # df_raw_data_dropna = data.dropna(subset=['offense_id'])
    df_raw_data_dropna = data.copy()
    df_raw_data_dropna.drop_duplicates(subset=['offense_id'], keep='first', inplace=True)
    df_raw_data_dropna['occur_date'] = pd.to_datetime(df_raw_data_dropna['occur_date'], errors='coerce')
    df_raw_data_dropna.dropna(subset=['occur_date'])
    df_raw_data_dropna.occur_day_num.fillna(df_raw_data_dropna.occur_date.dt.dayofweek, inplace=True)
    df_raw_data_dropna.occur_day.fillna(df_raw_data_dropna.occur_date.dt.day_name(), inplace=True)
    df_raw_data_dropna.dropna(subset=['neighborhood'], inplace=True)
    df_raw_data_dropna.dropna(subset=['occur_day_num'], inplace=True)
    df_raw_data_dropna['occur_time_hour'] = df_raw_data_dropna.occur_time.map(lambda x: str(x)[:-2] if len(str(x)) > 2 else str(x))
    df_raw_data_dropna['occur_time_hour'].replace(':', '', inplace=True, regex=True)
    df_raw_data_dropna = df_raw_data_dropna[df_raw_data_dropna.occur_time_hour != 'T']
    df_raw_data_dropna = df_raw_data_dropna[pd.to_numeric(df_raw_data_dropna.occur_time_hour) <= 23]
    df_raw_data_dropna['time_range'] = pd.cut(pd.to_numeric(df_raw_data_dropna.occur_time_hour)
                                              ,bins=[0, 4, 8, 12, 16, 20, 24]
                                              ,labels=['0-4', '4-8', '8-12', '12-16', '16-20', '20-24']
                                              ,right=False)
    df_raw_data_dropna.drop(df_raw_data_dropna[df_raw_data_dropna['location']  == 'Unknown'].index, axis=0, inplace=True)
    df_raw_data_dropna['Street'] = df_raw_data_dropna.location.map(lambda x: strip_street_from_location(str(x)))
    df_raw_data_dropna['Street'] = df_raw_data_dropna['Street'].str.replace('STREE', 'ST', regex=True, case=False)
    df_raw_data_dropna['Street'] = df_raw_data_dropna['Street'].str.replace('STREET', 'ST', regex=True, case=False)
    df_raw_data_dropna['Street'] = df_raw_data_dropna['Street'].str.replace('st', 'ST', regex=True, case=False)
    df_raw_data_dropna['Street'] = df_raw_data_dropna['Street'].str.replace(r's$', 'ST', regex=True, case=False)
    df_raw_data_dropna['Street'] = df_raw_data_dropna['Street'].str.replace('AV', 'AVE', regex=True, case=False)
    df_raw_data_dropna['Street'] = df_raw_data_dropna['Street'].str.replace('AVEE', 'AVE', regex=True, case=False)
    df_raw_data_dropna['Street'] = df_raw_data_dropna['Street'].str.replace('AVENUE', 'AVE', regex=True, case=False)
    # for equal distribution of the shift occurrence
    bins = [0, 4, 12, 20, 24]
    labels = ['Evening Watch', 'Morning Watch', 'Day Watch', 'Evening Watch']
    df_raw_data_dropna.drop(['Shift Occurence'], axis=1, inplace=True)
    df_raw_data_dropna['Shift Occurence'] = pd.cut(pd.to_numeric(df_raw_data_dropna.occur_time_hour), bins=bins, labels=labels, right=False, ordered=False)


    df_raw_data_dropna['Location Type'].fillna('Unknown', inplace=True)

    return df_raw_data_dropna

def main():
    basepath = os.path.dirname(__file__)
    file_list = ['COBRA-2022.csv', 'COBRA-2021.csv', 'COBRA-2020(NEW RMS 9-30 12-31).csv',
                 'COBRA-2009-2019 (Updated 1_9_2020)/COBRA-2009-2019.csv']


    # ETL process, data clean up
    data = []
    for file in file_list:
        data.append(pd.read_csv(os.path.join(basepath, 'DataFiles', file),skipinitialspace = True))

    pd.set_option('display.max_columns', 50)
    pd.set_option('display.width', 1500)
    df_raw_data = pd.concat(data)

    # run exploratory data analysis

    df_data_transformed = transform_data(df_raw_data)
    df_data_transformed.to_csv('Transformed_data.csv')


if __name__ == '__main__':
    main()