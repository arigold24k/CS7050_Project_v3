import sys
from typing import Any
import pandas as pd
import os
from pandas.core.generic import NDFrame
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori, association_rules
import configparser
import re


def expl_data_analysis(data: pd.DataFrame):
    df_raw_data_dropna = data
    print(df_raw_data_dropna.describe())
    # df_raw_data_dropna.to_csv('cleaned_raw_data_1201.csv')
    # df_raw_data_dropna['occur_day'].fillna(pd.to_datetime(df_raw_data_dropna['occur_date'].astype(str), format='%m/%d/%Y').strftime('%A'), inplace=True)
    print(df_raw_data_dropna)

    # exploratory data analysis / Data mining

    # By Crime Type
    total_crimes = df_raw_data_dropna['UC2_Literal'].count()
    print(f'Total Incidents: {total_crimes}')
    df_crime_type = df_raw_data_dropna.groupby(by='UC2_Literal').size().reset_index()
    df_crime_type.columns = ['UC2_Literal', 'Total']
    df_crime_type['Percentage'] = df_crime_type['Total'] / total_crimes * 100
    print(df_crime_type)
    df_crime_type.to_csv('by_crime_type.csv')

    #     Broken down by day
    df_count_by_day: NDFrame | Any = df_raw_data_dropna.groupby(by=['UC2_Literal', 'occur_day']).size().reset_index()
    df_count_by_day.columns = ['UC2_Literal', 'occur_day', 'Total']
    print(df_count_by_day)
    df_count_by_day.to_csv('Output/crime_by_day.csv')

    #      Broken down by time
    df_time_count_temp = pd.DataFrame()
    df_time_count_temp = df_raw_data_dropna.copy()

    df_time_count = df_time_count_temp.groupby(by=['UC2_Literal', 'occur_time_hour']).size().reset_index()
    df_time_count.columns = ['UC2_Literal', 'occur_time_hour', 'Total']
    df_time_count = df_time_count.groupby(['UC2_Literal', pd.cut(pd.to_numeric(df_time_count['occur_time_hour']),
                                                                 bins=[0, 3, 6, 9, 12, 15, 18, 21, 24],
                                                                 labels=['0-3', '3-6', '6-9', '9-12', '12-15', '15-18',
                                                                         '18-21', '21-24'])])[
        'Total'].sum().reset_index()
    print(df_time_count)
    df_time_count.to_csv('crime_by_time.csv')
    #      Broken down by Zip
    df_time_count_temp = pd.DataFrame()
    df_time_zip = df_raw_data_dropna.groupby(by=['UC2_Literal', 'neighborhood']).size().reset_index()

    df_time_zip.columns = ['UC2_Literal', 'neighborhood', 'Total']
    # df_time_count = df_time_count.groupby(['UC2_Literal', pd.cut(pd.to_numeric(df_time_count['occur_time_hour']), bins=[0,3,6,9,12,15,18,21,24], labels=['0-3', '3-6', '6-9', '9-12', '12-15', '15-18', '18-21', '21-24'])])['Total'].sum().reset_index()
    print(df_time_zip)
    df_time_zip.to_csv('crime_by_neighborhood.csv')



def main():
    basepath = os.path.dirname(__file__)
    transformed_data = pd.read_csv(os.path.join(basepath, 'DataFiles', 'Transformed_data.csv'), skipinitialspace = True)

    pd.set_option('display.max_columns', 50)
    pd.set_option('display.width', 1500)

    # run exploratory data analysis
    expl_data_analysis(data=transformed_data)


if __name__ == '__main__':
    main()
