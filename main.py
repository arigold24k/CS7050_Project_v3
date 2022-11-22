from typing import Any

import pandas as pd
import os
from pandas.core.generic import NDFrame
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori, association_rules


def transform_data(data: pd.DataFrame) -> pd.DataFrame:
    df_raw_data_dropna = data.dropna(subset=['offense_id'])
    df_raw_data_dropna['occur_date'] = pd.to_datetime(df_raw_data_dropna['occur_date'], errors='coerce')
    df_raw_data_dropna.dropna(subset=['occur_date'])
    df_raw_data_dropna.occur_day_num.fillna(df_raw_data_dropna.occur_date.dt.dayofweek, inplace=True)
    df_raw_data_dropna.occur_day.fillna(df_raw_data_dropna.occur_date.dt.day_name(), inplace=True)
    df_raw_data_dropna.dropna(subset=['neighborhood'], inplace=True)

    return df_raw_data_dropna


def expl_data_analysis(data: pd.DataFrame):
    df_raw_data = data
    df_raw_data_dropna = transform_data(data=df_raw_data)
    # df_raw_data_dropna['occur_day'].fillna(pd.to_datetime(df_raw_data_dropna['occur_date'].astype(str), format='%m/%d/%Y').strftime('%A'), inplace=True)
    print(df_raw_data_dropna)

    #     exploratory data analysis / Data mining

    # By Crime Type
    total_crimes = df_raw_data_dropna['UC2_Literal'].count()
    print(f'Total Incidents: {total_crimes}')
    df_crime_type = df_raw_data_dropna.groupby(by='UC2_Literal').size().reset_index()
    # df_crime_type = df_crime_type.to_frame()
    # df_crime_type.T
    df_crime_type.columns = ['UC2_Literal', 'Total']
    df_crime_type['Percentage'] = df_crime_type['Total'] / total_crimes * 100
    print(df_crime_type)

    #     Broken down by day
    df_count_by_day: NDFrame | Any = df_raw_data_dropna.groupby(by=['UC2_Literal', 'occur_day']).size().reset_index()
    df_count_by_day.columns = ['UC2_Literal', 'occur_day', 'Total']
    print(df_count_by_day)

    #      Broken down by time
    df_time_count_temp = pd.DataFrame()
    df_time_count_temp = df_raw_data_dropna.copy()
    df_time_count_temp['occur_time_hour'] = df_time_count_temp.occur_time.str[:2]
    df_time_count_temp['occur_time_hour'].replace(':', '', inplace=True, regex=True)

    df_time_count = df_time_count_temp.groupby(by=['UC2_Literal', 'occur_time_hour']).size().reset_index()
    df_time_count.columns = ['UC2_Literal', 'occur_time_hour', 'Total']
    df_time_count = df_time_count.groupby(['UC2_Literal', pd.cut(pd.to_numeric(df_time_count['occur_time_hour']),
                                                                 bins=[0, 3, 6, 9, 12, 15, 18, 21, 24],
                                                                 labels=['0-3', '3-6', '6-9', '9-12', '12-15', '15-18',
                                                                         '18-21', '21-24'])])[
        'Total'].sum().reset_index()
    print(df_time_count)

    #      Broken down by Zip
    df_time_count_temp = pd.DataFrame()
    df_time_zip = df_raw_data_dropna.groupby(by=['UC2_Literal', 'neighborhood']).size().reset_index()

    df_time_zip.columns = ['UC2_Literal', 'neighborhood', 'Total']
    # df_time_count = df_time_count.groupby(['UC2_Literal', pd.cut(pd.to_numeric(df_time_count['occur_time_hour']), bins=[0,3,6,9,12,15,18,21,24], labels=['0-3', '3-6', '6-9', '9-12', '12-15', '15-18', '18-21', '21-24'])])['Total'].sum().reset_index()
    print(df_time_zip)


def run_assoc_rule_mining(data: pd.DataFrame):
    data_transformed = transform_data(data=data)
    data_transformed['occur_time_hour'] = data_transformed.occur_time.str[:2]
    data_transformed['occur_time_hour'].replace(':', '', inplace=True, regex=True)

    print(data_transformed.columns)

    # making dataframe into list
    data_list = data_transformed[['UC2_Literal', 'neighborhood', 'occur_time_hour', 'occur_day']].copy()
    # data_list.replace('nan', np.nan, inplace=True)
    data_list.dropna(inplace=True)
    data_list = data_list.astype(str)
    # data_list.replace('nan', np.nan, inplace=True)

    print(data_list)
    data_list = data_list.values.tolist()
    # print(data_list)

    a = TransactionEncoder()
    data_list_encoded = a.fit(data_list).transform(data_list)
    data_list_df = pd.DataFrame(data_list_encoded, columns=a.columns_)
    data_list_df = data_list_df.replace(False, 0)
    data_list_df = data_list_df.replace(True, 1)

    # print(data_list_df)

    item_set = apriori(data_list_df, min_support=0.03, use_colnames=True, verbose=1)
    print(item_set)

    data_association_rules = association_rules(item_set, metric="confidence", min_threshold=0.3)
    print(data_association_rules)


def main():
    basepath = os.path.dirname(__file__)
    file_list = ['COBRA-2022.csv', 'COBRA-2021.csv', 'COBRA-2020(NEW RMS 9-30 12-31).csv',
                 'COBRA-2009-2019 (Updated 1_9_2020)/COBRA-2009-2019.csv']

    file_list = ['COBRA-2022.csv', 'COBRA-2021.csv']

    # ETL process, data clean up
    data = []
    for file in file_list:
        data.append(pd.read_csv(os.path.join(basepath, 'DataFiles', file)))

    pd.set_option('display.max_columns', 50)
    pd.set_option('display.width', 1500)
    df_raw_data = pd.concat(data)

    # run exploratory data analysis
    expl_data_analysis(data=df_raw_data)

    run_assoc_rule_mining(data=df_raw_data)


if __name__ == '__main__':
    main()
