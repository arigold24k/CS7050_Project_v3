import pandas as pd
import os
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori, association_rules
import configparser

basepath = os.path.dirname(__file__)


config =configparser.ConfigParser()
config.sections()
config.read(os.path.join(basepath, 'config.ini'))
config.sections()


def run_assoc_rule_mining(data: pd.DataFrame):
    # making dataframe into list
    data_list = data[['UC2_Literal', 'time_range', 'occur_day', 'Shift Occurence', 'neighborhood', 'Street','ibr_code', 'npu']].copy()
    data_list.dropna(inplace=True)
    data_list = data_list.astype(str)

    print(data_list)
    data_list = data_list.values.tolist()

    a = TransactionEncoder()
    data_list_encoded = a.fit(data_list).transform(data_list)
    data_list_df = pd.DataFrame(data_list_encoded, columns=a.columns_)
    data_list_df = data_list_df.replace(False, 0)
    data_list_df = data_list_df.replace(True, 1)

    item_set = apriori(data_list_df, min_support=0.05, use_colnames=True, verbose=1)
    print(item_set)
    item_set.to_csv('item_set_2020-2022.csv')

    data_association_rules = association_rules(item_set, metric="confidence", min_threshold=0.9)
    print(data_association_rules)
    data_association_rules.to_csv('association_rule_mining_2020-2022.csv')



def main():
    basepath = os.path.dirname(__file__)
    transformed_data = pd.read_csv(os.path.join(basepath, 'DataFiles', 'Transformed_data.csv'), skipinitialspace = True)

    pd.set_option('display.max_columns', 50)
    pd.set_option('display.width', 1500)

    run_assoc_rule_mining(data=transformed_data)


if __name__ == '__main__':
    main()
