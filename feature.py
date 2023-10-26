import multiprocessing
import sqlite3
import pandas as pd


def create_db_connection():
    return sqlite3.connect('mydatabase.db')


def calculate_average_txn_amount(data, acct_num):

    average_txn_amount = data.groupby('acct_num')['txn_amt'].mean().reset_index()
    print(average_txn_amount)
    return average_txn_amount


def calculate_variance(data,acct_num ):
    
    variance_data = data.groupby('acct_num')['txn_amt'].var().reset_index()
    variance_data.rename(columns={'txn_amt': 'txn_amt_variance'}, inplace=True)
    
    return variance_data



def calculate_time_diff(data):
    account_data = data.copy()
    account_data['txn_time'] = pd.to_datetime(account_data['txn_time'], format='%H:%M')
    

    account_data = account_data.sort_values(by=['txn_date', 'txn_time'])
    account_data['time_diff'] = account_data['txn_time'].diff().dt.total_seconds() / 60.0
    
    return account_data[['acct_num', 'time_diff']]


def calculate_average_time_diff(time_df):
    
    average_time_diff = time_df.groupby('acct_num')['time_diff'].mean().reset_index()
    average_time_diff.rename(columns={'time_diff': 'time_diff_mean'}, inplace=True)
    
    return average_time_diff

def process_feature(data,acct_num):
    print('in function process_feature: ',data.shape)
    data = data[data['acct_num'] == acct_num]
    ori_df = calculate_average_txn_amount(data, acct_num)
    output_df = calculate_variance(data,acct_num )
    output_df = ori_df.merge(output_df, on='acct_num')
    print(output_df.columns)

    time_df = calculate_time_diff(data)


    time_df_o = calculate_average_time_diff(time_df)
    print(time_df_o.columns)
    output_df = output_df.merge(time_df_o, on='acct_num')

    return output_df