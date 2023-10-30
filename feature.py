import multiprocessing
import sqlite3
import pandas as pd


def create_db_connection():
    return sqlite3.connect('mydatabase.db')


def calculate_average_amount(data, num):

    average_amount = data.groupby('num')['amount'].mean().reset_index()
    print(average_amount)
    return average_amount


def calculate_variance(data, num ):
    
    variance_data = data.groupby('num')['amount'].var().reset_index()
    variance_data.rename(columns={'amount': 'amount_variance'}, inplace=True)
    
    return variance_data



def calculate_time_diff(data):
    account_data = data.copy()
    account_data['time'] = pd.to_datetime(account_data['time'], format='%H:%M')
    

    account_data = account_data.sort_values(by=['date', 'time'])
    account_data['time_diff'] = account_data['time'].diff().dt.total_seconds() / 60.0
    
    return account_data[['num', 'time_diff']]


def calculate_average_time_diff(time_df):
    
    average_time_diff = time_df.groupby('num')['time_diff'].mean().reset_index()
    average_time_diff.rename(columns={'time_diff': 'time_diff_mean'}, inplace=True)
    
    return average_time_diff

def process_feature(data,num):
    print('in function process_feature: ',data.shape)
    data = data[data['num'] == num]
    ori_df = calculate_average_amount(data, num)
    output_df = calculate_variance(data,num )
    output_df = ori_df.merge(output_df, on='num')
    print(output_df.columns)

    time_df = calculate_time_diff(data)
    time_df_o = calculate_average_time_diff(time_df)
    print(time_df_o.columns)
    output_df = output_df.merge(time_df_o, on='num')

    return output_df