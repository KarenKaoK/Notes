import multiprocessing
import sqlite3
import pandas as pd 
from feature import calculate_average_amount, calculate_variance, process_feature
from functools import partial

def create_db_connection():
    return sqlite3.connect('feature.db')

def process_acct_num(num, data):

    output_df = process_feature(data,num)
    conn = create_db_connection()
    output_df.to_sql('feature', conn, if_exists='append', index=False)

    conn.close()

if __name__ == '__main__':

    data = pd.read_csv('./data/transaction.csv')
    nums = data['num'].unique()
    start_date, end_date = '2023-10-20' , '2023-10-30'

    num_processes = multiprocessing.cpu_count()  
    print(num_processes)
    num_processes = num_processes-2

    process_acct_num_partial = partial(process_acct_num, data=data)

    with multiprocessing.Pool(num_processes) as pool:
        pool.map(process_acct_num_partial, nums)
