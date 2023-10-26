# Parallel Feature Engineering Example

This example demonstrates how to perform feature engineering tasks using multiple processes in Python and store the results in an SQLite database. Feature engineering is an essential step in data science for creating new features or transforming existing ones to improve the performance of machine learning models.

## Feature Engineering Example

In this example, feature engineering is performed in the process_feature function, including calculating the mean transaction amount, transaction amount variance, and the mean of transaction time differences. You can add more feature engineering functions as needed.


```python
import multiprocessing
import sqlite3
import pandas as pd
from feature import calculate_average_txn_amount, calculate_variance, process_feature
from functools import partial

# Create a database connection
def create_db_connection():
    return sqlite3.connect('feature.db')

# Process feature engineering for a single account
def process_acct_num(acct_num, data):
    output_df = process_feature(data, acct_num)
    conn = create_db_connection()
    output_df.to_sql('feature', conn, if_exists='append', index=False)
    conn.close()

if __name__ == '__main__':
    # Load data from a CSV file
    data = pd.read_csv('./data/transaction.csv')
    acct_nums = data['acct_num'].unique()

    # Get the number of CPU cores and subtract 2 for multiprocessing
    num_processes = multiprocessing.cpu_count()
    num_processes = num_processes - 2

    # Bind parameters to the process function using functools.partial
    process_acct_num_partial = partial(process_acct_num, data=data)

    # Use multiprocessing to parallelize feature engineering tasks
    with multiprocessing.Pool(num_processes) as pool:
        pool.map(process_acct_num_partial, acct_nums)

```

