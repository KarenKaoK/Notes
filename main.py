import os
import pandas as pd 

# data_path = './data/bank.xlsx'
# df = pd.read_excel(data_path, sheet_name='Sheet1')
# df.head(5)



def split_by_id_save_to_pickle(df: pd.DataFrame, output_path:str)  -> list:
    unique_accounts = df['ID'].unique()
    pickle_filename_list = []

    for account_id in unique_accounts:
        account_data = df[df['ID'] == account_id]
        account_id = account_id[:-1]
        pickle_filename = f'{account_id}.pickle'

        account_data = account_data.reset_index(drop=True)
        
        account_data.to_pickle(os.path.join(output_path,pickle_filename))
        pickle_filename_list.append(pickle_filename)

    return pickle_filename_list


# pickle_filename_list = split_by_id_save_to_pickle(df,'process')
# pickle_filename_list

import psutil
import concurrent.futures
from tqdm import tqdm 
from typing import Callable

def bytes_to_gb(bytes_value):
    return bytes_value / (1024 ** 3)  # 1 GB = 1024^3 bytes

def calculate_transaction_percentage(data):

    initial_memory = psutil.virtual_memory().used
    print('init ram: ', initial_memory)

    data.sort_values(by=['ID', 'DATE'], inplace=True)
    data['Accumulated Deposit'] = data.groupby('ID')['DEPOSIT AMT'].cumsum()


    data['Transaction Percentage'] = data['DEPOSIT AMT'] / (data['Accumulated Deposit'] )

    final_memory = psutil.virtual_memory().used
    memory_change_bytes = final_memory - initial_memory
    memory_change_gb = bytes_to_gb(memory_change_bytes)
    print(f"Memory change during processing: {memory_change_gb:.10f} GB")

    return data

def preporcess(args):
    
    file_name = args[0]
    input_path = args[1]
    output_path = args[2]
    
    try:
        # output_path_list = []
        
        data = pd.read_pickle(os.path.join(input_path,file_name))
        data = calculate_transaction_percentage(data)
        data.to_pickle(os.path.join(output_path,file_name))
        # output_path_list.append(os.path.join(output_path,file_name))
        return f"success: {file_name}"
    except Exception as e:
        print(e)
        return f"error: {file_name}"

def concurrent_multi_process(list_:list, function_:Callable, *para):
    """
    Implement multi-process to speed up process time.
    Args:
        Input: list, function
        output: list of function's output

    """
    args = ((element, *para) for element in list_)
    with concurrent.futures.ProcessPoolExecutor() as executor:
        result_list = list(tqdm(executor.map(function_, args), total = len(list_)))
        
    return result_list


# file_list = concurrent_multi_process(pickle_filename_list, 
#                                     preporcess, 
#                                     'process', 
#                                     'output')    
# preporcess('process')    

# result = calculate_transaction_percentage(data)
# result.tail(3)


if __name__ == '__main__':
    
    pickle_filename_list = os.listdir('./process')
    print(pickle_filename_list)
    
    file_list = concurrent_multi_process(pickle_filename_list, 
                                    preporcess, 
                                    'process', 
                                    'output')    