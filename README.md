# 20230814_bacth_multiprocess

Due to insufficient RAM encountered during feature engineering, the following approach was devised and implemented.

To address the RAM limitation issue during feature engineering, the following steps were taken and executed:

- Before performing feature engineering, the dataframe was split and saved as pickle files.
- The feature engineering process was executed using a batch approach and utilizing multiprocess functionality.
- Using batch processing effectively tackled the RAM limitations, in addition to the acceleration achieved through multiprocess.

## Implementation Details:

1. The dataframe was split based on ID and saved as pickle files.
2. Feature engineering was performed using a batch approach and augmented with multiprocess execution.


## pip list
```
pip install pandas
pip install openpyxl
pip install psutil # ram monitor
pip install tqdm
```

## data 
- from kaggle : https://www.kaggle.com/datasets/apoorvwatsky/bank-transaction-data?sort=votes
- 116201 rows × 9 columns
