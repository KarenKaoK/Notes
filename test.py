data_event_count = df.copy()
count_df = data_event_count.groupby('acct_num').size().reset_index(name='count')
acct_num_list = count_df[count_df['count']<=1000]['acct_num'].tolist()
del data_event_count, count_df
df = df[df.acct_num.isin(acct_num_list)]
