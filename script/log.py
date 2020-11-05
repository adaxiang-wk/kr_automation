import os
import pandas as pd 

json_fp = './data/json/prd_json'
prd_post_log_fp = './logs/prd_parse_log4.csv'
log_df = pd.read_csv(prd_post_log_fp)

js_files = os.listdir(json_fp)

for f in js_files:
    if '_' not in f:
        continue
    f_split = f.split('_')
    isin = f_split[0]
    deal_number = f_split[1][:-5]

    # prd_post_log_df.loc[prd_post_log_fp['isin'] == isin, ['deal_num']] = deal_number

    log_df.loc[log_df['isin'] == isin, ['deal_num']] = deal_number

log_df.to_csv('test.csv')
    
    