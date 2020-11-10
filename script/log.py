import os
import pandas as pd 

def log_all_json_in_dir():

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


def complete_log():
    prd_post_log_fp = './logs/prd_post_log1.csv'
    log_df = pd.read_csv(prd_post_log_fp)
    new_log = log_df.copy()

    isins = list(log_df['isin'])

    for isin in isins:
        tranche_df = log_df.loc[log_df['tranches in same deal'] == isin, :]
        if tranche_df.shape[0] > 1:
            deal_id = new_log.loc[new_log['isin'] == isin, ['deal_num']].values[0]
            post_notes = new_log.loc[new_log['isin'] == isin, ['post_notes']].values[0]
            new_log.loc[new_log['tranches in same deal'] == isin, ['deal_num']] = [deal_id] * tranche_df.shape[0]

            if not pd.isnull(post_notes):
                new_log.loc[new_log['tranches in same deal'] == isin, ['post_notes']] = [post_notes] * tranche_df.shape[0]

    new_log.to_csv('./logs/new_prd_post_log1.csv', index=False)


if __name__ == "__main__":
    complete_log()

    
    