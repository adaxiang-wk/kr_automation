import infoParser.main_parsing as mp
import pandas as pd 

# mp.update_rank(2394326, env_type='prd')




def update_rank():
    log_pf = './logs/prd_post_log2.csv'
    log_df = pd.read_csv(log_pf)

    deal_nums = list(log_df['deal_num'].unique())

    for deal_num in deal_nums:
        if deal_num == 0:
            continue

        tranche_df = log_df.loc[log_df['deal_num'] == deal_num, :]
        if tranche_df.shape[0] == 1:
            continue

        if ( tranche_df['parse_notes'].str.contains('no original book runner info').any()):
            continue
        
        # print(int(deal_num))
        mp.update_rank(int(deal_num), env_type='prd')


def update_with_json():
    fp = './data/json/prd_json/KR6140179A77_2394216.json'
    mp.update_tranches_from_file(2394216, fp, env_type='prd')


if __name__ == "__main__":
    update_with_json()