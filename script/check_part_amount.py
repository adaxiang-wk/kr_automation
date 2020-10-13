import pandas as pd

log_fp = './logs/post_log.csv'
data_fp = './data/dataframe/bkr_new.csv'
log_df = pd.read_csv(log_fp)
data_df = pd.read_csv(data_fp)

failed = log_df[~pd.isnull(log_df['notes'])]
unmatched_isin = list(failed[failed['notes'].str.contains('CurrencyAmount')]['ISIN'])

unmatched = data_df.loc[data_df['표준코드'].isin(unmatched_isin), ['표준코드', '발행금액', 'bookrunners', 'bkr_parts', 'comanagers', 'cmgr_parts']]
unmatched.to_csv('./script/unmatched.csv', index=False)

print(unmatched)