import pandas as pd 
log_fp = './logs/prd_post_log1.csv'
excel_fp = './data/dataframe/Korea202005_202007.xls'

xls = pd.ExcelFile(excel_fp)
excel_df = pd.read_excel(xls, 'Sheet1')

isin_ls = list(excel_df['ISIN'])

log_df = pd.read_csv(log_fp)

result_ls = list(log_df['isin'])

for idx, i in enumerate(isin_ls):
    if i not in result_ls:
        print(idx, i)
