import src.infoParser.data_utils as tools
import src.infoParser.info_parser as ipsr
import src.infoParser.main_parsing as mp
import re
import ast
import pandas as pd
from googletrans import Translator


du = tools.ParseToolBox(env_type='prd')
def test_get_company(df):
    companies = df.issuer_name.unique()
    for cpny in companies:
        cpny_id = du.get_company_info(cpny)
        print(cpny, cpny_id)


def test_parse_time(df):
    dates = df.settlement_date.unique()
    for d in dates:
        parsed_date = du.parse_date(d)
        print(parsed_date)


def test_ann_price_date(df):
    dates = df.settlement_date.unique()
    for d in dates:
        parsed_date = du.parse_date(d)

        ann_price_date = du.parse_ann_price_date(d)

        print(f'set date: {parsed_date}\nprice date: {ann_price_date}')


def test_parse_issue_size(df):
    sizes = df.issue_size.unique()
    for size in sizes:
        amount = du.parse_money(size)
        print(amount)


def test_parse_cp_freq(df):
    freqs = df.interest_calculation_cycle_months
    first_dates = df.initial_interest_payment_date
    
    for freq, first_day in zip(freqs, first_dates):
        if pd.isnull(first_day):
            print('no first day')
        else:
            print(first_day)
        print(du.parse_cp_freq(freq, first_day))


def test_parse_syndicate(df, data_fp):
    my_parser = ipsr.Parser(data_fp, env_type='prd')
    for _, record in df.iterrows():
        print(record['isin'])
        syndicate = my_parser.parse_syndicate(record)
        print(syndicate)
        if syndicate is not None:
            print(f'parsed len {len(syndicate)}')


def test_detect_tranche(df, data_fp):
    my_parser = ipsr.Parser(data_fp, env_type='prd')
    for _, record in df.iterrows():
        print(record['isin'])
        if record['isin'] in my_parser.parsed_history:
            continue
        
        one_deal = my_parser.find_tranches(record)
        print(one_deal)


def test_parse_one_deal(data_fp, isin):
    my_parser = ipsr.Parser(data_fp, env_type='prd')
    df = my_parser.data_df
    record = df.loc[df['isin'] == isin, :].iloc[0]
    print(record)
    my_parser.parse_one_deal(record)
    parsed = my_parser.deal_dict

    # isin = record['isin']
    tools.json_dumper(parsed, f'{isin}.json')
    
    print(isin)


def test_parse_margin_rate(df):
    special_conds = list(df.special_issuance_conditions)

    for cond in special_conds:
        if du.is_float(cond):
            margin_rate = du.parse_margin_rate(cond)
            print(margin_rate)


def test_parse_batch(data_fp):
    mp.parse_batch(data_fp, env_type='prd', output_dir='./data/json/pie_json3/', log_fp='./logs/parse_log3.csv')

def test_post_one_deal():
    mp.post_one_deal('KR6000011A83', './data/json/pie_json2', env_type='pie')

def test_post_batch():
    mp.post_batch('./data/json/pie_json3', './logs/parse_log3.csv', is_new_log=True, env_type='pie')


def translate():
    translator = Translator() 
    print(translator.translate('케이티비투자증권').text)

def test_search_company():
    company_list = ['디에스투자증권', '엔에이치투자증권', '우리은행']
    for company in company_list:
        print(du._search_company(company))


def test_company(name):
    idx = du.get_company_info(name)
    print(idx)




if __name__ == "__main__":
    # du = tools.ParseToolBox(env_type='pie')
    # print(du._search_company('bank of china'))
    bkr_fp = './data/dataframe/bkr_new2.csv'
    # df = du.load_bkr_df(bkr_fp)

    # print(du._search_company('1223240'))

    # test_get_company(df)
    # test_parse_time(df)
    # test_ann_price_date(df)
    # test_parse_issue_size(df)
    # test_parse_cp_freq(df)
    # test_parse_margin_rate(df)

    # test_parse_syndicate(df, bkr_fp)
    # test_detect_tranche(df, bkr_fp)
    # test_parse_one_deal(bkr_fp, 'KR6010121A55')

    # test_parse_batch(bkr_fp)
    # test_post_one_deal()
    # test_post_batch()

    # translate()
    # test_search_company()
    # ls = ['한국투자증권', '케이비증권', '신한금융투자', '미래에셋대우', '엔에이치투자증권', '신한금융투자', '한국투자증권', '엔에이치투자증권', '미래에셋대우']
    # for l in ls:
    #     test_company(l)
    test_company('NH투자증권')
    
    