import infoParser.data_utils as tools
import infoParser.info_parser as ipsr
import infoParser.main_parsing as mp
import re
import ast
import pandas as pd

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


def test_parse_one_deal(data_fp):
    my_parser = ipsr.Parser(data_fp, env_type='prd')
    record = my_parser.data_df.iloc[1, :]
    my_parser.parse_one_deal(record)
    parsed = my_parser.deal_dict

    isin = record['isin']
    tools.json_dumper(parsed, f'{isin}.json')
    
    print(isin)


def test_parse_margin_rate(df):
    special_conds = list(df.special_issuance_conditions)

    for cond in special_conds:
        if du.is_float(cond):
            margin_rate = du.parse_margin_rate(cond)
            print(margin_rate)


def test_parse_batch(data_fp):
    mp.parse_batch(data_fp, env_type='prd')



if __name__ == "__main__":
    du = tools.ParseToolBox(env_type='prd')
    bkr_fp = './data/dataframe/test_bkr.csv'
    df = du.load_bkr_df(bkr_fp)

    # test_parse_time(df)
    # test_ann_price_date(df)
    # test_parse_issue_size(df)
    # test_parse_cp_freq(df)
    # test_parse_margin_rate(df)

    # test_parse_syndicate(df, bkr_fp)
    # test_detect_tranche(df, bkr_fp)
    # test_parse_one_deal(bkr_fp)

    test_parse_batch(bkr_fp)
    
    
    