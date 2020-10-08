import infoParser.data_utils as tools
import re
import ast

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
        amount = du.parse_issue_size(size)
        print(amount)


def test_parse_cp_freq(df):
    freqs = df.interest_calculation_cycle_months.unique()
    
    for freq in freqs:
        print(du.parse_cp_freq(freq))



if __name__ == "__main__":
    du = tools.Tool(env_type='prd')
    bkr_fp = './data/bkr_df.csv'
    df = du.load_bkr_df(bkr_fp)

    # test_parse_time(df)
    # test_ann_price_date(df)
    # test_parse_issue_size(df)
    test_parse_cp_freq(df)
    
    
    