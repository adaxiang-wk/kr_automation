import json
import pandas as pd
from datetime import datetime, timedelta
import re
from dateutil.relativedelta import relativedelta
import ast
import envSetter.env_config as ec
import numpy as np
import sys

"""
In Anaconda Prompt, input `chcp 936` for displaying Chinese characters
"""
COL_MAP_CSV = {'产品全称':'name', '产品简称':'local_symbol', '产品代码':'prod_id',
           '发行（创设）价格（元/百元面值）':'issue_price_%', '面值（元）':'denominations', 
           '发行（创设）日':'issue_date',
           '期限':'terms', '到期（兑付）日':'mat_date', 
           '产品评级':'issue_rating', '产品评级机构':'issue_rating_agency', 
           '主体评级':'issuer_rating', '主体评级机构':'issuer_rating_agency',
           '计息方式':'coupon_type', '票面年利率（%）':'cp_rate',
           '基准利率种类':'base_rate_type', '初始基准利率（%）':'initial_base_rate', '利差（%）':'margin_rate',
           '付息频率':'coupon_freq', '起息日':'interest_accr_date', '发行（创设）总额（亿元）':'currency_amount', 
           '发行（创设）机构名称':'issuer', '备注':'notes'}

COL_MAP_EXL = {'Name':'name', '产品代码':'prod_id', '发行（创设）日':'issue_date', 
           '起息日':'interest_accr_date', '发行（创设）总额（亿元）':'currency_amount', 
           '发行（创设）价格（元/百元面值）':'issue_price_%', '到期（兑付）日':'mat_date', 
           '产品评级机构':'rating_agency', '产品评级':'rating', '票面年利率（%）':'cp_rate',
           '初始基准利率（%）':'initial_base_rate', '利差（%）':'margin_rate', '发行（创设）机构名称':'issuer',
           '取消发行':'is_canceled', '发行情况公告':'issue_annoucement'}

BOND_TYPE_MAP = {'中期票据': 'MTN', '资产支持票据': 'ABN', '资产支持证券': 'ABN'}

######################## ENV SETTING ########################
ENV_TYPE = 'prd'
data_env = ec.EnvConfig(env_type=ENV_TYPE)
#############################################################


######################## LOADING FILES ######################
def load_excel(fp):
    """
    Load China bonds log excel file
    Args:
        - fp: file path
    """
    xls = pd.ExcelFile(fp)
    df = pd.read_excel(xls, 'Sheet2')
    df = df.rename(columns=COL_MAP_EXL)
    return df


def load_csv(fp):
    """
    Load web scrapped csv
    Args: 
        - fp: file path
    """
    df = pd.read_csv(fp)
    df = df.rename(columns=COL_MAP_CSV)
    return df


def rename(df):
    df = df.rename(columns=COL_MAP_CSV)
    return df
#############################################################


######################## SEARCH COMPANY ########################
def _search_company(search_phrase, is_active='true'):
    """
    Search company id with given search phrase

    Args:
        - search_phrase
        _ is_active: whether the company is marked "activate" in the database
    """
    
    url = f'{data_env.COMPANY_BASE_URL}Lookup?name={search_phrase}&isActivateValues={is_active}'
    found = data_env.auth_app.get(url).json()

    # while len(found) < 1:
    #     print(f"wrong search phrase: {search_phrase}")
    #     search_phrase = search_phrase[:-1]
    #     print(f"try new search_phrase: {search_phrase}")
    #     url = f'{data_env.COMPANY_BASE_URL}Lookup?name={search_phrase}&isActivateValues={is_active}'
    #     found = data_env.auth_app.get(url).json()
    if len(found) < 1:
        candidates = None
    else:
        candidates = pd.json_normalize(found)
    return candidates


def _filter_search_result(full_name, candidates, countryID=218):
    if countryID > 0:
        company = candidates[candidates['NationalityOfBusinessId'] == countryID]
    else:
        company = candidates

    if company.shape[0] < 1:
        raise ValueError(f"cannot find companies with given countryID = {countryID}")
    else:
        # company = company.iloc[0, :] seems to be the one that got deleted
        company = company.iloc[-1, :]

    return company['Id']


def get_company_info(issuer, name, bond_type, countryID=44, is_active='true'):
    if bond_type == 'ABN':
        num_idx = name.index('2')
        search_phrase = search_phrase = name[:num_idx]
    else:
        search_phrase = issuer
    candidates = _search_company(search_phrase)
    if candidates is None:
        company_id = -1
    else:
        company_id = _filter_search_result(issuer, candidates, countryID=countryID)

    return company_id


def get_trusteeID(issuer, countryID=44, is_active='true'):
    search_phrase = issuer
    candidates = _search_company(search_phrase)
    company_id = _filter_search_result(issuer, candidates, countryID=countryID)

    return company_id

#############################################################


def _get_reference(ref_name, ref_type='DCM'):
    
    if len(ref_type) == 0:
        url = data_env.REF_BASE_URL + ref_name
    else:
        url = data_env.REF_BASE_URL + f'{ref_type}/{ref_name}'

    ref = data_env.auth_app.get(url).json()
    ref_df = pd.json_normalize(ref)

    return ref_df


################### PARSE DATE INFO #########################
def parse_date(date_obj):
    #datetime_object = datetime.strptime(date_str, '%Y-%m-%d')
    date_obj = datetime.strptime(date_obj, '%Y-%m-%d')
    return date_obj.isoformat('T')+'Z'


def parse_ann_price_date(date_obj):
    date_obj = datetime.strptime(date_obj, '%Y-%m-%d')

    result = date_obj - timedelta(days=1)

    week = result.strftime('%A')
    if week == 'Sunday':
        result = date_obj - timedelta(days=3)
    elif week == 'Saturday':
        result = date_obj - timedelta(days=2)

    return result.isoformat('T')+'Z'


def calc_1cp_date(interest_acc_datetime, pay_freq):
    #issue_datetime = datetime.strptime(issue_date, '%d %B %Y')
    interest_acc_datetime = datetime.strptime(interest_acc_datetime, '%Y-%m-%d')
    if '每年付息' in pay_freq:
        cp_date = interest_acc_datetime + relativedelta(years=1)
    elif '每季付息' in pay_freq:
        cp_date = interest_acc_datetime + relativedelta(months=3)
    elif pay_freq == '到期一次还本付息':
        return None
    else:
        print(f'calc_1cp_date: find new coupon frequency type {pay_freq}')
        sys.exit(0)
        
    """ Other type not found yet """
    # elif pay_freq == 'Quarterly':
    #     cp_date = issue_datetime + relativedelta(months=3)
    # elif pay_freq == 'At Maturity':
    #     return
    
    return cp_date.isoformat('T')+'Z'

#############################################################

def get_currency_info(currency_amount):
    amount = float(currency_amount*100)
    return amount


def get_cp_rate(cp_rate, initial_base_rate, margin_rate):
    if pd.isnull(cp_rate) and pd.isnull(initial_base_rate) and pd.isnull(margin_rate):
        return None

    if pd.isnull(cp_rate) or cp_rate == 0:
        result = round(float(initial_base_rate) + float(margin_rate), 2)
    else:
        result = cp_rate

    return result


def parse_cp_freq(pay_freq):
    cp_freq_df = _get_reference('CouponFrequency', ref_type='DCM')
    if '每年付息' in pay_freq:
        result_id = cp_freq_df[cp_freq_df.Name == 'Annual']['Id'].values[0]
    elif '每季付息' in pay_freq:
        result_id = cp_freq_df[cp_freq_df.Name == 'Quarterly']['Id'].values[0]
    elif pay_freq == '到期一次还本付息':
        result_id = cp_freq_df[cp_freq_df.Name == 'None']['Id'].values[0]
    else:
        print(f'parse_cp_freq: find new coupon frequency type {pay_freq}')
        sys.exit(0)

    return result_id


def parse_seniority(bond_type, name):
    if bond_type == 'MTN':
        senior_df = _get_reference('Seniority', ref_type='DCM')
        name = 'Senior Unsecured'
        senior_id = senior_df[senior_df.Name == name]['Id'].values[0]
        is_senior = True
    else:
        senior_id = None
        if '次级' in name:
            is_senior = False
        else:
            is_senior = True

    return senior_id, is_senior


def parse_characts(mat_date, bond_type, cp_rate):
    if bond_type == 'ABN' or bond_type == 'ABS':
        ids = [1]
    else:
        if pd.isnull(mat_date) or cp_rate == 0:
            ids = [11, 47, 29, 20]
        else:
            ids = [11]

    return ids


def parse_mat_date(mat_date, cp_rate):
    if pd.isnull(mat_date) or cp_rate == 0:
        type_id = 8
        date = None
    else:
        type_id = 1
        date = parse_date(mat_date)
    return {'TypeId': type_id, 'Date': date}


def parse_issue_type(cp_rate, initial_base_rate, margin_rate):
    coupon_type_df = _get_reference('IssueType', ref_type='DCM')
    if pd.isnull(cp_rate) and pd.isnull(initial_base_rate) and pd.isnull(margin_rate):
        cp_type_id = coupon_type_df[coupon_type_df['Name'] == 'Fixed rate zero coupon']['Id'].values[0]
    else:
        cp_type_id = coupon_type_df[coupon_type_df['Name'] == 'Fixed rate']['Id'].values[0]

    return cp_type_id





def json_dumper(parsed_dict, fp=''):

    def convert(obj):
        if isinstance(obj, np.int64): 
            return int(obj)  
        # raise TypeError

    if len(fp) < 1:
        data = json.dumps(parsed_dict, default=convert)
    else:
        with open(fp, "w") as outfile:  
            json.dump(parsed_dict, outfile, default=convert)
        data = f'{fp} saved'
    return data





if __name__ == "__main__":
    # df = load_excel('./dependencies/china_bond_log.xlsm')
    # df.to_csv('./testing_df.csv')
    # print(df.columns)
    # env = ec.EnvConfig(env_type='paa')
    # print(env.GET_BASE_URL)

    print(_get_reference('CouponFrequency', ref_type='DCM'))

    # df = load_csv('./final_testing_df.csv')
    # print(df)