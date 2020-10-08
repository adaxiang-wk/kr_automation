import pandas as pd 
import json
import re
import ast
from datetime import datetime, timedelta

import envSetter.env_config as ec


class Tool:
    def __init__(self, env_type):
        self.data_env = ec.EnvConfig(env_type=env_type)


    def _clean_bkr(self, bkr):
        bkr = re.sub(r'\n+', '', bkr)
        if bkr == '[]' or bkr == "['[]']":
            cleaned = '[]'
        else:
            bkr = ast.literal_eval(bkr)
            cleaned = list(map(lambda x: re.sub(r"㈜|\n+|[\(\[].*?[\)\]]", "", x), bkr))
        
        return cleaned

    
    def _get_reference(self, ref_name, ref_type='DCM'):
    
        if len(ref_type) == 0:
            url = self.data_env.REF_BASE_URL + ref_name
        else:
            url = self.data_env.REF_BASE_URL + f'{ref_type}/{ref_name}'

        ref = self.data_env.auth_app.get(url).json()
        ref_df = pd.json_normalize(ref)

        return ref_df


    def load_bkr_df(self, fp, save=False):
        bkr_df = pd.read_csv(fp)
        bkr_df['발행기관명'] = bkr_df['발행기관명'].apply(lambda x: re.sub(r'[\(\[].*?[\)\]]', "", x))
        bkr_df['bookrunners'] = bkr_df['bookrunners'].apply(lambda x: self._clean_bkr(x))
        
        with open('./dependencies/kr_eng_translation.json', encoding='UTF-8-sig') as js_file:
            translation = json.load(js_file)
        
        translated_df = bkr_df.rename(columns=translation)

        if save:
            translated_df.to_csv('./data/translated_bkr_df.csv')

        return translated_df 


    ######################## SEARCH COMPANY ########################
    def _search_company(self, search_phrase, is_active='true'):
        """
        Search company id with given search phrase

        Args:
            - search_phrase
            _ is_active: whether the company is marked "activate" in the database
        """
        
        url = f'{self.data_env.COMPANY_BASE_URL}Lookup?name={search_phrase}&isActivateValues={is_active}'
        found = self.data_env.auth_app.get(url).json()

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


    def _filter_search_result(self, full_name, candidates, countryID=119):
        if countryID > 0:
            company = candidates[candidates['NationalityOfBusinessId'] == countryID]
        else:
            company = candidates

        if company.shape[0] < 1:
            raise ValueError(f"cannot find companies with given countryID = {countryID}")
        else:
            # if possible, find the one with the exact proper name
            chosen = company.loc[company['ProperName'] == full_name, :]

            if chosen.shape[0] != 1:
                # company = company.iloc[0, :] seems to be the one that got deleted
                chosen = company.iloc[[-1], :]
        print(chosen)
        return chosen


    def get_company_info(self, issuer, countryID=119, is_active='true'):
        
        search_phrase = issuer
        candidates = self._search_company(search_phrase)
        if candidates is None:
            company_id = -1
        else:
            company = self._filter_search_result(issuer, candidates, countryID=countryID)
            company_id = company['Id'].values[0]

        return company_id

    ######################## SEARCH COMPANY END ########################

    ######################## PARSE DATE INFO ########################
    def parse_date(self, date_str):
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        # date_obj = datetime.strptime(date_obj, '%Y-%m-%d')
        return date_obj.isoformat('T')+'Z'


    def parse_ann_price_date(self, date_str):
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')

        result = date_obj - timedelta(days=1)

        week = result.strftime('%A')
        if week == 'Sunday':
            result = date_obj - timedelta(days=3)
        elif week == 'Saturday':
            result = date_obj - timedelta(days=2)

        return result.isoformat('T')+'Z'


    def parse_mat_date(self, mat_date, cp_rate):
        if pd.isnull(mat_date) or cp_rate == 0:
            type_id = 8
            date = None
        else:
            type_id = 1
            date = self.parse_date(mat_date)
        return {'TypeId': type_id, 'Date': date}
    ######################## PARSE DATE INFO END ########################


    def parse_issue_type(self):
        coupon_type_df = self._get_reference('IssueType', ref_type='DCM')
        cp_type_id = coupon_type_df[coupon_type_df['Name'] == 'Fixed rate']['Id'].values[0]

        return cp_type_id


    def parse_seniority(self, guarantee):
        senior_df = self._get_reference('Seniority', ref_type='DCM')

        if guarantee == '커버드본드':
            name = 'Senior Secured'
        else:
            name = 'Senior Unsecured'

        senior_id = senior_df[senior_df.Name == name]['Id'].values[0]

        is_senior = False
        if senior_id == 2 or senior_id == 1:
            is_senior = True

        return senior_id, is_senior


    def parse_issue_size(self, issue_size):
        amount = int(issue_size.replace(',', '')[:-6])

        return amount


    def parse_cp_freq(self, pay_freq):
        cp_freq_df = self._get_reference('CouponFrequency', ref_type='DCM')
        
        if pd.isnull(pay_freq):
            result_id = cp_freq_df[cp_freq_df.Name == 'None']['Id'].values[0]
        else:
            pay_freq = int(pay_freq)

            if pay_freq == 12:
                result_id = cp_freq_df[cp_freq_df.Name == 'Annual']['Id'].values[0]
            elif pay_freq == 6:
                result_id = cp_freq_df[cp_freq_df.Name == 'Semi-Annual']['Id'].values[0]
            elif pay_freq == 3:
                result_id = cp_freq_df[cp_freq_df.Name == 'Quarterly']['Id'].values[0]
            elif pay_freq == 1:
                result_id = cp_freq_df[cp_freq_df.Name == 'Monthly']['Id'].values[0]
            else:
                print(f'parse_cp_freq: find new coupon frequency type {pay_freq}')
                return -1

        return result_id



