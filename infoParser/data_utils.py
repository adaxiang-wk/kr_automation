import pandas as pd 
import json
import re
import ast
from datetime import datetime, timedelta
import numpy as np
import csv
import os
from googletrans import Translator

import envSetter.env_config as ec


with open('./dependencies/company_dict.json', encoding='UTF-8-sig') as js_file:
    BANKS_DICT = json.load(js_file)


class ParseToolBox:
    def __init__(self, env_type):
        # self.data_env = ec.EnvConfig(env_type=env_type)
        self.env_type = env_type


    def _clean_bkr(self, bkr):
        bkr = str(bkr)
        bkr = re.sub(r'\n+', '', bkr)
        if bkr == '[]' or bkr == '[[]]':
            cleaned = []
        else:
            bkr = ast.literal_eval(bkr)
            cleaned = list(map(lambda x: re.sub(r"㈜|\n+|[\(\[].*?[\)\]]", "", x), bkr))
        
        return cleaned


    def check_participation(self, issue_size, bkr_parts, cmgr_parts):
        if bkr_parts == []:
            return True


        bkr_parts_sum = sum(bkr_parts)
        cmgr_parts_sum = sum(cmgr_parts)

        total = bkr_parts_sum + cmgr_parts_sum

        return total == issue_size


    def get_deal_info(self, deal_num):
        data_env = ec.EnvConfig(self.env_type, 'dcm')
        url = data_env.GET_BASE_URL
        json_f = data_env.auth_app.get(url+deal_num).json()
        return json_f

    
    def _get_reference(self, ref_name, ref_type='DCM'):
        data_env = ec.EnvConfig(self.env_type, 'reference')
        if len(ref_type) == 0:
            url = data_env.REF_BASE_URL + ref_name
        else:
            url = data_env.REF_BASE_URL + f'{ref_type}/{ref_name}'

        ref = data_env.auth_app.get(url).json()
        ref_df = pd.json_normalize(ref)

        return ref_df


    def load_bkr_df(self, fp, save=False):
        bkr_df = pd.read_csv(fp)
        bkr_df['발행기관명'] = bkr_df['발행기관명'].apply(lambda x: re.sub(r'[\(\[].*?[\)\]]', "", x))
        bkr_df['bookrunners'] = bkr_df['bookrunners'].apply(lambda x: self._clean_bkr(x))
        bkr_df['comanagers'] = bkr_df['comanagers'].apply(lambda x: self._clean_bkr(x))
        
        with open('./dependencies/field_dict.json', encoding='UTF-8-sig') as js_file:
            translation = json.load(js_file)
        
        translated_df = bkr_df.rename(columns=translation)

        if save:
            translated_df.to_csv('./data/translated_bkr_df.csv')

        return translated_df 


    ######################## SEARCH COMPANY ########################
    def search_in_dict(self, search_phrase):
        if search_phrase in BANKS_DICT.keys():
            search_phrase = BANKS_DICT[search_phrase]
        else:
            translator = Translator()
            translated = str(translator.translate(search_phrase).text)
            BANKS_DICT.update({search_phrase: translated})
            with open('./dependencies/company_dict.json', "w", encoding='UTF-8-sig') as outfile:  
                json.dump(BANKS_DICT, outfile, ensure_ascii=False)
            search_phrase = translated

        return search_phrase 


    def _search_company(self, search_phrase, is_active='true'):
        """
        Search company id with given search phrase

        Args:
            - search_phrase
            _ is_active: whether the company is marked "activate" in the database
        """
        if len(search_phrase) <= 2:
            search_phrase = self.search_in_dict(search_phrase)

        if len(search_phrase) <= 2:
            return None
        
        data_env = ec.EnvConfig(self.env_type, 'company')
        url = f'{data_env.COMPANY_BASE_URL}Lookup?name={search_phrase}&isActivateValues={is_active}'
        found = data_env.auth_app.get(url).json()

        if len(found) < 1:
            search_phrase = self.search_in_dict(search_phrase)
            url = f'{data_env.COMPANY_BASE_URL}Lookup?name={search_phrase}&isActivateValues={is_active}'
            found = data_env.auth_app.get(url).json()
            if len(found) < 1:
                candidates = None
            else:
                candidates = pd.json_normalize(found)
        else:
            candidates = pd.json_normalize(found)
        return candidates


    def _filter_search_result(self, full_name, candidates, countryID=119):

        if countryID > 0:
            company = candidates[candidates['NationalityOfBusinessId'] == countryID]
        else:
            company = candidates

        print(full_name)

        if company.shape[0] < 1:
            raise ValueError(f"cannot find companies with given countryID = {countryID}")
        else:
            # if possible, find the one with the exact proper name
            chosen = company.loc[company['ProperName'] == full_name, :]

            if chosen.shape[0] != 1:
                chosen = company.iloc[[0], :] 
                # chosen = company.iloc[[-1], :]
        # print(chosen)
        return chosen


    def get_company_info(self, issuer, countryID=119, is_active='true'):
        search_phrase = re.sub(r"㈜|\n+|[\(\[].*?[\)\]]", "", issuer)
        print(search_phrase)
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


    def parse_issue_type(self, special_cond):
        if self.is_float(special_cond):
            name = "Floating rate note"
        else:
            name = 'Fixed rate'

        coupon_type_df = self._get_reference('IssueType', ref_type='DCM')
        cp_type_id = coupon_type_df[coupon_type_df['Name'] == name]['Id'].values[0]

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


    def parse_money(self, issue_size):
        amount = int(issue_size.replace(',', ''))
        amount = round((amount / 1000000), 2)

        return amount


    def parse_cp_freq(self, pay_freq, first_cp_day):
        cp_freq_df = self._get_reference('CouponFrequency', ref_type='DCM')
        
        if pd.isnull(first_cp_day):
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


    def is_float(self, special_cond):
        is_float = False

        if pd.isnull(special_cond) == False:
            if ('^' not in special_cond) and ('%' in special_cond):
                is_float = True

        return is_float 


    def parse_margin_rate(self, special_cond):
        perc_idx = special_cond.index('%')

        rate = []
        for i in range(perc_idx-1, -1, -1):
            cur_char = special_cond[i]
            if cur_char.isdigit() or cur_char == '.':
                rate.append(cur_char)
            else:
                break

        return float(''.join(rate[::-1]))
                



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


def parse_loging(isins, notes, idx, log_fp):
    if os.path.exists(log_fp):
        is_newlog = False
    else:
        is_newlog = True

    if is_newlog:
        write_option = 'w'
        with open(log_fp, mode=write_option, encoding='UTF-8-sig') as log_file:
            log_writer = csv.writer(log_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            if idx == 0:
                log_writer.writerow(['isin', 'tranches in same deal', 'parse_notes'])

            if len(isins) > 1:
                for isin in isins:
                    log_writer.writerow([isin, isins[0], notes])
            else:
                log_writer.writerow([isins[0], isins[0], notes])
    else:
        write_option = 'a'
        with open(log_fp, mode=write_option, encoding='UTF-8-sig') as log_file:
            log_writer = csv.writer(log_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            if len(isins) > 1:
                for isin in isins:
                    log_writer.writerow([isin, isins[0], notes])
            else:
                log_writer.writerow([isins[0], isins[0], notes])



def post_loging2(isins, notes, deal_number, idx, log_fp):
    if os.path.exists(log_fp):
        is_newlog = False
    else:
        is_newlog = True

    if is_newlog:
        write_option = 'w'
        with open(log_fp, mode=write_option, encoding='UTF-8-sig') as log_file:
            log_writer = csv.writer(log_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            if idx == 0:
                log_writer.writerow(['isin', 'tranches in same deal', 'deal_number', 'parse_notes'])

            if len(isins) > 1:
                for isin in isins:
                    log_writer.writerow([isin, isins[0], deal_number, notes])
            else:
                log_writer.writerow([isins[0], isins[0], deal_number, notes])
    else:
        write_option = 'a'
        with open(log_fp, mode=write_option, encoding='UTF-8-sig') as log_file:
            log_writer = csv.writer(log_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            if len(isins) > 1:
                for isin in isins:
                    log_writer.writerow([isin, isins[0], deal_number, notes])
            else:
                log_writer.writerow([isins[0], isins[0], deal_number, notes])


def post_loging(isin, deal_num, notes, log_fp, output_fp):
    print(output_fp)
    if log_fp[-3:] == 'xls':
        xls = pd.ExcelFile(log_fp)
        log_df = pd.read_excel(xls, 'Sheet1')
    else:
        log_df = pd.read_csv(log_fp)

    # log_df['deal_no'] = [0]*log_df.shape[0]
    # log_df['notes'] = ['']*log_df.shape[0]

    log_tranche_df = log_df.loc[log_df['tranches in same deal'] == isin, :]

    if log_tranche_df.shape[0] == 1:
        log_df.loc[log_df['tranches in same deal'] == isin, ['deal_num']] = deal_num
        log_df.loc[log_df['tranches in same deal'] == isin, ['post_notes']] = notes
    else:
        log_df.loc[log_df['tranches in same deal'] == isin, ['deal_num']] = [deal_num]*log_tranche_df.shape[0]
        if len(notes) != 0:
            log_df.loc[log_df['tranches in same deal'] == isin, ['post_notes']] = [notes]*log_tranche_df.shape[0] 

    log_df.to_csv(output_fp, index=False)


######## Checkings before Saving #################
def check_rank(data_dict):
    tranches = data_dict['Tranches']

    for idx, tranche_dict in enumerate(tranches):
        rank = tranche_dict['IsRankEligible']
        bkr_list = tranche_dict['Syndicate']

        bkr_available = True
        if (len(bkr_list) == 1) and (bkr_list[0]['Id'] == 141783):
            bkr_available = False
        
        if bkr_available == False and rank == True:
            tranche_dict.update({'IsRankEligible': False})
            tranches[idx] = tranche_dict
        elif bkr_available == True and rank == False:
            tranche_dict.update({'IsRankEligible': True})
            tranches[idx] = tranche_dict

    return tranches




    







