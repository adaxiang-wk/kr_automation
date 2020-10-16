import infoParser.data_utils as tools
import pandas as pd
import json
import numpy as np
import ast

"""
TODO:
- bookrunner
"""

class Parser:
    def __init__(self, data_fp, env_type):
        with open('./dependencies/post_format.json') as js_file:
            self.post_format_dict = json.load(js_file)

        self.deal_dict = self.post_format_dict.copy()
        self.tranche_format = self.deal_dict['Tranches'][0].copy()
        self.syndicate_format = self.tranche_format['Syndicate'][0].copy()
        self.parsed_history = []
        
        self.du = tools.ParseToolBox(env_type=env_type)
        self.data_df = self.du.load_bkr_df(data_fp)

        self.note = []


    def find_tranches(self, record):
        issuer_name = record['issuer_name']
        listing_date = record['listing_date']
        id1 = record['identifier1']
        id1 = ''.join(id1.split('-')[:-1])
        

        # from the candidates (all having same issuer as the record)
        # find the ones with the same listing date
        candicates = self.data_df[self.data_df['issuer_name'] == issuer_name].copy()
        filtered = candicates.loc[candicates['listing_date'] == listing_date, :].copy()
        filtered['id'] = filtered['identifier1'].apply(lambda x: ''.join(x.split('-')[:-1])).copy()
        one_deal = filtered.loc[filtered['id'] == id1, :].drop(['id'], axis=1)
        
        self.parsed_history.extend(list(one_deal['isin']))
        return one_deal


    def parse_one_deal(self, record):
        issuer_id = self.du.get_company_info(record['issuer_name'])
        if issuer_id < 0:
            self.note.append(f"cannot find issuer comanpy id {record['issuer_name']}")
        
        self.deal_dict['IssuerId'] = issuer_id
        self.deal_dict['IsInvestmentGrade'] = True # by default
        self.deal_dict['NationalityId'] = 119  # for Korea
        self.deal_dict['NationalityOfRiskIds'] = [119]  # for China
        self.deal_dict['GeneralMarketId'] = 20 # Non-US Domestic by default
        self.deal_dict['UseOfProceedsIds'] = [110]  # GCP default
        self.deal_dict['PricingStatusId'] = 1 # by default
        self.deal_dict['AnnouncementComplexDate'] = {
            "TypeId": 1, "Date": self.du.parse_ann_price_date(record['settlement_date'])}
        self.deal_dict['PricingComplexDate'] = {
            "TypeId": 1, "Date": self.du.parse_ann_price_date(record['settlement_date'])}
        self.deal_dict['SettlementComplexDate'] = {
            "TypeId": 1, "Date": self.du.parse_date(record['settlement_date'])}

        principal_paying_agency_id = self.du.get_company_info(record['principal_paying_agency'])
        if principal_paying_agency_id < 0:
            self.note.append(f"cannot find principal_paying_agency_id {record['principal_paying_agency']}")
        
        tranche_list = []
        tranche_df = self.find_tranches(record)
        for _, row in tranche_df.iterrows():
            tranche_dict = self.parse_one_tranche(row)
            tranche_list.append(tranche_dict)

        self.deal_dict['Tranches'] = tranche_list
        


    def parse_one_tranche(self, record):
        is_float_rate = self.du.is_float(record['special_issuance_conditions'])
        is_zero_coupon = pd.isnull(record['initial_interest_payment_date'])

        tranche_dict = self.tranche_format.copy()
        tranche_dict['MarketTypeId'] = 1 # Domestic market public issue by default
        tranche_dict['IssueTypeId'] = self.du.parse_issue_type(record['special_issuance_conditions'])
        tranche_dict['CharacteristicIds'] = None # default
        tranche_dict['IsInternationalMarket'] = False

        if is_zero_coupon:
            tranche_dict['FirstCouponDate'] = None
        else:
            tranche_dict['FirstCouponDate'] = self.du.parse_date(record['initial_interest_payment_date'])

        tranche_dict['InterestAccrualDate'] = self.du.parse_date(record['settlement_date'])
        tranche_dict['MaturityComplexDate'] = self.du.parse_mat_date(record['maturity_date'], record['coupon_rate'])

        tranche_dict['BearerRegisteredId'] = 3  # registered by default
        tranche_dict['SeniorityId'] = self.du.parse_seniority(record['guarantee_collateral'])[0]
        tranche_dict['IsSubordinated'] = not self.du.parse_seniority(record['guarantee_collateral'])[1]
        tranche_dict['IsFungible'] = False  # by default
        tranche_dict['IsCollateralized'] = False

        tranche_dict['CurrencyId'] = 89 # Korean Currency by default
        tranche_dict['CurrencyAmount'] = self.du.parse_money(record['issue_size'])
        tranche_dict['IssuePricePercentage'] = 100  # by default
        tranche_dict['RedemptionPricePercentage'] = 100  # by default
        
        if is_float_rate:
            tranche_dict['CouponSetFrequencyId'] = self.du.parse_cp_freq(record['interest_calculation_cycle_months'], 
                                                                         record['initial_interest_payment_date'])
            tranche_dict['BasisId'] = 7
            tranche_dict['MarginPercentage'] = self.du.parse_margin_rate(record['special_issuance_conditions'])
            tranche_dict['DiscountMarginBasisPoints'] = self.du.parse_margin_rate(record['special_issuance_conditions'])*100
            tranche_dict['CouponPercentage'] = None
        else:
            tranche_dict['CouponPercentage'] = round(float(record['coupon_rate']), 2)
            tranche_dict['CouponFrequencyId'] = self.du.parse_cp_freq(record['interest_calculation_cycle_months'], 
                                                                    record['initial_interest_payment_date'])
        tranche_dict['ISINs'] = [record['isin']]
        tranche_dict['GoverningLawIds'] = [681] # by default
        tranche_dict['ListingIds'] = [122] # by default

        tranche_dict['Syndicate'] = self.parse_syndicate(record)


        # bkrs = record['bookrunners']



        return tranche_dict

        #print(self.tranche_dict)

    
    def parse_one_syndicate(self, bk_name, bk_role, bk_part):
        syndicate_dict = self.syndicate_format.copy()

        if bk_role == 'bookrunner':
            title_id = 2
        elif bk_role == 'comanager':
            title_id = 8

        
        participation = bk_part
        
        syndi_id = self.du.get_company_info(bk_name)
        if syndi_id < 0:
            self.note.append(f'cannot find syndicate bank id {bk_name}')
        syndicate_dict['BankTitleIds'] = [title_id]
        syndicate_dict['Id'] = syndi_id
        syndicate_dict['Participation'] = participation


        return syndicate_dict

    def parse_no_syndicate(self):
        syndicate_dict = self.syndicate_format.copy()
        syndicate_dict['Id'] = 141783 # ID for no bookrunner
        syndicate_dict['BankTitleIds'] = [2]

        return syndicate_dict


    def parse_syndicate(self, record):
        syndicate = []

        if record['bookrunners'] == []:
            self.note.append('no original book runner info')
            return self.parse_no_syndicate() 

        if record['comanagers'] == []:
            cmgrs = []
            cmgr_parts = []
        else:
            cmgrs = record['comanagers']
            cmgr_parts_str =  ast.literal_eval(record['cmgr_parts'])
            cmgr_parts = list(map(lambda x: round(int(x.replace(',', '')) / 1000000, 2) if x != '-' else 0, cmgr_parts_str))


        bkrs = record['bookrunners']
        bkr_parts_str =  ast.literal_eval(record['bkr_parts'])
        bkr_parts = list(map(lambda x: round(int(x.replace(',', '')) / 1000000, 2) if x != '-' else 0, bkr_parts_str))

        issue_size = self.du.parse_money(record['issue_size'])

        is_matched = self.du.check_participation(issue_size, bkr_parts, cmgr_parts)
        if is_matched == False:
            self.note.append('participation value not matched with issue size')


        bkr_dict = {name: part for name, part in zip(bkrs, bkr_parts)}
        if len(cmgrs) > 0:
            cmgr_dict = {name: part for name, part in zip(cmgrs, cmgr_parts)}
        else:
            cmgr_dict = {}

        for name, part in bkr_dict.items():
            if is_matched == False:
                part = None
            one_synd = self.parse_one_syndicate(name, 'bookrunner', part)
            syndicate.append(one_synd)

        if len(cmgr_dict) > 0:
            for name, part in cmgr_dict.items():
                if is_matched == False:
                    part = None
                one_synd = self.parse_one_syndicate(name, 'comanager', part)
                syndicate.append(one_synd)

        # part_sum = sum(all_parts)
        # issue_size = self.du.parse_money(record['issue_size'])
        # is_matched = (part_sum == issue_size)


        return syndicate
