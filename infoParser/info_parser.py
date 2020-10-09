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
        
        self.du = tools.Tool(env_type=env_type)
        self.data_df = self.du.load_bkr_df(data_fp)


    def find_tranches(self, record):
        issuer_name = record['issuer_name']
        listing_date = record['listing_date']

        # from the candidates (all having same issuer as the record)
        # find the ones with the same listing date
        candicates = self.data_df[self.data_df['issuer'] == issuer_name].copy()
        one_deal = candicates.loc[candicates['listing_date'] == listing_date, :]
        
        self.parsed_history.extend(list(one_deal['local_symbol']))
        return one_deal


    def parse_one_deal(self, record):
        self.deal_dict['IssuerId'] = self.du.get_company_info(record['issuer_name'])
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
        

        
        # tranche_list = []
        # tranche_df = self.find_tranches(record)
        # for _, row in tranche_df.iterrows():
        #     tranche_dict = self.parse_one_tranche(row)

        #     syndicate = record['bookrunners']
        #     if syndicate == "['']" or syndicate == "[]":
        #         tranche_dict['Syndicate'] = [self.parse_no_syndicate()]
        #     else:
        #         bookrunners = ast.literal_eval(syndicate)
        #         syn_list = []
        #         bkr_history = []
        #         for bkr_name in bookrunners:
        #             if bkr_name in bkr_history:
        #                 continue
        #             syn = self.parse_syndicate(bkr_name, record)
        #             if syn is not None:
        #                 syn_list.append(syn)
        #             bkr_history.append(bkr_name)
        #         if len(syn_list) > 0:
        #             tranche_dict['Syndicate'] = syn_list
        #         else:
        #             tranche_dict['Syndicate'] = [self.parse_no_syndicate()]
        #     tranche_list.append(tranche_dict)
        # self.deal_dict['Tranches'] = tranche_list
        


    def parse_one_tranche(self, record):
        tranche_dict = self.tranche_format.copy()
        tranche_dict['MarketTypeId'] = 1 # Domestic market public issue by default
        tranche_dict['IssueTypeId'] = self.du.parse_issue_type() # default TODO
        tranche_dict['CharacteristicIds'] = None # default
        tranche_dict['IsInternationalMarket'] = False

        tranche_dict['FirstCouponDate'] = self.du.parse_date(record['initial_interest_payment_date'])
        tranche_dict['InterestAccrualDate'] = self.du.parse_date(record['settlement_date'])
        tranche_dict['MaturityComplexDate'] = self.du.parse_mat_date(record['maturity_date'], record['coupon_rate'])

        tranche_dict['BearerRegisteredId'] = 3  # registered by default
        tranche_dict['SeniorityId'] = self.du.parse_seniority(record['guarantee_collateral'])[0]
        tranche_dict['IsSubordinated'] = not self.du.parse_seniority(record['guarantee_collateral'])[1]
        tranche_dict['IsFungible'] = False  # by default
        tranche_dict['IsCollateralized'] = False
        tranche_dict['CurrencyId'] = 89 # Yuan by default
        tranche_dict['CurrencyAmount'] = self.du.parse_issue_size(record['issue_size'])
        tranche_dict['IssuePricePercentage'] = 100  # by default
        tranche_dict['RedemptionPricePercentage'] = 100  # by default
        # self.tranche_dict['DayCountId'] 
        tranche_dict['CouponPercentage'] = 0
        tranche_dict['CouponFrequencyId'] = self.du.parse_cp_freq(record['interest_calculation_cycle_months'])
        tranche_dict['GoverningLawIds'] = [681] # by default
        tranche_dict['ListingIds'] = [122] # by default

        return tranche_dict

        #print(self.tranche_dict)

        
    def parse_no_syndicate(self):
        syndicate_dict = self.syndicate_format.copy()
        syndicate_dict['Id'] = 141783 # ID for no bookrunner
        syndicate_dict['BankTitleIds'] = [2]

        return syndicate_dict

    
    def parse_syndicate(self, bkr_name, record):
        syndicate_dict = self.syndicate_format.copy()
        syndicate_dict['BankTitleIds'] = [2]
        syndi_id = du.get_company_info(bkr_name, record['name'], record['bond_type'])
        if syndi_id > 0:
            syndicate_dict['BankTitleIds'] = [2]
            syndicate_dict['Id'] = syndi_id
        else:
            syndicate_dict = None


        return syndicate_dict




    def get_info(self, info_type):
        """
        info_type should be string: 
            - 'deal'
            - 'tranche'
            - 'syndicate'
        """
        deal = self.deal_dict
        tranche = deal['Tranches']

        if info_type == 'deal':
            return deal
        elif info_type == 'tranche':
            return tranche
        else:
            raise ValueError('Wrong info type')


    def get_parsed_history(self):
        # print('HISTORY ', self.parsed_history)
        return self.parsed_history



if __name__ == "__main__":
    ### Test 
    format_fp = './dependencies/post_format.json'
    data_fp = './testing_files/bonds_testing_v1.csv'
    my_parser = Parser(format_fp=format_fp, data_fp=data_fp, file_type='csv')
    # my_parser.parse_one_deal(my_parser.data_df.iloc[17, :])

    # print((my_parser.data_df.loc[1, ['mat_date']].isnull()))
    # print(my_parser.data_df.columns)
    # my_parser.parse_one_deal(my_parser.data_df.iloc[1, :])
    # print(my_parser.parse_one_tranche(my_parser.data_df.iloc[0, :]))
    deals = my_parser.data_df
    for idx, deal in deals.iterrows():
        # print(my_parser.parsed_history)
        if deal['local_symbol'] in my_parser.parsed_history:
            continue
        tranches = my_parser.find_tranches(deal)
        if tranches.shape[0] > 1:
            print(tranches)