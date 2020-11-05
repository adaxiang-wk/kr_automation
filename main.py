import scraper.general_info as gi 
import scraper.bookrunner as bk

import infoParser.info_parser as ipsr
import infoParser.data_utils as tools
import infoParser.main_parsing as mp

import argparse

DATA_ENV = 'prd'
ACTION_ENV = 'pie'
# start_date = 20190930
# end_date = 20200930

isin_fp = './data/dataframe/Korea20200801_20201027.xls'
general_info_fp = './data/dataframe/gi_df_2.csv'
bkr_fp = './data/dataframe/bkr_df_2.csv'


def get_info(start_date, end_date):
    gi_scrapper = gi.Scrapper(start_date=start_date, end_date=end_date)
    gi_scrapper.scrap_list_of_deals(isin_fp, general_info_fp)
    gi_scrapper.driver.quit()


def get_br():
    sorted_gi_df = bk.preprocess(general_info_fp, sort=True, save=True)
    bkr_df = bk.search_bookrunner(sorted_gi_df, save_fp=bkr_fp)
    # bkr_df.to_csv(bkr_fp, index=False)


def parse_one(isin):
    my_parser = ipsr.Parser(bkr_fp, env_type=DATA_ENV)
    df = my_parser.data_df
    record = df.loc[df['isin'] == isin, :].iloc[0]
    my_parser.parse_one_deal(record)
    parsed = my_parser.deal_dict
    tools.json_dumper(parsed, f'{isin}.json')
    
    print(isin)


def parse_batch():
    mp.parse_batch(bkr_fp, env_type=DATA_ENV, 
                   output_dir='./data/json/pie_json3/', 
                   log_fp='./logs/parse_log3.csv')


def post_one(isin):
    mp.post_one_deal(isin, './data/json/pie_json2', env_type=ACTION_ENV)


def post_batch():
    mp.post_batch('./data/json/pie_json3', './logs/parse_log3.csv', is_new_log=True, env_type=ACTION_ENV)


if __name__ == "__main__":
    action_map = {
        'getinfo':get_info, 'getbr':get_br, 
        'parse_one':parse_one, 'parse_batch':parse_batch, 
        'post_one':post_one, 'post_batch':post_batch
    }
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-action', type=str, required=True, help=f'choose an action from {action_map.keys()}')
    arg_parser.add_argument('-date', nargs=2, type=int, required=False, help=f'start and end date')
    arg_parser.add_argument('-isin', type=str, required=False, help='isin the deal')

    args = arg_parser.parse_args()
    if args.action not in action_map.keys():
        raise ValueError("No such action available")

    if args.action == 'getinfo':
        if args.date is None:
            raise ValueError("No date passed")
        else:
            action_map[args.action](args.date[0], args.date[1])
    elif 'one' in args.action:
        if args.isin is None:
            raise ValueError("No isin passed")
        else:
            action_map[args.action](args.isin)
    else:
        action_map[args.action]()

    

    

    

    # gi_scrapper = gi.Scrapper()
    # gi_scrapper.scrap_list_of_deals(isin_fp, general_info_fp)
    # gi_scrapper.driver.quit()


    # sorted_gi_df = bk.preprocess(general_info_fp, sort=True, save=False)
    # bkr_df = bk.search_bookrunner(sorted_gi_df)
    # bkr_df.to_csv(bkr_fp, index=False)
 