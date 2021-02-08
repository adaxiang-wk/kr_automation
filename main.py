import src.scraper.general_info as gi 
import src.scraper.bookrunner as bk

import src.infoParser.info_parser as ipsr
import src.infoParser.data_utils as tools
import src.infoParser.main_parsing as mp

import argparse


input_fp = './data/dataframe/input.xlsx'
driver_path = './dependencies/chromedriver'
headless = True


def get_info(start_date, end_date):
    print("Getting info from KRX ......")
    gi_scrapper = gi.Scrapper(driver_path, start_date=start_date, end_date=end_date, headless=headless)
    gi_scrapper.scrap_list_of_deals(input_fp, general_info_fp)
    gi_scrapper.driver.quit()

    print("Getting bookrunner info from DART ......")
    sorted_gi_df = bk.preprocess(general_info_fp, sort=True, save=True)
    bk.search_bookrunner(sorted_gi_df, driver_path=driver_path, save_fp=bkr_fp, headless=headless)


def get_br():
    sorted_gi_df = bk.preprocess(general_info_fp, sort=True, save=True)
    bk.search_bookrunner(sorted_gi_df, driver_path=driver_path, save_fp=bkr_fp, headless=headless)
    # bkr_df.to_csv(bkr_fp, index=False)


def parse_one(isin, env):
    my_parser = ipsr.Parser(bkr_fp, env_type=env)
    df = my_parser.data_df
    record = df.loc[df['isin'] == isin, :].iloc[0]
    my_parser.parse_one_deal(record)
    parsed = my_parser.deal_dict
    tools.json_dumper(parsed, f'{isin}.json')
    
    print(isin)


def parse_batch(env):
    mp.parse_batch(bkr_fp, env_type=env, 
                   output_dir=json_dir, 
                   log_fp=parse_log_fp)


def post_one(isin, env):
    mp.post_one_deal(isin, json_dir, env_type=env)


def post_batch(env):
    mp.post_batch(json_dir, env_type=env, post_log=post_log_fp)

def parse_post_one(isin, env):
    mp.parse_post_one(bkr_fp, isin, env, save_fp=json_dir)

def parse_post_batch(env):
    mp.parse_post_batch(bkr_fp, env_type=env, parse_log=parse_log_fp, post_log=post_log_fp, save_fp=json_dir)


if __name__ == "__main__":
    action_map = {
        'getinfo':get_info,
        'parse_one':parse_one, 'parse_batch':parse_batch, 
        'post_one':post_one, 'post_batch':post_batch,
        'parse_post_one': parse_post_one, 'parse_post_batch':parse_post_batch
    }
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-action', type=str, required=True, help=f'choose an action from {action_map.keys()}')
    arg_parser.add_argument('-date', nargs=2, type=int, required=False, help=f'start and end date')
    arg_parser.add_argument('-isin', type=str, required=False, help='isin the deal')
    arg_parser.add_argument('-env', type=str, required=False, help='env for parsing and uploading')

    args = arg_parser.parse_args()

    parse_log_fp = f'./logs/{args.env}_parse_log.csv'
    post_log_fp = f'./logs/{args.env}_post_log.csv'
    json_dir = f'./data/json/{args.env}_json'

    general_info_fp = './data/dataframe/general_df.csv'
    bkr_fp = './data/dataframe/final_df.csv'

    if args.action not in action_map.keys():
        raise ValueError("No such action available")

    if args.action == 'getinfo':
        if args.date is None:
            raise ValueError("No date passed")
        else:
            action_map[args.action](args.date[0], args.date[1])
    elif 'one' in args.action:
        if args.env is None:
            raise ValueError("no env specified")
        if args.isin is None:
            raise ValueError("No isin passed")
        else:
            action_map[args.action](args.isin, args.env)
    else:
        if args.env is None:
            raise ValueError("no env specified")
        action_map[args.action](args.env)

    

    

    

    # gi_scrapper = gi.Scrapper()
    # gi_scrapper.scrap_list_of_deals(input_fp, general_info_fp)
    # gi_scrapper.driver.quit()


    # sorted_gi_df = bk.preprocess(general_info_fp, sort=True, save=False)
    # bkr_df = bk.search_bookrunner(sorted_gi_df)
    # bkr_df.to_csv(bkr_fp, index=False)
 