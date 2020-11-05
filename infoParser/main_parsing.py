import infoParser.data_utils as du
import infoParser.info_parser as ipsr 
import envSetter.env_config as ec

import os
import json

############### parse info into json ############################
def parse_one_deal(data_fp, env_type, deal_idx):
    my_parser = ipsr.Parser(data_fp, env_type=env_type)
    record = my_parser.data_df.iloc[deal_idx, :]
    my_parser.parse_one_deal(record)
    parsed = my_parser.deal_dict

    isin = record['isin']
    du.json_dumper(parsed, f'./data/json/pie_json/{isin}.json')
    
    print(f'parsed {isin}')


def parse_batch(data_fp, env_type, output_dir, log_fp):
    my_parser = ipsr.Parser(data_fp, env_type=env_type)
    df = my_parser.data_df.reset_index()
    # df = df.iloc[463:, :]

    print(f'{df.shape[0]} deals to parse')
    for idx, record in df.iterrows():
        isin = record['isin']
        saved_fp = os.path.join(output_dir, f'{isin}.json')
        if os.path.exists(saved_fp):
            continue

        if isin in my_parser.parsed_history:
            continue

        print(f"Parsing {isin}")
        parsed_isins = my_parser.parse_one_deal(record)
        parsed = my_parser.deal_dict

       
        du.json_dumper(parsed, saved_fp)
        du.parse_loging(parsed_isins, my_parser.note, idx, log_fp)
        print(my_parser.note)
        print(f'parsed {parsed_isins}, {df.shape[0] - idx -1} left')
        my_parser.note = []


############################ post json #############################
def post_one_deal(isin, json_fp, env_type):
    before_post = os.path.join(json_fp, f'{isin}.json')
    post_env = ec.EnvConfig(env_type=env_type, section='dcm')
    notes = ''

    with open(before_post) as f:
        data_json = json.load(f)
        json_str = json.dumps(data_json)

    deal_number = 0
    req_post = post_env.auth_app.post(f'{post_env.POST_URL}', json_str)
    if req_post.status_code == 200 or req_post.status_code == 201:
        deal_number = req_post.content.decode("utf-8")
        after_post = os.path.join(json_fp, f'{isin}_{deal_number}.json')
        os.rename(before_post, after_post)

        success_info = f'{after_post} is posted'
        print(success_info)
    else:
        notes = str(req_post) + ' ' + str(req_post.content)
        print(notes)

    return notes, deal_number


def post_batch(json_fp, env_type, parse_log, post_log):
    json_files = os.listdir(json_fp)

    for idx, f in enumerate(json_files):
        if '_' in f:
            continue

        isin = f.split('.')[0]
        print(f'posting {isin} ...')
        notes, deal_num = post_one_deal(isin, json_fp, env_type=env_type)

        # if idx == 0:
        #     parse_log = parse_log
        # else:
        #     parse_log = post_log
        du.post_loging(isin, deal_num, notes, parse_log, post_log)

        # if idx == 5:
        #     break


############################ update #############################
def update_one_deal():
    pass