import infoParser.data_utils as du
import infoParser.info_parser as ipsr 
import envSetter.env_config as ec

import os
import json
import pandas as pd

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
    df = df.iloc[552:, :]

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
def post_one_deal(isin, json_fp, env_type, data=None):
    
    post_env = ec.EnvConfig(env_type=env_type, section='dcm')
    notes = ''

    if data is None:
        before_post = os.path.join(json_fp, f'{isin}.json')
        with open(before_post) as f:
            data_json = json.load(f)
            json_str = json.dumps(data_json)
    else:
        json_str = data

    deal_number = 0
    req_post = post_env.auth_app.post(f'{post_env.POST_URL}', json_str)
    if req_post.status_code == 200 or req_post.status_code == 201:
        deal_number = req_post.content.decode("utf-8")
        success_info = f'{isin}_{deal_number} is posted'
        print(success_info)

        if len(json_fp) > 0:
            after_post = os.path.join(json_fp, f'{isin}_{deal_number}.json')
            os.rename(before_post, after_post)

    else:
        notes = str(req_post) + ' ' + str(req_post.content)
        print(notes)

        if len(json_fp) == 0:
            failed_fp = './data/json/failed_pie'
            if not os.path.exists(failed_fp):
                os.mkdir(failed_fp)
            with open(os.path.join(failed_fp, f'{isin}.json'), 'w') as f:
                json.dump(json_str, f)

    return notes, deal_number


def parse_post_one(data_fp, isin, env_type, save_fp=''):
    my_parser = ipsr.Parser(data_fp, env_type=env_type)
    df = my_parser.data_df
    record = df.loc[df['isin'] == isin, :].iloc[0]
    my_parser.parse_one_deal(record)
    parsed = my_parser.deal_dict

    if len(save_fp) > 0:
        data = du.json_dumper(parsed, os.path.join(save_fp, f'{isin}.json'))
    else:
        data = du.json_dumper(parsed)
    
    post_one_deal(isin, json_fp=save_fp, env_type=env_type, data=data)


def parse_post_batch(data_fp, env_type, parse_log, post_log, save_fp):
    my_parser = ipsr.Parser(data_fp, env_type=env_type)
    df = my_parser.data_df.reset_index()

    for idx, record in df.iterrows():
        isin = record['isin']
        
        if os.path.exists(parse_log):
            parse_df = pd.read_csv(parse_log)
            parse_isins = list(parse_df['isin'])
            if isin in parse_isins:
                continue

        if isin in my_parser.parsed_history:
            continue

        print(f"Parsing {isin}")
        parsed_isins = my_parser.parse_one_deal(record)
        parsed = my_parser.deal_dict
        du.parse_loging(parsed_isins, my_parser.note, idx, parse_log)

        data = du.json_dumper(parsed)
        post_notes, deal_number = post_one_deal(isin, json_fp='', env_type=env_type, data=data)
        du.post_loging2(parsed_isins, post_notes, deal_number, idx, post_log)

        if len(save_fp) > 0:
            if deal_number != 0:
                file_name = f'{isin}_{deal_number}.json'
            else:
                file_name = f'{isin}.json'
            with open(os.path.join(save_fp, file_name), 'w') as f:
                json.dump(data, f)

        

        print(my_parser.note)
        print(f'parsed {parsed_isins}, {df.shape[0] - idx -1} left')
        my_parser.note = []


def post_batch(json_fp, env_type, parse_log, post_log):
    json_files = os.listdir(json_fp)
    origin_post_log = post_log

    for idx, f in enumerate(json_files):
        if '_' in f:
            continue

        isin = f.split('.')[0]
        print(f'posting {isin} ...')
        notes, deal_num = post_one_deal(isin, json_fp, env_type=env_type)

        if idx == 0:
            post_log = parse_log
        else:
            post_log = origin_post_log
        
        du.post_loging(isin, deal_num, notes, parse_log, post_log)

        # if idx == 15:
        #     break



############################ update #############################
def update_rank(deal_num, env_type):
    print(deal_num)
    update_env = ec.EnvConfig(env_type, 'dcm')
    url = update_env.GET_BASE_URL
    data_dict = update_env.auth_app.get(url+str(deal_num)).json()
    
    tranches = data_dict['Tranches']
    for idx, tranche in enumerate(tranches):
        tranche.update({'IsRankEligible': True})
        tranches[idx] = tranche

    data_dict.update({'Tranches': tranches})
    new_json = du.json_dumper(data_dict)

    post_url = update_env.PUT_BASE_URL+str(deal_num)
    resp = update_env.auth_app.put(post_url, new_json)

    print(resp.status_code, resp.content)


def update_tranches_from_file(deal_num, fp, env_type):
    update_env = ec.EnvConfig(env_type, 'dcm')
    url = update_env.GET_BASE_URL
    data_dict = update_env.auth_app.get(url+str(deal_num)).json()

    with open(fp) as json_file:
        new_data = json.load(json_file)

    new_tranches = new_data['Tranches']
    data_dict.update({'Tranches': new_tranches})

    new_json = du.json_dumper(data_dict)
    post_url = update_env.PUT_BASE_URL+str(deal_num)
    resp = update_env.auth_app.put(post_url, new_json)

    print(resp.status_code, resp.content)



