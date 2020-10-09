import infoParser.data_utils as du
import infoParser.info_parser as ipsr 


def parse_one_deal(data_fp, env_type, deal_idx):
    my_parser = ipsr.Parser(data_fp, env_type=env_type)
    record = my_parser.data_df.iloc[deal_idx, :]
    my_parser.parse_one_deal(record)
    parsed = my_parser.deal_dict

    isin = record['isin']
    du.json_dumper(parsed, f'./data/json/pie_json/{isin}.json')
    
    print(f'parsed {isin}')


def parse_batch(data_fp, env_type):
    my_parser = ipsr.Parser(data_fp, env_type=env_type)
    df = my_parser.data_df.reset_index()

    print(f'{df.shape[0]} deals to parse')
    for idx, record in df.iterrows():
        
        my_parser.parse_one_deal(record)
        parsed = my_parser.deal_dict
        notes = my_parser.note

        isin = record['isin']
        du.json_dumper(parsed, f'./data/json/pie_json/{isin}.json')
        du.loging(isin, notes, idx)
        print(f'parsed {isin}, {df.shape[0] - idx -1} left')

