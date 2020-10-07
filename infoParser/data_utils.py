import pandas as pd 
import json
import re
import ast


def _clean_bkr(bkr):
    bkr = re.sub(r'\n+', '', bkr)
    if bkr == '[]' or bkr == "['[]']":
        cleaned = '[]'
    else:
        bkr = ast.literal_eval(bkr)
        cleaned = list(map(lambda x: re.sub(r"㈜|\n+|[\(\[].*?[\)\]]", "", x), bkr))
    
    print(cleaned)
    return cleaned


def load_bkr_df(fp):
    bkr_df = pd.read_csv(fp)
    bkr_df['발행기관명'] = bkr_df['발행기관명'].apply(lambda x: re.sub(r'[\(\[].*?[\)\]]', "", x))
    bkr_df['bookrunners'] = bkr_df['bookrunners'].apply(lambda x: _clean_bkr(x))
    
    with open('./dependencies/kr_eng_translation.json', encoding='UTF-8-sig') as js_file:
        translation = json.load(js_file)
    
    translated_df = bkr_df.rename(columns=translation)

    translated_df.to_csv('./data/translated_bkr_df.csv')

    return translated_df 


