import pandas as pd 
import json

def load_bkr_df(fp):
    bkr_df = pd.read_csv(fp)
    
    with open('./dependencies/kr_eng_translation.json', encoding='UTF-8-sig') as js_file:
        translation = json.load(js_file)
    
    translated_df = bkr_df.rename(columns=translation)

    translated_df.to_csv('./data/translated_bkr_df.csv')

    return translated_df