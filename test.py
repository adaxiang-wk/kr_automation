import infoParser.data_utils as du
import re
import ast

if __name__ == "__main__":
    bkr_fp = './data/bkr_df.csv'
    du.load_bkr_df(bkr_fp)

    # bkrs = u"['NH투자증권㈜', '한국투자증권㈜', 'KB증권㈜']"
    # bkrs = ast.literal_eval(bkrs)
    # print(list(map(lambda x: re.sub(r"㈜|\n+|[\(\[].*?[\)\]]", "", x), bkrs)))
