import scraper.general_info as gi 
import scraper.bookrunner as bk 

if __name__ == "__main__":
    start_date = 20190930
    end_date = 20200930

    isin_fp = './data/korea.xls'
    general_info_fp = './data/gi_df.csv'
    bkr_fp = './data/bkr_df.csv'

    gi_scrapper = gi.Scrapper()
    gi_scrapper.scrap_list_of_deals(isin_fp, general_info_fp)
    gi_scrapper.driver.quit()


    sorted_gi_df = bk.preprocess(general_info_fp)
    bkr_df = bk.search_bookrunner(sorted_gi_df)
    bkr_df.to_csv(bkr_fp, index=False)
 