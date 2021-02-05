import requests
from bs4 import BeautifulSoup
from selenium import webdriver
import time
import pandas as pd
import os
from tqdm import tqdm
from selenium.webdriver.chrome.options import Options

class Scrapper:
    def __init__(self, driver_path, start_date=20190920, end_date=20200920, headless=True):
        if headless:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            self.driver = webdriver.Chrome(executable_path=driver_path, options=chrome_options)
        else:
            self.driver = webdriver.Chrome(executable_path=driver_path)
        self.base_url = 'https://isin.krx.co.kr/srch/srch.do?method=srchList'
        self.start_date = start_date
        self.end_date = end_date


    def search_by_isin(self, isin):
        self.driver.get(self.base_url)
        # fill the forms
        isin_input = self.driver.find_element_by_id("isur_nm1")
        isin_input.clear()
        isin_input.send_keys(isin)

        start_date_input = self.driver.find_element_by_id("std_cd_grnt_start_dd")
        start_date_input.clear()
        start_date_input.send_keys(self.start_date)

        end_date_input = self.driver.find_element_by_id("std_cd_grnt_end_dd")
        end_date_input.clear()
        end_date_input.send_keys(self.end_date)

        # submit & search
        self.driver.find_element_by_link_text('조회').click()
        time.sleep(2)
        window_before = self.driver.window_handles[0]

        # detailed table
        self.driver.find_element_by_link_text(isin).click()
        time.sleep(2)
        window_after = self.driver.window_handles[1]
        return window_before, window_after


    def scrap_table(self, soup):
        cell_names = list(map(lambda x: x.text, soup.find_all('th')))
        cell_vals = list(map(lambda x: x.text, soup.find_all('td')))
        cell_dict = {name:[val] for name, val in zip(cell_names, cell_vals)}
        df = pd.DataFrame.from_dict(cell_dict)
        return df

    def scrap_call_table(self, soup):
        cell_names = list(map(lambda x: x.text, soup.find_all('th')))[:2]
        cell_vals = list(map(lambda x: x.text, soup.find_all('td')))[:2]
        cell_dict = {name:[val] for name, val in zip(cell_names, cell_vals)}
        df = pd.DataFrame.from_dict(cell_dict)
        return df


    def scrap_one_deal(self, isin):
        _, popup_table = self.search_by_isin(isin)
        self.driver.switch_to.window(popup_table)
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        tables = soup.find_all(lambda tag: tag.name == 'table' and 
                                   len(list(tag.attrs.keys())) == 2 and
                                   'summary' in list(tag.attrs.keys()))
        dfs = []
        for table in tables:
            df = self.scrap_table(table)
            dfs.append(df)

        call_table = soup.find('table', {'id':'id_call_option'})
        if call_table is not None:
            call_df = self.scrap_call_table(call_table)
            dfs.append(call_df)

        final_df = pd.concat(dfs, axis=1)
        return final_df



    def scrap_list_of_deals(self, file_fp, saving_path):
        xls = pd.ExcelFile(file_fp)
        data = pd.read_excel(xls, 'Sheet1')

        isin_list = list(data['ISIN'])
        # print(f'Total {len(isin_list)} records to scrap')

        dfs = []
        saved_isins = []
        if os.path.exists(saving_path):
            log_df = pd.read_csv(saving_path)
            dfs.append(log_df)

            saved_isins = list(log_df['표준코드'])

        isin_list = [isin for isin in isin_list if isin not in saved_isins]
        for _, isin in tqdm(enumerate(isin_list), total=data.shape[0], initial=data.shape[0]-len(isin_list)):
            if len(saved_isins) > 0:
                if isin in saved_isins:
                    continue

            df = self.scrap_one_deal(isin)
            dfs.append(df)
            
            saved_isins.append(isin)

            # print(f'Scraped {isin}, {len(isin_list) - idx -1} left')

            # if idx % 10 == 0:
            final_df = pd.concat(dfs, axis=0)
            final_df.to_csv(saving_path, index=False)

        final_df = pd.concat(dfs, axis=0)
        final_df.to_csv(saving_path, index=False)



# if __name__ == "__main__":
#     start_date = 20190930
#     end_date = 20200930
#     # isin = 'KR6000012A90'
#     input_fp = './data/korea.xls'
#     output_fp = './data/final_df.csv'

#     my_scrapper = Scrapper()
#     my_scrapper.scrap_list_of_deals(input_fp, output_fp)
#     my_scrapper.driver.quit()
#     # my_scrapper.scrap_one_deal(isin)


    """
    self.driver.switch_to.window(window_after)
    soup = BeautifulSoup(self.driver.page_source, 'html.parser')
    table_htmls = soup.find_all('table', {'summary': "상세조회"})
    table_element = self.driver.find_element_by_xpath(f"//table[@summary='{table_name}']")
    table_html = table_element.get_attribute('outerHTML')
    """