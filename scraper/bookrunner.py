import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import selenium
import time
import pandas as pd
import sys
import pandas as pd 
from datetime import datetime, timedelta
import re

th_list = ['명칭', '주소', '합계', '인수인', '인수금액및수수료율', '인수조건', '고유번호', '인수금액', '수수료율']


class Scrapper:
    def __init__(self):
        driver_path = r'/Users/AdaXiang/geckodriver'
        self.driver = webdriver.Firefox(executable_path=driver_path)
        self.base_url = 'http://dart.fss.or.kr/'
        self.search_base_url = 'http://dart.fss.or.kr/dsab007/main.do'


    def find_prospectus(self, identifier):
        id1 = identifier[0]
        id2 = identifier[1]
        main_window = self.driver.current_window_handle
        try:
            prospectus = self.driver.find_elements_by_partial_link_text('투자설명서')

            found_p = False
            for p in prospectus:
                p.click()

                time.sleep(3)
                report_page = self.driver.window_handles[1]
                self.driver.switch_to.window(report_page)

                side_panel = self.driver.find_elements_by_id('west-panel')
                while len(side_panel) == 0:
                    self.driver.refresh()
                    side_panel = self.driver.find_elements_by_id('west-panel')

                # pros_link = p.get_attribute('href')
                # self.driver.get(pros_link)

                main_info_pages = self.driver.find_elements_by_partial_link_text('모집 또는 매출에 관한 사항')
                if len(main_info_pages) == 0:
                    print('no general info table in prosepectus file')
                    self.driver.close()
                    self.driver.switch_to.window(main_window)
                    continue
                else:

                    main_info_pages[0].click()
                    time.sleep(1)
                    iframes = self.driver.find_elements_by_tag_name('iframe')

                    if len(iframes) > 0:
                        iframe = iframes[0]
                        self.driver.switch_to.frame(iframe)
                    source = self.driver.page_source
                    # soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                    # found = soup.find_all(lambda tag: id1 in tag.text)
                    

                    if (id1 in source) or (id2 in source):
                        found_p = True
                        print(f'Found prospectus with {id1}')
                        break
                    
                    self.driver.close()
                    self.driver.switch_to.window(main_window)


            if found_p == True:
                self.driver.switch_to.window(report_page)
                self.driver.find_elements_by_partial_link_text('모집 또는 매출에 관한 일반사항')[-1].click()
                iframes = self.driver.find_elements_by_tag_name('iframe')

                if len(iframes) > 0:
                    iframe = iframes[0]
                    self.driver.switch_to_frame(iframe)
                source = self.driver.page_source
                self.driver.switch_to.window(main_window)
                return source
            else:
                print(f'no prospectus with {id1} found')
                return None
        except selenium.common.exceptions.NoSuchElementException:
            print('no prospectus file found')
            return None
            # try:
            #     prospectus = self.driver.find_element_by_partial_link_text('증권발행실적보고서')
            #     pros_link = prospectus.get_attribute('href')
            #     self.driver.get(pros_link)
            #     self.driver.find_element_by_partial_link_text('청약 및 배정에 관한 사항').click()
            # except selenium.common.exceptions.NoSuchElementException:
            #     print('no Securities Issuance Performance Report Found')


    def search_by_keyword(self, keyword, identifier):
        self.driver.get(self.base_url)
        # fill the forms
        keyword_input = self.driver.find_element_by_id("t_search")
        keyword_input.clear()
        keyword_input.send_keys(keyword)
        self.driver.find_element_by_xpath("//input[@class='btnTotalSearch']").click()

        report_html = self.find_prospectus(identifier)
        
        return report_html


    def search_by_company(self, company_name, start_date, end_date, identifier):
        print('searching...')
        self.driver.get(self.base_url)
        cpny_name_input = self.driver.find_element_by_id("textCrpNm")
        cpny_name_input.clear()
        cpny_name_input.send_keys(company_name)

        start_date_input = self.driver.find_element_by_id("startDate")
        start_date_input.clear()
        start_date_input.send_keys(start_date)

        end_date_input = self.driver.find_element_by_id("endDate")
        end_date_input.clear()
        end_date_input.send_keys(end_date)

        # self.driver.find_element_by_xpath("//select[@name='maxResultsCb']/option[text()='100']").click()
        # time.sleep(1)

        self.driver.find_element_by_xpath("//input[@class='ibtn']").click()
        time.sleep(2)

        try:
            chosen_cpy = self.driver.find_elements_by_id('checkCorpSelect')
            if len(chosen_cpy) > 0:
                chosen_cpy[0].click()
                enter_btn = self.driver.find_element_by_xpath("//a[@onclick='selectSearchCorp();return false;']")
                enter_btn.click()
                time.sleep(2)
                self.driver.find_element_by_xpath("//select[@name='maxResultsCb']/option[text()='100']").click()
                time.sleep(1)
                self.driver.find_elements_by_id('searchpng')[0].click()
                time.sleep(2)
                report_html = self.find_prospectus(identifier)
            else:
                
                report_html = self.find_prospectus(identifier)
        except selenium.common.exceptions.NoSuchElementException:
            report_html = self.find_prospectus(identifier)

        return report_html


    def search_by_keyword_and_company(self, keyword, company_name, start_date, end_date, identifier):
        self.driver.get(self.search_base_url)

        keyword_input = self.driver.find_element_by_id("keyword")
        keyword_input.clear()
        keyword_input.send_keys(keyword)

        cpny_name_input = self.driver.find_element_by_id("textCrpNm")
        cpny_name_input.clear()
        cpny_name_input.send_keys(company_name)

        start_date_input = self.driver.find_element_by_id("startDate")
        start_date_input.clear()
        start_date_input.send_keys(start_date)

        end_date_input = self.driver.find_element_by_id("endDate")
        end_date_input.clear()
        end_date_input.send_keys(end_date)

        self.driver.find_element_by_xpath("//input[@id='keywordBtn']").click()
        time.sleep(2)

        try:
            chosen_cpy = self.driver.find_elements_by_id('checkCorpSelect')
            if len(chosen_cpy) > 0:
                chosen_cpy[0].click()
                enter_btn = self.driver.find_element_by_xpath("//a[@onclick='selectSearchCorp();return false;']")
                enter_btn.click()
                time.sleep(2)
                self.driver.find_element_by_xpath("//input[@id='keywordBtn']").click()
                time.sleep(2)
                report_html = self.find_prospectus(identifier)
            else:
                report_html = self.find_prospectus(identifier)
        except selenium.common.exceptions.NoSuchElementException:
            report_html = self.find_prospectus(identifier)

        return report_html
        
        
    def get_bookrunner(self, report_html, tranche_num=0):
        soup = BeautifulSoup(report_html, 'html.parser')
        th = soup.find_all(lambda tag:tag.text == '인수인')

        if len(th) == 0:
            print('cannot find bookrunner tables in prospectus')
            return [[], []]
        elif len(th) == 1 and tranche_num > 0:
            print('not a tranche')
            return 'not a tranche'
        else:

            bookrunner_table = th[tranche_num].find_parent('table')
            table_body = bookrunner_table.find('tbody')
            all_records = table_body.find_all('tr')

            bkrs = []
            participates = []
            for record in all_records:
                bkr_cand = record.find_all('td')
                for idx, bkr in enumerate(bkr_cand[:3]):
                    txt = re.sub(r"[\n\t\s]*|[\(\[].*?[\)\]]","", bkr.text)
                    # txt = re.sub(r"[\(\[].*?[\)\]]", "", txt)
                    if (txt == '-') or (txt in th_list) or (len(txt) < 3):
                        continue
                    else:
                        bkrs.append(txt)
                        participate = bkr_cand[idx+3].text.replace(",", "")
                        participate = re.sub(r"[\n\t\s]*|[\(\[].*?[\)\]]","", participate)
                        participates.append(participate)
                        break
            
            return (bkrs, participates)




def extract_identifier(evt_num):
    candicates = evt_num.split(" ")

    identifier = ''
    for candidate in candicates:
        if candidate[0].isdigit():
            identifier = candidate
            break
    
    return re.sub(r"[\(\[].*?[\)\]]", "", identifier)


def detect_tranches(record, df):
    cpny_name = record['발행기관명']
    date = record['상장일']

    deal = df[(df['발행기관명'] == cpny_name) & (df['상장일'] == date)].reset_index()

    return deal


def preprocess(input_fp, sort=True):
    df = pd.read_csv(input_fp)
    df['identifier1'] = df['한글종목명'].apply(lambda x: extract_identifier(x))
    df['identifier2'] = df['identifier1'].apply(lambda x: x[3:])

    if sort:
        df = df.sort_values(by=['발행기관명', '상장일', 'identifier1']).reset_index()
    else:
        df = df.reset_index()

    # df.to_csv('./data/sorted_final_df.csv')

    return df
            
                
def search_bookrunner(df):
    print(f'Total {df.shape[0]} records')
    history = []
    syndicate = []
    synd_part = []
    for _, record in df.iterrows():
        if record['표준코드'] in history:
            continue
        my_scrapper = Scrapper()
        print(record['표준코드'])

        date_str = record['발행일']
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        start_date = date_obj - timedelta(days=4)
        startDate_str = start_date.strftime("%Y%m%d")

        end_time = date_obj + timedelta(days=3)
        endDate_str = end_time.strftime("%Y%m%d")

        deal = detect_tranches(record, df)
        # isin_log = list(deal['표준코드'])
        # history.extend(isin_log)

        company_name = re.sub(r"[\(\[].*?[\)\]]", "", record['발행기관명'])
        
        identifier = [record['identifier1'], record['identifier2']]

        report = my_scrapper.search_by_company(company_name, startDate_str, endDate_str, identifier)

        if report is not None:
            for tranch_idx, tranche in deal.iterrows():
                bookrunner_info = my_scrapper.get_bookrunner(report, tranche_num=tranch_idx)
                if bookrunner_info == 'not a tranche':
                    continue
                bookrunners, participates = bookrunner_info[0], bookrunner_info[1]
                syndicate.append(bookrunners)
                synd_part.append(participates)
                history.append(tranche['표준코드'])
                print(f'Detected book runners:\n{bookrunners}, {participates}')
        else:
            for _ in range(deal.shape[0]):
                bookrunners = ['[]']
                participates = ['[]']
                syndicate.append(bookrunners)
                synd_part.append(participates)
                isin_log = list(deal['표준코드'])
                history.extend(isin_log)
                print(f'Detected book runners:\n{bookrunners}, {participates}')
        my_scrapper.driver.quit()

        # print(f'Got book runners {history}')
        print(f'{df.shape[0] - len(history)} left')
    df['bookrunners'] = syndicate
    df['participation'] = synd_part
        

    return df

        

if __name__ == "__main__":
    # start_date = 20190920
    # end_date = 20200920
    # isin = 'KR6000012A90'
    # fp = './korea.xls'
    # keyword = '산은캐피탈 648-2'
    # company_name = '000010'
    # date = '20200803'

    # my_scrapper = Scrapper()
    # # report_html = my_scrapper.search(keyword)
    # my_scrapper.search_company(company_name, date)
    # my_scrapper.get_bookrunner(report_html)

    input_fp = './data/final_df.csv'
    output_fp = './data/result_df.csv'

    
    df = preprocess(input_fp)
    result_df = search_bookrunner(df)
    result_df.to_csv(output_fp, index=False)
 