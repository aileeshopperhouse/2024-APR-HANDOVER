from bs4 import BeautifulSoup
from Crawler import Crawler
from logging.handlers import RotatingFileHandler
from user_agent import generate_user_agent

import datetime
import json
import logging
import numpy as np
import os
import pandas as pd
import pymysql
import random
import requests
import sys
import time
import traceback

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

class NaverPrdCrawler(Crawler):
    # product_set // {'model':'가격비교', 'checkout':'네이버페이'}
    # page_limit // 접근할 최대 페이지 수(e.g. page_limit = 10 -> 10페이지까지 접근하겠다)
    # max_loop // block당했을 경우 매 페이지마다 다시 접근할 최대 횟수(e.g. max_loop = 10 -> block당하면 10번까지 header를 update하며 재접속하겠다)
    # wait_low & wait_high // 페이지 이동 시 random sleep 값 설정을 위한 하한선 & 상한선(e.g. wait_low = 5 & wait_high = 10일 경우, 5~10 사이의 random 값 만큼 초단위로 쉬겠다.)
    # paging_size // 1번에 보여줄 상품의 수(e.g. paging_size = 80 -> 1페이지에 80개의 상품을 보여줌)
    # sort_type // {'rel': '네이버랭킹순'}
    def __init__(self, product_set='model', page_limit=10, max_loop=10, wait_low=5, wait_high=10, paging_size=80, sort_type='rel'):
        super().__init__()
        self.df = pd.DataFrame()
        self.sbth = self.get_sbth(60)
        self.product_set = product_set
        self.page_limit = page_limit
        self.max_loop = max_loop
        self.wait_low = wait_low
        self.wait_high = wait_high
        self.paging_size = paging_size
        self.sort_type = sort_type
        self.logger = self.create_logger()

    def create_logger(self):
        return super().create_logger()    


    def renew_tor_ip(self, port):
        return super().renew_tor_ip(port)
    

    def reset_session(self, method, url, headers, json, port, session, proxies, timeout):
        return super().reset_session(method, url, headers, json, port, session, proxies, timeout)
    

    def connect_database(self, db_name):
        return super().connect_database(db_name)
    

    def tunneling(self):
        return super().tunneling()
    

    def save_data(self, data, crawling_type, category, ctg_fullname, ecommerce, bucket_name = 'ailee-crawling-data'):
        return super().save_data(data, crawling_type, category, ctg_fullname, ecommerce, bucket_name = 'ailee-crawling-data')
    

    def wait(self, low, high):
        return random.uniform(low, high)


    def update_done(self, ctg_id):
        query = f"""
        UPDATE ailee.naver_ctg_url
        SET is_done = 1
        WHERE id = {ctg_id}
            AND product_set = '{self.product_set}'
        """
        conn = self.connect_database('ailee')
        with conn:
            with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute(query)
                conn.commit()

        self.logger.info(f'DONE {ctg_id} / {self.product_set}')


    def get_sbth(self, wait):
        query = """
        SELECT
            sbth
        FROM
            ailee.naver_sbth
        ORDER BY
            updated_at DESC
        LIMIT 1
        """
        try:
            with self.connect_database('ailee') as conn:
                with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                    cursor.execute(query)
                    sbth = cursor.fetchall()[0]['sbth']
        except:
            time.sleep(wait)
            with self.connect_database('ailee') as conn:
                with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                    cursor.execute(query)
                    sbth = cursor.fetchall()[0]['sbth']
        
        return sbth


    def prd_crawling(self, port, ctg_name, ctg_fullname, ctg_param1, ctg_param2, max_page_num = True):
        timeout = 60

        headers = {'User-Agent': generate_user_agent(device_type = 'desktop'),
                   'referer': f"https://search.shopping.naver.com/api/search/category/{ctg_param1}?catId={ctg_param2}&eq=&iq=&pagingIndex=1&pagingSize={self.paging_size}&productSet={self.product_set}&sort={self.sort_type}&viewType=list&xq=",
                   'sbth': self.sbth
                   }
        
        proxies = {
                'http': f'socks5://127.0.0.1:{port}',
                'https': f'socks5://127.0.0.1:{port}',
                }

        time.sleep(self.wait(self.wait_low, self.wait_high))

        for paging_index in range(1, self.page_limit+1, 1):
            start_time = datetime.datetime.now()
            url = f"""https://search.shopping.naver.com/api/search/category/{ctg_param1}?catId={ctg_param2}&eq=&iq=&pagingIndex={paging_index}&pagingSize={self.paging_size}&productSet={self.product_set}&sort={self.sort_type}&viewType=list&xq="""

            with requests.session() as session:
                js = ''
                bs_title = '504 Gateway Timeout'
                bs_heads = ['비정상적 요청이 감지되었습니다.', '부적절한 요청입니다.']
                bs_head = '부적절한 요청입니다.'
                bs_desc = '부적절한 요청으로 서비스 연결이 일시적으로 제한 되었습니다.'
                loop_counter = 0
                while(
                    (bs_title == '504 Gateway Timeout')
                    or (bs_title == 'ERROR: The requested URL could not be retrieved')
                    or (bs_head in bs_heads)
                    or (bs_desc == '부적절한 요청으로 서비스 연결이 일시적으로 제한 되었습니다.')
                    or (bs.find('span', {'class' : lambda L : L and L.startswith('subFilter_num')}) == None)
                    or (js == '')
                    ):

                    loop_counter += 1
                    if(loop_counter > self.max_loop):
                        break;
                    time.sleep(self.wait(self.wait_low, self.wait_high))
                    
                    page = self.reset_session(method='get',
                                            url=url,
                                            headers=headers,
                                            json=False,
                                            port=port,
                                            session=session,
                                            proxies=proxies,
                                            timeout=timeout).text
                    
                    bs = BeautifulSoup(page, 'html.parser')

                    if(page == ''):
                        #print("empty page...")
                        break;
                    else:
                        js = json.loads(str(page))

                    try:
                        bs_head = bs.find('div', class_='head').get_text()
                    except:
                        bs_head = ''
                    try:
                        bs_desc = bs.find('div', class_='desc').get_text()
                    except:
                        bs_desc = ''
                    try:
                        bs_title = bs.find('title').get_text()
                    except:
                        bs_title = ''
                
                end_time = datetime.datetime.now()
                total_time = end_time-start_time
                self.logger.info(f"[{paging_index}] | CTG: {ctg_name} | TIME: {total_time}")
                try:
                    total_prd_num = js['shoppingResult']['total']
                except:
                    self.logger.error(traceback.format_exc())
                    break
                
                if(max_page_num):
                    max_page_num = int(np.ceil(total_prd_num/self.paging_size))
                else:
                    if(int(np.ceil(total_prd_num/self.paging_size)) > self.page_limit):
                        max_page_num = self.page_limit
                    else:
                        max_page_num = int(np.ceil(total_prd_num/self.paging_size))
                try:
                    self.df = pd.concat([self.df, pd.DataFrame(js['shoppingResult']['products'])])
                except:
                    self.logger.error(traceback.format_exc())
                    break
                        
        self.df['crawling_date'] = datetime.datetime.now()

        self.save_data(data=self.df,
                        crawling_type=self.product_set,
                        category = ctg_name,
                        ctg_fullname = ctg_fullname,
                        ecommerce = 'naver'
                        )
        self.df = pd.DataFrame()


    def main(self, port, ctg_url):
        df = pd.DataFrame(ctg_url).reset_index(drop=True)

        for _, item in df.iterrows():
            ctg_name = item['ctg_name_eng']
            ctg_fullname = item['ctg_fullname']
            ctg_param1 = item['ctg_param1']
            ctg_param2 = item['ctg_param2']
            try:
                self.prd_crawling(port, ctg_name, ctg_fullname, ctg_param1, ctg_param2, False)
                self.update_done(ctg_id=ctg_fullname)
            except:
                self.logger.error(traceback.format_exc())