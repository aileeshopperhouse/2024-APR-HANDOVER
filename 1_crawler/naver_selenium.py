from tqdm import tqdm
import re
import time
import datetime
from datetime import timedelta

import random

import pandas as pd
import numpy as np
from tqdm import tqdm

import pymysql
import json

import requests
from bs4 import BeautifulSoup

from user_agent import generate_user_agent, generate_navigator

from selenium import webdriver
from selenium.common.exceptions import NoSuchWindowException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def found_window(tab_num):
    def predicate(driver):
        try:
            driver.switch_to.window(driver.window_handles[tab_num])
        except IndexError:
             return False
        else:
             return True # found window
    return predicate

def process_browser_log_entry(entry):
    response = json.loads(entry['message'])['message']
    return response

class NaverCrawler:
    def __init__(self):
        self.caps = DesiredCapabilities.CHROME
        self.caps['goog:loggingPrefs'] = {'performance': 'ALL'}
        self.host = 'localhost'
        self.user = 'root'
        self.password = '1amnotamrlong!'
        self.db = 'naver'
        self.charset = 'utf8mb4'
        self.url = 'https://search.shopping.naver.com/'
        self.chrome_options = webdriver.ChromeOptions()
        self.chrome_options.add_argument("start-maximized")
#         self.chrome_options.add_argument(generate_user_agent(os='win',
#                     device_type='desktop'))
#         self.chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko")
#         self.chrome_options.add_argument('headless')
#         self.chrome_options.add_argument('--no-sandbox')
#         self.chrome_options.add_argument('--disable-dev-shm-usage')
        options = webdriver.ChromeOptions()
        # 크롬드라이버 헤더 옵션추가 (리눅스에서 실행시 필수)
        options.add_argument("start-maximized")
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        self.chromedriver = r'C:\Users\User\Code\chromedriver.exe'
        self.driver = webdriver.Chrome(self.chromedriver,
                                       chrome_options = self.chrome_options,
                                       desired_capabilities=self.caps)
        self.driver.get(self.url)
        
    def get_parsed_html(self):
        self.html = self.driver.page_source
        self.bs = BeautifulSoup(self.html, 'html.parser')
        
    def get_info(self):
        self.now_date = ''
        self.prd_name = ''
        self.prd_url = ''
        self.prd_registration_date = ''
        self.prd_company = ''
        self.prd_brand = ''
        self.prd_heart = 0
        self.prd_rate = 0.0
        self.prd_review_cnt = 0
        self.ctg_1 = ''
        self.ctg_2 = ''
        self.ctg_3 = ''
        self.ctg_4 = ''
        self.prd_price = 0
        
        option_list = ''
        
        self.prd_option_list = []
        
        self.prd_lowest_price_2week = []
        self.prd_lowest_price_1month = []
        self.prd_lowest_price_3month = []
        self.prd_lowest_price_6month = []
        self.lowest_price = []
        
        self.lowest_price_list = [self.prd_lowest_price_2week,
                                  self.prd_lowest_price_1month,
                                  self.prd_lowest_price_3month,
                                  self.prd_lowest_price_6month
                                 ]

        self.now_date = datetime.datetime.now().strftime("%Y%m%d")
        
        waiting = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located(
                            (By.XPATH,
                             f"""//*[@id="__next"]/div/div[2]/div[2]/div[1]""")))
        
        ctgs = self.bs.findAll(
            'div',
            class_='top_breadcrumb__yrBH6'
        )[0].get_text().split('>')
        
        self.ctg_1 = ctgs[0]
        self.ctg_2 = ctgs[1]
        self.ctg_3 = ctgs[2]
        self.ctg_4 = ctgs[3]
        
        self.prd_name = self.bs.findAll(
            'div',
            class_='top_summary_title__15yAr'
        )[0].findAll('h2')[0].get_text()
        
        self.prd_price = int(self.bs.findAll(
            'em',
            class_='lowestPrice_num__3AlQ-'
        )[0].get_text().replace(',', ''))
        
        self.prd_url = self.driver.current_url
        
        for i in self.bs.findAll('span', class_='top_cell__3DnEV'):
            if '등록일' in i.get_text():
                self.prd_registration_date = i.findAll('em')[0].get_text()
            elif '제조사' in i.get_text():
                self.prd_company = i.findAll('em')[0].get_text()
            elif '브랜드' in i.get_text() and i.get_text() != '브랜드 카탈로그':
                self.prd_brand = i.findAll('em')[0].get_text()
            elif '찜하기' in i.get_text():
                self.prd_heart = i.findAll('em')[0].get_text().replace(',', '')
        
        try:
            self.prd_rate = self.bs.findAll(
                'div',
                class_='top_grade__3jjdl'
            )[0].get_text().replace('평점', '')
        except:
            pass
        
        try:
            self.prd_review_cnt = self.bs.findAll(
                'a',
                {'data-nclick' : 'N=a:tab*s.srev'}
            )[0].findAll('em')[0].get_text().replace(',', '')
        except:
            pass
            
        try:
            option_list = self.bs.findAll(
                'div',
                class_='filter_condition_group__2SPoo'
            )[0].findAll('ul')[0].findAll('li')
        
            for i in option_list:
                option = i.findAll(
                    'span',
                    class_='filter_text__3m_XA'
                )[0].get_text()
                option_price = i.findAll(
                    'span',
                    class_='filter_price__1ggj4'
                )[0].get_text()

                prd_option_price = option_price[:option_price.find('원')].replace(',', '')

                self.prd_option_list.append((option, prd_option_price))
        except:
            pass
                
        try:
            for i in range(0, len(self.bs.findAll(
                'ul',
                class_='asideChart_progress_tab__K7A5q'
            )[0].findAll('li')), 1):
                actions = ActionChains(self.driver)

                svg_taplist = self.driver.find_element(
                    By.XPATH,
                    f"""//*[name()='ul' and @role='tablist']
                    //*[name()='li' and @role='presentation']
                    //*[name()='a' and @data-nclick='N=a:sid*g.t{i+1}']"""
                )

                actions.click(svg_taplist)
                actions.perform()

                self.get_parsed_html()

                date_1 = self.bs.findAll('tspan', x='0')[0].get_text()
                date_2 = self.bs.findAll('tspan', x='0')[1].get_text()
                
                if(date_2 != '1'):
                    month_1 = int(date_1[:date_1.find('.')])
                    day_1 = int(date_1[date_1.find('.')+1:])
                    year = int(datetime.datetime.now().strftime("%Y"))
                    month_2 = int(date_2[:date_2.find('.')])
                    day_2 = int(date_2[date_2.find('.')+1:])

                    tooltip_date_1 = datetime.date(year, month_1, day_1)#.strftime("%Y%m%d")
                    tooltip_date_2 = datetime.date(year, month_2, day_2)#.strftime("%Y%m%d")

                    diff = (tooltip_date_2 - tooltip_date_1).days/2

                    for j in range(0, len(self.bs.findAll('g', class_='bb-shapes bb-shapes-y bb-circles bb-circles-y')[0].findAll('circle')), 1):
                        actions = ActionChains(self.driver)

                        svg_circle = self.driver.find_element(
                            By.XPATH,
                            f"//*[name()='svg']//*[name()='circle' and contains(@class, 'bb-shape bb-shape-{j} bb-circle bb-circle-{j}')]"
                        )

                        actions.move_to_element(svg_circle)
                        actions.perform()

    #                     waiting = WebDriverWait(self.driver, 10).until(
    #                         EC.presence_of_element_located(
    #                             (By.XPATH,
    #                              f"""//*[name()='svg']
    #                              //*[name()='circle'
    #                              and contains (@class, 'bb-shape bb-shape-{j} bb-circle bb-circle-{j}""")))

                        svg_tooltip = self.driver.find_element(
                            By.XPATH,
                            "//*[name()='div' and @class='bb-tooltip-container']"
                        )

                        self.lowest_price_list[i].append(
                            (
                                (tooltip_date_1 + timedelta(days = diff*j)).strftime("%Y%m%d"),
                                svg_tooltip.text.replace(',', '')
                            )
                        )
                else:
                    month_1 = int(date_1[:date_1.find('.')])
                    day_1 = int(date_1[date_1.find('.')+1:])
                    year = int(datetime.datetime.now().strftime("%Y"))
                    
                    tooltip_date_1 = datetime.date(year, month_1, day_1)
                    
                    for j in range(0, len(self.bs.findAll('g', class_='bb-shapes bb-shapes-y bb-circles bb-circles-y')[0].findAll('circle')), 1):
                        actions = ActionChains(self.driver)

                        svg_circle = self.driver.find_element(
                            By.XPATH,
                            f"//*[name()='svg']//*[name()='circle' and contains(@class, 'bb-shape bb-shape-{j} bb-circle bb-circle-{j}')]"
                        )

                        actions.move_to_element(svg_circle)
                        actions.perform()

    #                     waiting = WebDriverWait(self.driver, 10).until(
    #                         EC.presence_of_element_located(
    #                             (By.XPATH,
    #                              f"""//*[name()='svg']
    #                              //*[name()='circle'
    #                              and contains (@class, 'bb-shape bb-shape-{j} bb-circle bb-circle-{j}""")))

                        svg_tooltip = self.driver.find_element(
                            By.XPATH,
                            "//*[name()='div' and @class='bb-tooltip-container']"
                        )

                        self.lowest_price_list[i].append(
                            (
                                (tooltip_date_1).strftime("%Y%m%d"),
                                svg_tooltip.text.replace(',', '')
                            )
                        )

            self.lowest_price.extend(self.prd_lowest_price_2week)
            self.lowest_price.extend(self.prd_lowest_price_1month)
            self.lowest_price.extend(self.prd_lowest_price_3month)
            self.lowest_price.extend(self.prd_lowest_price_6month)

            self.lowest_price = list(dict.fromkeys(self.lowest_price))
            self.lowest_price.sort()
        except:
            pass
                              
    def get_max_prd_num(self):
        self.max_prd_num = int(len(self.bs.findAll(
            'ul',
            class_='list_basis'
        )[0].find('div')))
    
    def get_prd_display_list(self):
        waiting = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located(
                            (By.XPATH,
            f"""//*[name()='div' and @class='basicList_title__3P9Q7']""")))
        
        element = self.driver.find_element(
            By.XPATH,
            f"""//*[name()='div' and @class='basicList_title__3P9Q7']"""
        )
        
        self.options = element.find_elements(
            By.XPATH,
            f"""//*[name()='div' and @class='basicList_title__3P9Q7']"""
        )
        
    def get_prd_thumbnail_list(self):
        waiting = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located(
                            (By.XPATH,
            f"""//*[name()='div' and @class='thumbnail_thumb_wrap__1pEkS _wrapper']""")))
        
        element = self.driver.find_element(
            By.XPATH,
            f"""//*[name()='div' and @class='thumbnail_thumb_wrap__1pEkS _wrapper']"""
        )
        
        self.options = element.find_elements(
            By.XPATH,
            f"""//*[name()='div' and @class='thumbnail_thumb_wrap__1pEkS _wrapper']"""
        )
        
    def get_max_page_num(self):
        self.max_page_num = int(self.bs.findAll(
            'div',
            class_='pagination_num__-IkyP'
        )[0].findAll('a')[-1].get_text())
        
    def get_now_page_num(self):
        self.now_page_num = int(self.bs.findAll(
            'div',
            class_='pagination_num__-IkyP'
        )[0].findAll(
            'span',
            class_='pagination_btn_page__FuJaU active'
        )[0].get_text().replace('현재 페이지', ''))
    
    def click_pagination_left(self):
        if(self.bs.findAll('a', class_='pagination_next__1ITTf') != []):
            self.driver.find_element(
                By.XPATH,
                '//*[@class="pagination_next__1ITTf"]'
            ).click()
    
    def move_page(self, num):
        self.driver.find_element(
            By.XPATH,
            f"""//*[name()='a' and @data-nclick='N=a:pag.page,i:{num}']"""
        ).click()
        
    def click_price_comparison(self):
        waiting = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located(
                            (By.XPATH,
                             f"""//*[name()='ul' and @class='subFilter_seller_filter__3yvWP']
                             //*[name()='li']""")))
        
        element = self.driver.find_element(
            By.XPATH,
            f"""//*[name()='ul' and @class='subFilter_seller_filter__3yvWP']
            //*[name()='li']"""
        )
        subtab = element.find_elements(
            By.XPATH,
            f"""//*[name()='ul' and @class='subFilter_seller_filter__3yvWP']
            //*[name()='li']"""
        )
        for i in subtab:
            if('가격비교' in i.text):
                i.click()
                break;
                
    def click_review_tab(self):
        self.review_tab_prescence = False
        waiting = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located(
                            (By.XPATH,
                             f"""//*[name()='ul' and @role='tablist']""")))
        
        try:
            self.driver.find_element(
                By.XPATH,
                f"""//*[name()='ul' and @role='tablist']
                //*[name()='a' and @data-nclick='N=a:tab*s.srev']"""
            )
            self.review_tab_prescence = True
        except:
            self.review_tab_prescence = False
                
    def get_now_review_page(self):
        self.now_rev_page_attr = ''
        self.next_rev_page_attr = ''

        waiting = WebDriverWait(self.driver, 20).until(
            EC.presence_of_element_located(
                (By.XPATH,
                 f"""//*[name()='div' and @class = 'pagination_pagination__2M9a4']""")))

        self.now_rev_page_attr = self.driver.find_element(
        By.XPATH,
        """//*[name()='div' and @class = 'pagination_pagination__2M9a4']
        //*[name()='a'
        and contains(@data-nclick, 'N=a:rev.page')
        and @class = 'pagination_now__gZWGP']"""
        ).get_attribute('data-nclick')

        split_num = self.now_rev_page_attr.find(',')+3

        self.now_rev_page_num = int(self.now_rev_page_attr[split_num:])

        self.next_rev_page_attr = self.now_rev_page_attr[:split_num] + str(self.now_rev_page_num + 1)
        
    def click_review_next_pagination(self):
        waiting = WebDriverWait(self.driver, 20).until(
            EC.presence_of_element_located(
                (By.XPATH,
                 f"""//*[name()='div' and @class = 'pagination_pagination__2M9a4']""")))
        
        self.driver.find_element(
        By.XPATH,
        f"""//*[name()='div' and @class = 'pagination_pagination__2M9a4']
        //*[name()='a'
        and @data-nclick = '{self.next_rev_page_attr}']"""
        ).click()
        
    def get_insert_id(self):
        self.insert_id = ''
        waiting = WebDriverWait(self.driver, 20).until(
            EC.presence_of_element_located(
                (By.XPATH,
                 f"""//*[name()='ul' and @class = 'reviewItems_list_review__1sgcJ']""")))
        
        waiting = WebDriverWait(self.driver, 20).until(
            EC.presence_of_element_located(
                (By.XPATH,
                 f"""//*[name()='div' and @class = 'floatingTab_info_area__Ou-9l']""")))
        
        self.get_parsed_html()
            
        self.prd_name = self.bs.findAll(
            'div',
            class_='floatingTab_info_area__Ou-9l'
        )[0].strong.get_text()
        
        conn = pymysql.connect(host=self.host,
                               user=self.user,
                               password=self.password,
                               db=self.db,
                               charset=self.charset)

        sql = f"""select id from naver.product_info
            where name = '{self.prd_name}'"""

        curs = conn.cursor(pymysql.cursors.DictCursor)
        curs.execute(sql)
        result = curs.fetchall()
        self.insert_id = result[0].get('id')
                
    def get_review_info(self):
        self.now_date = datetime.datetime.now().strftime("%Y%m%d")
        self.user_id = ''
        self.review_registration_date = datetime.datetime(1900, 1, 1).strftime("%Y%m%d")
        self.review_prd_option = ''
        self.review_title = ''
        self.review_text = ''
        self.review_rate = 0
        self.review_image = 0
        self.prd_name = ''
        
        waiting = WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH,
                 f"""//*[name()='ul' and @class = 'reviewItems_list_review__1sgcJ']""")))
        
        self.get_parsed_html()
        
        reviews = self.bs.findAll(
            'ul',
            class_='reviewItems_list_review__1sgcJ'
        )[0].findAll('li')
        
        for review in reviews:
            try:
                self.review_rate = review.findAll(
                    'div',
                    class_='reviewItems_etc_area__2P8i3'
                )[0].findAll(
                    'span',
                    class_='reviewItems_average__16Ya-'
                )[0].get_text().replace('평점', '')
            except IndexError:
                self.review_rate = ''
            
            try:
                self.user_id = review.findAll(
                    'div',
                    class_='reviewItems_etc_area__2P8i3'
                )[0].findAll(
                    'span',
                    class_='reviewItems_etc__1YqVF'
                )[1].get_text()
            except IndexError:
                self.user_id = ''

            try:
                self.review_date = review.findAll(
                    'div',
                    class_='reviewItems_etc_area__2P8i3'
                )[0].findAll(
                    'span',
                    class_='reviewItems_etc__1YqVF'
                )[2].get_text()
                self.review_registration_date = '20'+self.review_date.replace('.', '')
                
            except IndexError:
                self.review_registration_date = datetime.datetime(1900, 1, 1).strftime("%Y%m%d")

            try:
                self.review_prd_option = review.findAll(
                    'div',
                    class_='reviewItems_etc_area__2P8i3'
                )[0].findAll(
                    'span',
                    class_='reviewItems_etc__1YqVF'
                )[3].get_text()
                
            except IndexError:
                self.review_prd_option = ''
            
            self.review_title = review.findAll(
                'em',
                class_='reviewItems_title__39Z8H'
            )[0].get_text().replace('"', '').replace("'", '')
            
            self.review_text = review.findAll(
                'p',
                class_='reviewItems_text__XIsTc'
            )[0].get_text().replace('"', '').replace("'", '')
            
            review_image_thumb = review.findAll(
                'div',
                class_='reviewItems_review_thumb__CK7I2'
            )
            
            if(review_image_thumb == []):
                self.review_image = 0
            else:
                self.review_image = 1
                
            """
            sql insert
            """
            # insert into naver.option_info
            if(self.insert_id != ''):
                conn = pymysql.connect(host=self.host,
                       user=self.user,
                       password=self.password,
                       db=self.db,
                       charset=self.charset)

                sql = f"""INSERT IGNORE INTO naver.prd_review_info
                (prd_id, user_id, crawling_date, review_registration_date, review_prd_option,
                review_title, review_text, review_rate, review_image)
                VALUES("{self.insert_id}",
                "{self.user_id}",
                "{self.now_date}",
                "{self.review_registration_date}",
                "{self.review_prd_option}",
                "{self.review_title}",
                "{self.review_text}",
                "{self.review_rate}",
                "{self.review_image}"
                )"""

                with conn:
                    with conn.cursor() as cur:
                        cur.execute(sql)
                        conn.commit()
                        
    def get_prd_info_main(self, url_list):
        for i in url_list:
            self.move_url(f'https://search.shopping.naver.com/search/category/{i}')
            self.click_price_comparison()
            self.croll_down_to_end_of_page()
            self.get_parsed_html()
            self.get_now_page_num()
            # <h2 class="style_head__1EHvK">상품이 <em>존재하지 않습니다.</em></h2>
            # '다음' element가 사라질 때까지 반복
        #     while(crawler.bs.findAll('a', class_='pagination_next__1ITTf') != []):
            while(int(self.now_page_num) < 22):
                waiting = WebDriverWait(self.driver, 10).until(
                                EC.presence_of_element_located(
                                    (By.XPATH,
                                     f"""//*[name()='ul' and @class='subFilter_seller_filter__3yvWP']
                                     //*[name()='li']""")))
                self.get_parsed_html()
                waiting = WebDriverWait(self.driver, 10).until(
                                EC.presence_of_element_located(
                                    (By.XPATH,
                                    "//*[name()='div' and @class = 'thumbnail_thumb_wrap__1pEkS _wrapper']")))
        #         crawler.get_prd_thumbnail_list()
                trial_counter = 0
                while(trial_counter < 11):
                    try:
                        self.options[0].tag_name()
                        trial_counter = 100
                    except:
                        self.get_prd_thumbnail_list()

                    time.sleep(1)
                    trial_counter += 1

                for j in self.options:
                    waiting = WebDriverWait(self.driver, 10).until(
                                EC.presence_of_element_located(
                                    (By.XPATH,
                                     f"""//*[name()='ul' and @class='subFilter_seller_filter__3yvWP']
                                     //*[name()='li']""")))
                    waiting = WebDriverWait(self.driver, 10).until(
                                EC.presence_of_element_located(
                                    (By.XPATH,
                                    "//*[name()='div' and @class = 'thumbnail_thumb_wrap__1pEkS _wrapper']")))
                    j.click()
                    WebDriverWait(self.driver, timeout=5).until(found_window(1))
                    self.driver.window_handles
                    self.switch_tab(-1)
                    try:
                        self.get_parsed_html()
                        self.get_info()

                        """
                        sql insert

                        """
                        conn = pymysql.connect(host=self.host,
                                   user=self.user,
                                   password=self.password,
                                   db=self.db,
                                   charset=self.charset)

                        # insert into naver.product_info table
                        sql = f"""INSERT INTO product_info
                        (name, url, crawling_date, registration_date,
                        company, brand, heart, rate, review_cnt,
                        prd_price, ctg_1, ctg_2, ctg_3, ctg_4)
                        VALUES("{self.prd_name}",
                        "{self.prd_url}",
                        "{self.now_date}",
                        "{self.prd_registration_date.replace('.', '')+'01'}",
                        "{self.prd_company}",
                        "{self.prd_brand}",
                        "{self.prd_heart}",
                        "{self.prd_rate}",
                        "{self.prd_review_cnt}",
                        "{self.prd_price}",
                        "{self.ctg_1}",
                        "{self.ctg_2}",
                        "{self.ctg_3}",
                        "{self.ctg_4}")
                        ON DUPLICATE KEY UPDATE
                        url = "{self.prd_url}",
                        crawling_date = "{self.now_date}",
                        registration_date = "{self.prd_registration_date.replace('.', '')+'01'}",
                        company = "{self.prd_company}",
                        brand = "{self.prd_brand}",
                        heart = "{self.prd_heart}",
                        rate = "{self.prd_rate}",
                        review_cnt = "{self.prd_review_cnt}",
                        prd_price = "{self.prd_price}",
                        ctg_1 = "{self.ctg_1}",
                        ctg_2 = "{self.ctg_2}",
                        ctg_3 = "{self.ctg_3}",
                        ctg_4 = "{self.ctg_4}"
                        """

                        with conn:
                            with conn.cursor() as cur:
                                cur.execute(sql)
                                conn.commit()
                                
                        self.get_insert_id()

                        # insert into naver.option_info
                        if(self.prd_option_list != []):
                            insert_line = ''
                            insert_text = ''
                            for i in self.prd_option_list:
                                insert_line = '(' + "'" + self.insert_id + "'" + ', ' + "'" + i[0] + "'" + ', ' + "'" + i[1] + "'" + '),'
                                insert_text += insert_line

                            conn = pymysql.connect(host=self.host,
                                   user=self.user,
                                   password=self.password,
                                   db=self.db,
                                   charset=self.charset)

                            sql = f"""INSERT IGNORE INTO naver.option_info
                            (id, option_name, option_price)
                            VALUES{insert_text[:-1]}"""

                            with conn:
                                with conn.cursor() as cur:
                                    cur.execute(sql)
                                    conn.commit()

                        # insert into naver.lowest_price_info
                        if(self.lowest_price != []):
                            insert_line = ''
                            insert_text = ''
                            for i in self.lowest_price:
                                insert_line = '(' + "'" + self.insert_id + "'" + ', ' + "'" + i[0] + "'" + ', ' + "'" + i[1] + "'" + '),'
                                insert_text += insert_line

                            conn = pymysql.connect(host=self.host,
                                   user=self.user,
                                   password=self.password,
                                   db=self.db,
                                   charset=self.charset)

                            sql = f"""INSERT IGNORE INTO naver.lowest_price_info
                            (id, date, lowest_price)
                            VALUES{insert_text[:-1]}"""

                            with conn:
                                with conn.cursor() as cur:
                                    cur.execute(sql)
                                    conn.commit()
                    except:
                        pass
                    finally:
                        self.close_current_tab()
                        self.switch_tab(0)

                self.get_parsed_html()
                waiting = WebDriverWait(self.driver, 10).until(
                                EC.presence_of_element_located(
                                    (By.XPATH,
                                     f"""//*[name()='ul' and @class='subFilter_seller_filter__3yvWP']
                                     //*[name()='li']""")))
                self.click_pagination_left()
                self.get_now_page_num()

#                 conn.commit()
        crawler.quit()
                
    def get_sales_amount(self):
        self.now_date = datetime.datetime.now().strftime("%Y%m%d")
        self._3days_sales = 0
        self._6months_sales = 0
        self.smartstore_url = ''
        
        conn = pymysql.connect(host=self.host,
                       user=self.user,
                       password=self.password, 
                       db=self.db, 
                       charset=self.charset)
        
        sql = """select id, url from naver.product_info"""
        
        curs = conn.cursor(pymysql.cursors.DictCursor)
        curs.execute(sql)
        self.target_list = curs.fetchall()
        self.target_list = self.target_list[400:]
        
#         self.target_list = self.target_list[1100:3000]
        
        caps = DesiredCapabilities.CHROME
        caps['goog:loggingPrefs'] = {'performance': 'ALL'}
        print('caps done')
        for row in self.target_list:
            self.driver.get(row['url'])
            self.driver.refresh()
            
            try:
                waiting = WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located(
                                (By.XPATH,
                                 """//*[name()='div'
                                 and contains(@class, 'top_summary_title')]""")))
            except:
                continue
            
            self.get_parsed_html()
            print('파싱 완료')
            # 옵션 리스트 추출
            try:
                option_list = self.driver.find_elements(
                    By.XPATH,
                    """//*[name()='div' and contains(@class, 'filter_condition_group')]
                    //*[name()='ul']
                    //*[name()='li']"""
                )
                option_list = option_list[int(len(option_list)/2):]
                self.options = option_list
                option_presence = True
            except:
                option_presence = False
            
            print('옵션 리스트 추출 완료')
            # 옵션 순차적 클릭
            try:
                if(option_presence==True):
                    for i in range(0, len(option_list), 1):
    #                     option_list = self.driver.find_elements(
    #                         By.XPATH,
    #                         """//*[name()='div' and contains(@class, 'filter_condition_group')]
    #                         //*[name()='ul']
    #                         //*[name()='li']"""
    #                     )
    #                     option_list = option_list[int(len(option_list)/2):]
    #                     self.options = option_list
                        time.sleep(2)
                        option_list[i].click()
                        print('click finished')
                        try:
                            waiting = WebDriverWait(self.driver, 10).until(
                                        EC.presence_of_element_located(
                                            (By.XPATH,
                                             """//*[name()='div' and contains(@class, 'filter_condition_group')]
                                             //*[name()='ul']
                                             //*[name()='li']""")))
                        except:
                            continue

                        self.option_name = option_list[i].get_attribute('innerText').split('\n')[0]
                        print(self.option_name)
                        time.sleep(2)
                        self.get_parsed_html()
                        # 쇼핑몰 리스트 구분 및 추출
                        if(self.bs.findAll(
                            'table',
                            {'class' :lambda L: L and L.startswith('productByMall_list_seller__')},
    #                         partial=True
                        ) != []):
                            mall_urls = []
                            malls = self.bs.findAll(
                                'table',
                                {'class' :lambda L: L and L.startswith('productByMall_list_seller__')},
    #                             partial = True
                            )[0].findAll(
                                'td',
                                {'class' :lambda L: L and L.startswith('productByMall_mall_area__')},
    #                             partial = True
        #                             class_='productByMall_mall_area__1oEU_'
                            )
                            print('for mall in malls')
                            for mall in malls:
                                if(mall.findAll('a',
                                                {'class' :lambda L: L and L.startswith('productByMall_mall__')},
    #                                             partial = True
        #                                             class_='productByMall_mall__1ITj0'
                                               )[0].findAll('img') == []):
                                    mall_urls.append(
                                        mall.findAll(
                                            'a',
                                            {'class' :lambda L: L and L.startswith('productByMall_mall__')},
    #                                         partial = True
        #                                         class_='productByMall_mall__1ITj0'
                                        )[0]['href'])
                                else:
                                    pass
                        print('for mall in mall_urls')
                        for mall in mall_urls:
                            session = requests.session()
                            page = session.get(mall)
                            try:
                                bs_fulltext = BeautifulSoup(page.text, 'html.parser')

        #                         target_json = json.loads(bs_fulltext.findAll('script')[1].get_text()[27:])
                                json_parsed = json.loads(str(bs_fulltext.findAll('script')[1])[35:].replace('</script>', ''))
                                self._3days_sales = json_parsed['product']['A']['saleAmount']['recentSaleCount']
                                self._6months_sales = json_parsed['product']['A']['saleAmount']['cumulationSaleCount']
                                self.smartstore_url = mall

                                conn = pymysql.connect(host=self.host,
                                   user=self.user,
                                   password=self.password, 
                                   db=self.db, 
                                   charset=self.charset)

                                print(self._3days_sales)
                                print(self._6months_sales)

                                sql = f"""INSERT IGNORE INTO naver.new_prd_sales
                                (id, crawling_date, url, sales_3days, sales_6months, option_name)
                                VALUES("{row['id']}",
                                "{self.now_date}",
                                "{self.smartstore_url}",
                                "{self._3days_sales}",
                                "{self._6months_sales}",
                                "{self.option_name}"
                                )"""

                                with conn:
                                    with conn.cursor() as cur:
                                        cur.execute(sql)
                                        conn.commit()

                                print('sql inserted')

                            except:
                                print('except error')
            except:
                pass

            
            else:
                option_name = ''
                self.get_parsed_html()
                # 쇼핑몰 리스트 구분 및 추출
                if(self.bs.findAll(
                    'table',
                    {'class' :lambda L: L and L.startswith('productByMall_list_seller__')},
#                         partial=True
                ) != []):
                    mall_urls = []
                    malls = self.bs.findAll(
                        'table',
                        {'class' :lambda L: L and L.startswith('productByMall_list_seller__')},
#                             partial = True
                    )[0].findAll(
                        'td',
                        {'class' :lambda L: L and L.startswith('productByMall_mall_area__')},
#                             partial = True
#                             class_='productByMall_mall_area__1oEU_'
                    )
                    print('for mall in malls')
                    for mall in malls:
                        if(mall.findAll('a',
                                        {'class' :lambda L: L and L.startswith('productByMall_mall__')},
#                                         partial = True
#                                             class_='productByMall_mall__1ITj0'
                                       )[0].findAll('img') == []):
                            mall_urls.append(
                                mall.findAll(
                                    'a',
                                    {'class' :lambda L: L and L.startswith('productByMall_mall__')},
#                                         partial = True
#                                         class_='productByMall_mall__1ITj0'
                                )[0]['href'])
                        else:
                            pass
                print('for mall in mall_urls')
                for mall in mall_urls:
                    session = requests.session()
                    page = session.get(mall)
                    try:
                        bs_fulltext = BeautifulSoup(page.text, 'html.parser')

#                         target_json = json.loads(bs_fulltext.findAll('script')[1].get_text()[27:])
                        json_parsed = json.loads(str(bs_fulltext.findAll('script')[1])[35:].replace('</script>', ''))
                        self._3days_sales = json_parsed['product']['A']['saleAmount']['recentSaleCount']
                        self._6months_sales = json_parsed['product']['A']['saleAmount']['cumulationSaleCount']
                        self.smartstore_url = mall

                        conn = pymysql.connect(host=self.host,
                           user=self.user,
                           password=self.password, 
                           db=self.db, 
                           charset=self.charset)

                        print(self._3days_sales)
                        print(self._6months_sales)

                        sql = f"""INSERT IGNORE INTO naver.new_prd_sales
                        (id, crawling_date, url, sales_3days, sales_6months, option_name)
                        VALUES("{row['id']}",
                        "{self.now_date}",
                        "{self.smartstore_url}",
                        "{self._3days_sales}",
                        "{self._6months_sales}",
                        "{self.option_name}"
                        )"""

                        with conn:
                            with conn.cursor() as cur:
                                cur.execute(sql)
                                conn.commit()

                        print('sql inserted')

                    except:
                        print('except error')

    def croll_down_to_end_of_page(self):
        previous_height = 0
        now_height = 1

        while(now_height != previous_height):
            previous_height = self.driver.execute_script("return document.body.scrollHeight")
            self.driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.PAGE_DOWN)
            self.driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.PAGE_DOWN)
            self.driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.PAGE_DOWN)
            self.driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.PAGE_DOWN)
            self.driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.PAGE_DOWN)
            now_height = self.driver.execute_script("return document.body.scrollHeight")
        
    def move_url(self, url):
        self.url = url
        self.driver.get(self.url)
        
    def close_current_tab(self):
        self.driver.close()
        
    def switch_tab(self, num):
        self.driver.switch_to.window(self.driver.window_handles[num])
        
    def quit(self):
        self.driver.quit()