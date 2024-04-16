from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler
from pathlib import Path
from requests.exceptions import ProxyError, SSLError, ConnectTimeout, Timeout
from sshtunnel import SSHTunnelForwarder
from stem import Signal
from stem.control import Controller

import boto3
import datetime
import logging
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pymysql
import random
import time
import traceback


class Crawler:
		# 초기화
    def __init__(self):
        self.data = []
        self.sales_data = []
        dotenv_path = Path("y/home/ailee/ailee-data-app/.env") # your .env file path
        load_dotenv(dotenv_path=dotenv_path)
        AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
        AWS_SECRET_ACCESS_KEY_ID = os.getenv("AWS_SECRET_ACCESS_KEY_ID")
        self.mysql_host = os.getenv("MYSQL_HOST")
        self.mysql_id = os.getenv("MYSQL_ID")
        self.mysql_password = os.getenv("MYSQL_PASSWORD")
        self.tor_password = os.getenv("TOR_PASSWORD")
        self.s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID)
        self.log_path = f'/home/ailee/ailee-data-app/naver/log/'
        self.logger = self.init_logger()


    def init_logger(self, ):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

        if(len(logger.handlers) == 3):
            return logger
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        handler1 = logging.StreamHandler()
        handler1.setLevel(logging.INFO)
        handler1.setFormatter(formatter)

        handler2 = RotatingFileHandler(f'{self.log_path}debug.log', maxBytes=100*1024*1024, backupCount=5)
        handler2.setLevel(logging.DEBUG)
        handler2.setFormatter(formatter)

        handler3 = RotatingFileHandler(f'{self.log_path}error.log', maxBytes=100*1024*1024, backupCount=5)
        handler3.setLevel(logging.ERROR)
        handler3.setFormatter(formatter)

        logger.addHandler(handler1)
        logger.addHandler(handler2)
        logger.addHandler(handler3)

        return logger
    

    def set_sbth(self, wait):
        query = """
        SELECT
            sbth
        FROM
            ailee.naver_sbth
        ORDER BY
            RAND()
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

        # self.logger.info('GET SBTH')
        
        self.sbth = sbth


    def renew_tor_ip(self, port):
        with Controller.from_port(port = int(port)+1) as controller:
            controller.authenticate(password=self.tor_password)
            controller.signal(Signal.NEWNYM)

        self.logger.info(f'RENEW TOR IP ADDRESS {port:4d}')
        time.sleep(random.uniform(3, 5))
        

    def reset_session(self, method, url, headers, json, port, session, proxies, timeout):
        if(method == 'get'):
            try:
                page = session.get(url,
                                headers = headers,
                                proxies = proxies,
                                timeout = (timeout, timeout),
                                )
                if(page.status_code == 200):
                    return page
                else:
                    self.logger.debug(page.status_code)
                    self.logger.debug(page.text)
                    self.renew_tor_ip(port)
                    
                    return False

            except:
                self.logger.error(traceback.format_exc())
                self.renew_tor_ip(port)
                
                return False
        
        elif(method == 'post'):
            try:
                page = session.post(url,
                                headers = headers,
                                json = json,
                                proxies = proxies,
                                timeout = (timeout, timeout),
                                )
                if(page.status_code == 200):
                    return page
                else:
                    self.logger.debug(page.status_code)
                    self.logger.debug(page.text)
                    self.renew_tor_ip(port)
                    
                    return False

            except:
                self.logger.error(traceback.format_exc())
                self.renew_tor_ip(port)
                
                return False
        
        
    def connect_database(self, db_name):
        conn = pymysql.connect(
            host=self.mysql_host,
            user=self.mysql_id,
            password=self.mysql_password,
            db=db_name,
            charset='utf8mb4'
        )

        return conn

    def save_data(self, data, crawling_type, category, ctg_fullname, ecommerce, file_format = 'parquet', bucket_name = 'ailee-crawling-data'):
        now = datetime.datetime.now()
        year = now.year
        week = str(now.isocalendar()[1]).zfill(2)
        file_name = f'{crawling_type}_{ctg_fullname}_{now.strftime("%Y%m%d")}.{file_format}'
        s3_folder_path = f'new-{ecommerce}/{crawling_type}/{category}/raw/Y{year}/W{week}/'
        s3_key = s3_folder_path+file_name

        local_folder_path = f'/home/ailee/download/{ecommerce}/data/{crawling_type}/{category}/Y{year}/W{week}/'
        local_key = local_folder_path+file_name

        Path(local_folder_path).mkdir(parents=True, exist_ok=True)

        table = pa.Table.from_pandas(data)

        pq.write_table(table, local_key)

        self.s3.upload_file(local_key, bucket_name, s3_key)
        self.logger.info(f'[UPLOAD] {ecommerce:20s} | {crawling_type:20s} | {category:20s} | {year:4d} | {week:2d} ... DONE')