# -*- coding: utf-8 -*-
"""
Created on Fri Aug 19 21:59:49 2022

@author: Viki
"""

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.edge.service import Service
import time
import os
# import json
# import requests
import re
import random
# import datetime 

from time import strftime
from time import gmtime

def change_df(df):
    arr=df.values
    new_df = pd.DataFrame(arr[1:,1:], index=arr[1:,0], columns=arr[0,1:])
    new_df.index.name = arr[0,0]
    new_df.reset_index(inplace=True)
    return new_df


gp='002340'
url='http://quote.eastmoney.com/sz002340.html'
driver_path = r'D:\edgedriver\msedgedriver.exe'
options = webdriver.EdgeOptions()
options.add_argument('--headless=new')#无界面
options.add_argument('--disable-gpu')
service = Service(driver_path)
browser = webdriver.Edge(service=service, options=options)
# browser=webdriver.Edge(service=service, options=options)
browser.get(url)

html=browser.page_source
soup = BeautifulSoup(html, 'html.parser')#内置的标准库进行网页的解析
#开始收集数据
tbody=soup.find('div',class_='brief_info_c').tbody.find_all('td')

dataRegex=re.compile(r'([\u4e00-\u9fa5]*)：.*?class=.*?>(.*?)<')#,,<td.*?>(.*?)</td>


data=dataRegex.findall(str(tbody))

df=pd.DataFrame(data)
#统一创业板的标签
try: 
    df.replace('换手率', '换手',inplace=True)
    df.replace('涨停价', '涨停',inplace=True)
    df.replace('跌停价', '跌停',inplace=True)
    df.replace('市净率', '市净',inplace=True)
    
except Exception as e:
                print(e)
            
 #添加股市代码
df.loc[-1]={0:'股票代码',1:gp}
 
df.rename(columns={0:'匹配列',1:gp},inplace=True)
df=change_df(df.T)
 
df=df[['股票代码', '今开', '最高', '涨停', '换手', '成交量', '总市值', '昨收', '最低', '跌停', '量比', '成交额',
'市净', '流通市值']]
 

    
    
    