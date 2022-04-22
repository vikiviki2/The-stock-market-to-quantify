# -*- coding: utf-8 -*-
"""
Created on Sun Mar 27 15:28:35 2022

@author: Viki
"""

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
import time
import os
import json
import requests
import re
import random
import datetime 

from time import strftime
from time import gmtime

#%%从gp_att 抽取代码

import mysql.connector as sql
db_connection = sql.connect(host='localhost', database='science', 
user='root', password='ZXCVBNM123')
sql="""select t.`股票代码`,t.`股票名称` from gp_att t """
df_gp= pd.read_sql(sql, con=db_connection)
gp_code=df_gp['股票代码'].values.tolist()
# gp_code=['688606','002435','001313','600798','002589','300081','600192','002178','300966','600272','002474','000710','002207','000881','000078','600661','600684','002878','600190','600982','300075','301126','002246','000953','603368','605116','002626','600051','603963','600833','603122','000411','002800','601615','603569','002898','600510','603029','601199','000862','300254','600777','600587','301130','001202','000723','601600','002997','300261','002717','002104','600546','600396','000807','600113','300264','600792','300249','002758','002385','600111','601101','300164','605333','000792','600880','600735','601880','000858','002172','603466','000718','000908','002340']

star=time.time()#计算结束时间
#%%
# 沪市股票包含上证主板和科创板和B股：沪市主板股票代码是60开头、科创板股票代码是688开头、B股代码900开头。
# 深市股票包含主板、中小板、创业板和B股：深市主板股票代码是000开头、中小板股票代码002开头、创业板300开头、B股代码200开头
def gp_type_szsh(gp):
    if gp.find('6',0,2)==0:
        gp_type='sh'
    elif gp.find('900',0,4)==0:
        gp_type='sh'
    elif gp.find('0',0,2)==0:
        gp_type='sz'
    elif gp.find('3',0,2)==0:
        gp_type='sz'
    elif gp.find('200',0,4)==0:
        gp_type='sz'
    return gp_type

ms=[]
number=0
for gp in gp_code:
    try:
        ms1=[]#装一维数据
        szsh=gp_type_szsh(gp)#分辨沪深股票  
        url_pre='http://quote.eastmoney.com/'
        # http://quote.eastmoney.com/sz002340.html
        url=url_pre+szsh+gp +'.html'
        os.chdir(r'D:\chromedriver')
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')#无界面
        browser=webdriver.Chrome(chrome_options=chrome_options)
        # browser=webdriver.Chrome()
        browser.get(url)
        time.sleep(random.randint(3,8))
        
        html=browser.page_source
        soup = BeautifulSoup(html, 'html.parser')#内置的标准库进行网页的解析
        
        #获取其股票名称及股票代码
        gp_sup=soup.find('div',class_='qphox layout gubamodule2017').find(id='ivap').text   
        gp_nsup=soup.find('div',class_='qphox layout gubamodule2017').find(id='ivbp').text
        
        #把数据放入列表
        ms1.append(gp)
        ms1.append(gp_sup)
        ms1.append(gp_nsup)
        print('已完成'+str(gp))
        ms.append(ms1)
        
        number+=1
        aroumt=format(number/len(gp_code), '.4%')
        
        process=time.time()#计算结束时间 
        process_time= process-star
        process_time=strftime("%H:%M:%S", gmtime(process_time))
        print('已经成功运行代码，用时'+ str(process_time)+'s,完成百分比：'+aroumt)
        
        
    except:
        	pass
        	continue
    
makets=pd.DataFrame(ms,columns=['股票代码','看涨','看跌'])

df_ms=df_gp.merge(makets,how='left',on='股票代码')

#添加日期标签
from datetime import date     
today=date.today()
df_ms["日期"]=today

df_ms = df_ms.reindex(['日期','股票代码', '股票名称', '看涨', '看跌'], axis = 1)

#删除百分号
df_ms=df_ms[(df_ms['看涨'].isnull()==False)]
df_ms['看涨']=df_ms['看涨'].apply(lambda i :i.strip('%'))
df_ms['看跌']=df_ms['看跌'].apply(lambda i :i.strip('%'))
df_ms['股票名称']=df_ms['股票名称'].apply(lambda i :i.replace(" ",""))

from zyq_df_mysql import * 
df_mysql(df_ms,'market_s','append')


end=time.time()#计算结束时间 
use_time= end-star
use_time=strftime("%H:%M:%S", gmtime(use_time))

from zyq_send_msg import *
send_msg('已成功捕捉市场涨跌情况，已经成功运行代码，用时'+ str(use_time)+'s')







