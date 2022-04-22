# -*- coding: utf-8 -*-
"""
Created on Wed Sep 15 10:30:14 2021

@author: Viki
"""
##
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
import time
import os
import json
import requests
import re
os.chdir(r'D:\chromedriver')
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')#无界面
browser=webdriver.Chrome(chrome_options=chrome_options)
# browser=webdriver.Chrome()
url='http://guba.eastmoney.com/rank/'
browser.get(url)
browser.execute_script('window.scrollTo(0,document.body.scrollHeight)')
time.sleep(5)
# browser.execute_script('window.scrollTo(0,document.body.scrollHeight)')
# time.sleep(5)


data_all=[]

htmlregex=re.compile('<a href="(.*)"')

   

for a in range(1,6):
    print('正在爬取第'+str(a)+'页')
    html=browser.page_source
    soup = BeautifulSoup(html, 'html.parser')#内置的标准库进行网页的解析
    items=soup.find('tbody',class_='stock_tbody')
    item=items.find_all('tr')

    for i in range(0,len(item)):
        l=[]   
        gp=item[i].find('a',class_='stock_code').text
        l.append(item[i].find('a',class_='stock_code').text) #股票编码
        l.append(item[i].find('div',class_='nametd_box').a.text) #股票名称
           
        
        
        try:
            l.append(item[i].find('div',class_='price_'+str(gp)+' red').text)
        except:
             try:
                 l.append(item[i].find('div',class_='price_'+str(gp)+' green').text)   
             except:
                 l.append(item[i].find('div',class_='price_'+str(gp)+' grey').text)           
                 
      
             
        try:
            l.append(item[i].find('div',class_='zde_'+str(gp)+' red').text)
        except:
            try:
                l.append(item[i].find('div',class_='zde_'+str(gp)+' green').text)
            except:
                l.append(item[i].find('div',class_='zde_'+str(gp)+' grey').text)          
            
        
      
        try:
            l.append(item[i].find('div',class_='zdf_'+str(gp)+' red').text)
        except:
            try:
                l.append(item[i].find('div',class_='zdf_'+str(gp)+' green').text)
            except:
                l.append(item[i].find('div',class_='zdf_'+str(gp)+' grey').text)             
        
        
        # l.append(item[i].find('td',class_='relative').find_all('a')[1]) 
        html=[]
        html.append(item[i].find('td',class_='relative').find_all('a')[1])#股吧链接
        data=htmlregex.findall(str(html))
             
        l.append('http:'+data[0])#股吧链接
        data_all.append(l)
    browser.execute_script('window.scrollTo(0,1188)')
    time.sleep(5)
    try:
        browser.find_element_by_link_text("下一页").click()
    except:
        pass
        continue
    time.sleep(5)
    
    


    
df=pd.DataFrame(data_all,columns=['股票代码','股票名称','最新价','涨跌额','涨跌幅','股吧链接'])
##########################################################
df['涨跌幅'].replace('\%','', regex=True,inplace=True)

df["涨跌幅"] = pd.to_numeric(df["涨跌幅"],errors='coerce')


df['股票名称']=df['股票名称'].apply(lambda i :i.replace(" ",""))
    
df.drop('股吧链接',axis=1,inplace=True)

#%%添加时间戳
from datetime import date     
today=date.today()
df_s=df.copy()
df_s["日期"]=today
df_s=df_s.reset_index()
df_s['排名']=df_s['index']+1
df_s.drop('index',axis=1,inplace=True)
data_new = df_s.reindex(['日期','排名','股票代码', '股票名称', '最新价', '涨跌额', '涨跌幅'], axis = 1)
    


#%%开始对数据入库

from zyq_df_mysql import * 
df_mysql(data_new,'rqlhb','append')

from zyq_send_msg import *
send_msg('今日龙虎榜已更新')