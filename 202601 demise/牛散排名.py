# -*- coding: utf-8 -*-
"""
Created on Sun Jul  3 16:02:58 2022

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
from lxml import etree


#%%假设重跑先将今日数据删除

from datetime import date  
today=date.today()

import pymysql
conn = pymysql.connect(host="localhost", user='root', password='ZXCVBNM123', database='science')
cursor = conn.cursor()

sql="""DELETE  FROM `retail_in_rank`
where `数据入库日期`='%s';"""%(today)

sql_list = sql.split(';')
for i in range(len(sql_list)):
    time.sleep(1)
    try:
        cursor.execute(sql_list[i])
        
        
    except:
        pass
        continue
    
# 提交事务
conn.commit()
# 关闭游标
cursor.close()
# 关闭数据库连接
conn.close()
    
print("已检查retail_in_rank是否有重复数据")




















os.chdir(r'D:\chromedriver')

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')#无界面
browser=webdriver.Chrome(chrome_options=chrome_options)
# browser=webdriver.Chrome()
url='http://cwzx.shdjt.com/top500.asp'
browser.get(url)
browser.implicitly_wait(3)

#获取网页信息
html=browser.page_source

#利用正则提取信息
itemsRegex=re.compile(r'<td[\s]*class="tdlefttop">(.*?)<a[\s]*target="_blank"[\s]*class="(.*?)"[\s]*title=.*?href="(.*?)">(.*?)</a></td>')
items=itemsRegex.findall(html)


#放入数据整理数据
df=pd.DataFrame(items,columns=['排名','15天内更新情况','网址','名称'])

df['15天内更新情况'].replace('akred', '更新持股', regex=True,inplace=True) 
df['15天内更新情况'].replace('ak', '未更新持股', regex=True,inplace=True) 

df['排名']=df['排名'].apply(lambda x:x.strip('.'))

from datetime import date  
today=date.today()
df['日期']=today



from zyq_df_mysql import *
df_mysql(df,'retail_in_rank','append')




from zyq_send_msg import *
send_msg('已成功捕捉前500的牛散，已经成功运行代码')
#%%输入查询的股票的代码    #这个方法不好可用但是不好用已经废除
# os.chdir(r'D:\chromedriver')
# # chrome_options = webdriver.ChromeOptions()
# # chrome_options.add_argument('--headless')#无界面
# # browser=webdriver.Chrome(chrome_options=chrome_options)
# browser=webdriver.Chrome()
# url='http://www.aigu5.com/cccb/000718.html                                       '
# browser.get(url)

# time.sleep(5)

# data_all=[]

# # htmlregex=re.compile('<a href="(.*)"')

# html=browser.page_source
# soup = BeautifulSoup(html, 'html.parser')#内置的标准库进行网页的解析


##################################################################################

# from selenium.webdriver.common.keys import Keys
# # driver = webdriver.Chrome()
# gp_code='300059'
# # browser.find_element_by_id('page_code').send_keys(gp_code)  #在搜索框中输入"selenium"
# input=browser.find_element_by_id('page_code')#
# input.clear()
# input.send_keys('300059')
# # browser.find_element_by_class_name('button').click()#回车
# browser.find_element_by_xpath('//input[@value="查 询"]').click()
# time.sleep(3)