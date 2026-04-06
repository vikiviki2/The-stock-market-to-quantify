# -*- coding: utf-8 -*-
"""
Created on Sun Feb 12 11:43:38 2023

@author: Viki
"""

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from lxml import etree
import time
import os
# import json
# import requests
import re
import random
# import datetime 
# from selenium.webdriver.support.ui import WebDriverWait
from time import strftime
from time import gmtime
from threading import Thread
# import threading
from lxml import etree
from lxml.html import fromstring,tostring

star=time.time()#计算结束时间

#%%从gp_att 抽取代码

import mysql.connector as sql
db_connection = sql.connect(host='localhost', database='science', 
user='root', password='ZXCVBNM123')
sql="""select t.`股票代码`,t.`股票名称` from gp_att t  """
df_gp= pd.read_sql(sql, con=db_connection)
gp_code=df_gp['股票代码'].values.tolist()

#将上述的gp_code列表尽量均匀分为四份
def round_robin_sublists(l,n=5):
    lists = [[] for _ in range(n)]
    i = 0
    for elem in l:
        lists[i].append(elem)
        i = (i + 1) % n
    return lists

gp_code = round_robin_sublists(gp_code) 


#gp='600500'


def find_data(gp):
    urlpre='http://qgqp.shdjt.com/gpdm.asp?gpdm='
    url=urlpre+gp
    #print(url)
    driver_path = r'D:\edgedriver\msedgedriver.exe'
    options = webdriver.EdgeOptions()
    options.add_argument('--headless=new')#无界面
    options.add_argument('--disable-gpu')
    service = Service(driver_path)
    browser = webdriver.Edge(service=service, options=options)
    # browser=webdriver.Edge(service=service, options=options)

    browser.get(url)
    browser.implicitly_wait(4)
    time.sleep(random.randint(3,8))
    html=browser.page_source

    #开始收集数据
    
    #截取某部分节点的代码，找出其table所在节点的代码
    selector=etree.HTML(html)
    content=selector.xpath("//table[@class='tb0td1']//tbody")[0]
    original_html=tostring(content)
    
    
    #对所在节点的代码寻找某个分快
    html_str=str(original_html)
    listRegex=re.compile(r'onmouseover="this.style.backgroundColor.*?height="25"')
    gp_info_list=listRegex.findall(html_str)
    browser.quit()
    return gp_info_list


#gp_info_list=find_data(gp)





columns=['排名','日期','股票名称','comment']
def list_table(gp_info_list,columns):
    n_list=len(gp_info_list)
    df_result=pd.DataFrame()
    for n in range (0,n_list):
        try:
            import html
            uncode_html = html.unescape(gp_info_list[n]) 
            
            Regex1=re.compile(r'<td>(.*?)</td><td>(.*?)</td><td class=".*?">(.*?)<.*?<td class=".*?">(.*?)</td><td',re.S)
            gp_info_item1=Regex1.findall(uncode_html)
            gp_info1=pd.DataFrame(gp_info_item1)
            
            
    
            df_result=pd.concat([df_result,gp_info1],axis=0)
        except Exception as e:
                    # print (e)
                    # print(n)
                    # print(gp_info1.iloc[0,2])
                    pass
                    continue
        
    
    
        
    df_result.columns = columns
    return df_result
            

def fetch(gp_code):
    global df_allgp  
    number=0
    for gp in gp_code:
        try:
            gp_info_list=find_data(gp)
            df_result=list_table(gp_info_list,columns)
            df_result=list_table(gp_info_list,columns)
            df_result['股票代码']=gp

            from datetime import date  
            today=date.today()
            df_result['数据入库日期']=today
            df_allgp=pd.concat([df_allgp,df_result],axis=0)
            
            
            number+=1
            aroumt=format(number/len(gp_code), '.4%')
            
            process=time.time()#计算结束时间 
            process_time= process-star
            process_time=strftime("%H:%M:%S", gmtime(process_time))
            print('已完成'+str(gp)+'，用时'+ str(process_time)+'s,完成百分比：'+aroumt)
            
            
        except Exception as e:
                print(e)
                
    return df_allgp








#%% 开始多线程处理任务
from threading import Thread
if __name__ == '__main__':   
    start = time.perf_counter()
    df_allgp=pd.DataFrame()   

    thread_list = []
    for gp_t in gp_code:
         
        try:

            thread =Thread(target=fetch, args=[gp_t])
            thread.start()
            thread_list.append(thread)
        except Exception as e:
            print (e)
            pass
            continue

    for t in thread_list:
        t.join()
        
finish = time.perf_counter()     
print(f"全部任务执行完成，耗时 {round(finish - start,2)} 秒")


#%%
df_allgp['日期'].replace('年', '-', regex=True,inplace=True) 
df_allgp['日期'].replace('月', '-', regex=True,inplace=True) 
df_allgp['日期'].replace('日', '', regex=True,inplace=True) 

#df_allgp['日期'].replace('2023-2-10-', '2023-2-10', regex=True,inplace=True) 

#%%放入数据
sql="""drop table `science`.`qgqp_detail`;
CREATE TABLE `science`.`qgqp_detail`  (
  `排名` int  ,
  `日期` date ,
  `股票名称` varchar(255) NULL,
  `comment` varchar(255) NULL,
  `股票代码` varchar(255) NULL,
  `数据入库日期` date NULL

)"""
import pymysql
conn = pymysql.connect(host="localhost", user='root', password='ZXCVBNM123', database='science')
cursor = conn.cursor()

sql_list = sql.split(';')
for i in range(len(sql_list)):
    time.sleep(1)
    try:
        cursor.execute(sql_list[i])
    except:
        pass
        continue



from zyq_df_mysql import *
df_mysql(df_allgp,'qgqp_detail','append')



end=time.time()#计算结束时间 
use_time= end-star
use_time=strftime("%H:%M:%S", gmtime(use_time))


from zyq_send_msg import *
send_msg('已完成qgqp_new更新,'+ str(use_time)+'s')
