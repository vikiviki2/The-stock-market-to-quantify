# -*- coding: utf-8 -*-
"""
Created on Sun Sep  3 16:08:58 2023

@author: Viki
"""

#更新牛散排名
import sys
sys.path.append(r"C:\Users\Viki\Desktop\python\stock代码仓库")
from 牛散排名  import * 



import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver import ChromeOptions
from selenium.webdriver.chrome.service import Service
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



#%%假设重跑先将今日数据删除

from datetime import date  
today=date.today()

import pymysql
conn = pymysql.connect(host="localhost", user='root', password='ZXCVBNM123', database='science')
cursor = conn.cursor()

sql="""DELETE  FROM `stock_holder_detail`
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
    
print("已检查stock_holder_detail是否有重复数据")





#控制观察的数量
rk=500
rk_list=[n  for n in range(1,rk+1)]

#将上述的gp列表尽量均匀分为四份用于后面多线程
def round_robin_sublists(l, n=4):
    lists = [[] for _ in range(n)]
    i = 0
    for elem in l:
        lists[i].append(elem)
        i = (i + 1) % n
    return lists

rk_list_all = round_robin_sublists(rk_list) 



import mysql.connector as sql
db_connection = sql.connect(host='localhost', database='science', 
user='root', password='ZXCVBNM123')
sql="""select * from retail_in_rank_new
limit %s """%(rk)
df_hoder= pd.read_sql(sql, con=db_connection)


#收集转换过名字的人
df_rename =pd.read_excel(r'C:\Users\Viki\Desktop\python\python路径文件（勿动）\股票\dic\name_dic.xlsx'
                     ,header=0)


#收集未成功收集的人
df_hoder_miss_name=[]
df_hoder_miss_net=[]


    
star=time.time()#计算结束时间
number=0#计算完成率

def hoder_find(n,df_hoder):   
    df_hoder_c=df_hoder.iloc[n-1:n,:]
    df_hoder_rank=str(df_hoder_c.iloc[0,2])
    #df_hoder_sta=str(df_hoder_c.iloc[0,1])
    df_hoder_net=str(df_hoder_c.iloc[0,0])
    df_hoder_name=str(df_hoder_c.iloc[0,1])
    #df_hoder_date=str(df_hoder_c.iloc[0,4])
    
    #有些名字不会完全对的上
    if df_hoder_name in df_rename.name_be.values:
        df_hoder_name=df_rename[(df_rename['name_be']==df_hoder_name)].values[0][1]
    
    return df_hoder_rank,df_hoder_net,df_hoder_name


def find_data(df_hoder_net):
    url_pre='http://cwzx.shdjt.com/'
    url=url_pre+df_hoder_net
    #print(url)
    os.chdir(r'D:\chromedriver')
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')#无界面
    browser=webdriver.Chrome(chrome_options=chrome_options)
    #browser=webdriver.Chrome()

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

    return gp_info_list





#对每个分块节点进行查找其各列数据
columns=['排名','股票代码','股票名称','散户大家庭','千古千评','视讯大家庭','股东','F10','原价','现价','区间涨幅','股东类型','更新日期','持股数_万','市值_亿','比例','类型','动作','数量_万股']    
def list_table(gp_info_list,columns):
    n_list=len(gp_info_list)
    df_result=pd.DataFrame()
    for n in range (0,n_list):
        try:
            import html
            uncode_html = html.unescape(gp_info_list[n]) 
            #'排名','股票代码','股票名称'
            Regex1=re.compile(r'<td>(\d*)</td><td>(\d*)</td>.*?class=.*?>(.*?)<.*?href',re.S)
            gp_info_item1=Regex1.findall(uncode_html)
            gp_info1=pd.DataFrame(gp_info_item1)
            
            #'散户大家庭','千古千评','视讯大家庭','股东','F10'
            Regex2=re.compile(r'href="(.*?)".*?href="(.*?)".*?href="(.*?)".*?href="(.*?)".*?href="(.*?)"',re.S)
            gp_info_item2=Regex2.findall(uncode_html)
            gp_info2=pd.DataFrame(gp_info_item2)
            
            #'原价','现价','区间涨幅','股东类型','更新日期'
            Regex3=re.compile(r'>F10</font></a></td><td>(.*?)</td><td>(.*?)</td>.*?>(.*?)</td><td>(.*?)</td><td>(.*?)</td>',re.S)
            gp_info_item3=Regex3.findall(uncode_html)
            gp_info3=pd.DataFrame(gp_info_item3)
            
            #'持股数（万）','市值（亿）','比例','类型'
            # Regex4=re.compile(r'>%s<.*?><td>(.*?)</td><td>(.*?)</td><td>(.*?)</td><td>(.*?)</td><td title="(.*?)">'%(df_hoder_name),re.S)
            Regex4=re.compile(r'tdleft1.*?</td><td>(.*?)</td><td>(.*?)</td><td>(.*?)</td><td>(.*?)</td><td title="(.*?)">',re.S)
            gp_info_item4=Regex4.findall(uncode_html)
            gp_info4=pd.DataFrame(gp_info_item4)
            gp_info4=gp_info4.drop(2,axis=1)#删除拿不到数据的那一列
            
            #'动作','数量（万股）'
            Regex5=re.compile(r'tdleft1.*?<td class=".*?">(.*?)</td><td class=".*?">(.*?)</td></tr><tr height="25"',re.S)
            gp_info_item5=Regex5.findall(uncode_html)
            gp_info5=pd.DataFrame(gp_info_item5)
            
            gp_info=pd.concat([gp_info1,gp_info2,gp_info3,gp_info4,gp_info5],axis=1)
    
            df_result=pd.concat([df_result,gp_info],axis=0)
        except Exception as e:
                    # print (e)
                    # print(n)
                    # print(gp_info1.iloc[0,2])
                    pass
                    continue
        
    
    
        
    df_result.columns = columns
    return df_result



#global 


df_all_result=pd.DataFrame()
def fetch(rk_list_part):
    global df_all_result
    star=time.time()#计算结束时间
    number=0#计算完成率
    for rk1 in rk_list_part:
        try:
            df_hoder_rank,df_hoder_net,df_hoder_name=hoder_find(rk1 ,df_hoder)
    
            gp_info_list=find_data(df_hoder_net)
      
            df_result=list_table(gp_info_list,columns)
            
            
            
            
            #整理数据，放入数据库
            from datetime import date  
            today=date.today()
            df_result['数据入库日期']=today
            
            #删去百分号
            df_result['区间涨幅']=df_result['区间涨幅'].apply(lambda i :i.strip('%'))
            
            #添加股东名称
            df_result['股东名称']=df_hoder_name
            
            #替换空值为0
            df_result=df_result.fillna(0)
            
            df_result=df_result.replace('',0)
            df_all_result=pd.concat([df_all_result,df_result],axis=0)
    
            
            
    
            #print('已成功捕捉%s的信息，已经成功运行代码'%(df_hoder_name))
            number+=1
            aroumt=format(number/len(rk_list_part), '.4%')
            process=time.time()#计算结束时间 
            process_time= process-star
            process_time=strftime("%H:%M:%S", gmtime(process_time))
            
            print('已成功捕捉%s的信息，用时'%(df_hoder_name)+ str(process_time)+'s,完成百分比：'+aroumt+'。' )
        except Exception as e:
            
                  print (e)
                  #number+=1
                  print('未捕捉%s的信息，网址如下，用时'%(df_hoder_name)+ str(process_time)+'s,完成百分比：'+aroumt+'。' )
                  print('http://cwzx.shdjt.com/%s'%(df_hoder_net))
                  df_hoder_ap_net='http://cwzx.shdjt.com/%s'%(df_hoder_net)
                  
                  
                  
                  
                  df_hoder_miss_name.append(df_hoder_name)
                  df_hoder_miss_net.append(df_hoder_ap_net)
                  
                  
                  pass
                  continue
                







#fetch(2)


#%%开始进行多线程
from threading import Thread
if __name__ == '__main__':   
    start = time.perf_counter()

    df_all_result=pd.DataFrame()
    # df_hiclabel=pd.DataFrame()    
    

    thread_list = []
    
    for rk_list_part in rk_list_all :
        try:
            thread =Thread(target=fetch, args=[rk_list_part])
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





 #%%整理下数据


df_all_result['区间涨幅'].replace(',', '', regex=True,inplace=True) 




end=time.time()#计算结束时间 
use_time= end-star
use_time=strftime("%H:%M:%S", gmtime(use_time))

from zyq_send_msg import *
send_msg('已成功获取牛散，已经成功运行代码,'+ str(use_time)+'s')


#在检查是记得注释这一行，不要重复放入数据
from zyq_df_mysql import *
df_mysql(df_all_result,'stock_holder_detail','append')


from zyq_send_msg import *
send_msg('已成功获取牛散，已经成功运行代码,'+ str(use_time)+'s')




    
