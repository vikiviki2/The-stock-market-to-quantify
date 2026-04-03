
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  6 20:06:18 2022

@author: Viki
"""
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
import time
import os
# import json
# import requests
import re
import random
# import datetime 

from time import strftime
from time import gmtime
# from multiprocessing import Process

#%%
import mysql.connector as sql
db_connection = sql.connect(host='localhost', database='science', 
user='root', password='ZXCVBNM123')
sql="""select t.`股票代码`,t.`股票名称` from gp_att t """
df_gp= pd.read_sql(sql, con=db_connection)
gp_code=df_gp['股票代码'].values.tolist()

#将上述的gp_code列表尽量均匀分为四份
def round_robin_sublists(l, n=2):
    lists = [[] for _ in range(n)]
    i = 0
    for elem in l:
        lists[i].append(elem)
        i = (i + 1) % n
    return lists

gp_code = round_robin_sublists(gp_code) 



#%%
star=time.time()#计算结束时间
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

from datetime import date     
today=date.today()


def change_df(df):
    arr=df.values
    new_df = pd.DataFrame(arr[1:,1:], index=arr[1:,0], columns=arr[0,1:])
    new_df.index.name = arr[0,0]
    new_df.reset_index(inplace=True)
    return new_df



def fetch(gp_code):
    global df_all
    number=0#计算完成率
    for gp in gp_code:
        try:
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
            browser.implicitly_wait(4)
            time.sleep(random.randint(3,8))
            html=browser.page_source
            soup = BeautifulSoup(html, 'html.parser')#内置的标准库进行网页的解析
            #开始收集数据
            
            #may be need to modify
            tbody=soup.find('div',class_='brief_info_c').tbody.find_all('td')#
            dataRegex=re.compile(r'([\u4e00-\u9fa5]*)：.*?class=.*?>(.*?)<')#,,<td.*?>(.*?)</td>
            

            data=dataRegex.findall(str(tbody))
            
            
            # for child in  tbody.children:
            #     for chi in child:
            #         try:
            #             print(chi.text)
            #         except:
            #             pass
                    
            
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
    
            df_all=pd.concat([df_all,df])

            number+=1
            aroumt=format(number/len(gp_code), '.4%')
            process=time.time()#计算结束时间 
            process_time= process-star
            process_time=strftime("%H:%M:%S", gmtime(process_time))
            print('已完成'+str(gp)+'，用时'+ str(process_time)+'s,完成百分比：'+aroumt)
            
        except Exception as e:
                print(e)
    return df_all
    
# df_all=fetch(gp_code[1])

       
#%% 尝试过多进程不行用多线程
from threading import Thread
if __name__ == '__main__':   
    start = time.perf_counter()
    df_all=pd.DataFrame()    

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
        


df_l=df_all.copy()

#把对不上的行全删掉
df_l.dropna(axis=1,inplace=True)

from zyq_send_msg import *
if df_l.shape[1]!=14:
    a=df_l.shape[1]
    send_msg('捕获股票详细信息列数异常，现有'+str(a)+'行')
 
#整理下表里面的内容

def str2value(valueStr):#转化为数字，把亿与万换成1  #这里可能有bug
    valueStr = str(valueStr)
    idxOfYi = valueStr.find('亿')
    idxOfWan = valueStr.find('万')
    idxOfWanYi = valueStr.find('万亿')
    
    if idxOfWanYi>=0: 
        return int(float(valueStr[:idxOfWanYi])*1e12)
    elif idxOfYi != -1 and idxOfWan != -1:
        a=int(float(valueStr[:idxOfYi])*1e8 + float(valueStr[idxOfYi+1:idxOfWan])*1e4)
    elif idxOfYi != -1 and idxOfWan == -1:
        return int(float(valueStr[:idxOfYi])*1e8)
    elif idxOfYi == -1 and idxOfWan != -1:
        return int(float(valueStr[idxOfYi+1:idxOfWan])*1e4)
    elif idxOfYi == -1 and idxOfWan == -1:
        return float(valueStr)
    
    
    
def bivalue(valueStr):
     return str2value(valueStr)/100000000   #转化为亿
 
def mivalue(valueStr):
     return str2value(valueStr)/10000  #转化为万

# valueStr='2.381万亿'
# idxOfWanYi = valueStr.find('万亿')
# if idxOfWanYi>=0: 
#     print('s')
# else:
#     print('f')


df_l=df_l[~df_l.isin(['-']).any(axis=1)] #删除那些查不到数值的数据

df_l['换手']=df_l['换手'].apply(lambda i :i.strip('%'))


df_l['流通市值'].apply(lambda i :str2value(i))

df_l['流通市值']=df_l['流通市值'].apply(lambda i :bivalue(i))
df_l['成交额']=df_l['成交额'].apply(lambda i :bivalue(i))
df_l['总市值']=df_l['总市值'].apply(lambda i :bivalue(i))


df_l['成交量']=df_l['成交量'].apply(lambda i :i.strip('手'))
df_l['成交量']=df_l['成交量'].apply(lambda i :mivalue(i))


# df_l=df_l.replace('-',0)






df_l.rename(columns={'总市值':'总市值亿'
                     ,'流通市值':'流通市值亿'
                     ,'成交额':'成交额亿'
                     ,'成交量':'成交量万手'},inplace=True)


df_l['日期']=today
df_l = df_l.reindex(['日期','股票代码', '最高', '今开', '换手', '总市值亿', '涨停', '成交量万手', '最低', '昨收', '量比', '流通市值亿', '跌停',
       '成交额亿', '市净'], axis = 1)


#有些中文字符有问题，手动删除
df_2l=df_l[~df_l['涨停'].isin(['盘后成交量：'])]

from zyq_df_mysql import *
df_mysql(df_2l,'stock_weekly_detial','append')

from zyq_send_msg import *
send_msg('今日股票详细信息已更新')