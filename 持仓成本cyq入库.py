    # -*- coding: utf-8 -*-
"""
Created on Sun Feb 27 08:48:27 2022

@author: Viki
"""
#获取数据
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
import time
import os
import json
import requests
import re
import random


from time import strftime
from time import gmtime

star=time.time()#计算结束时间 

import mysql.connector as sql
db_connection = sql.connect(host='localhost', database='science', 
user='root', password='ZXCVBNM123')
sql="""select distinct t.`股票代码` from gp_att t """
df_gp= pd.read_sql(sql, con=db_connection)
gp=df_gp['股票代码'].values.tolist()
# gp=['002194', '002280', '002285', '002287', '002326', '002330', '002336', '002370', '002424', '002466', '002486', '002565', '002566', '002613', '002644', '002657', '002803', '002864', '002907', '002910', '002915', '300209', '300464', '300582', '300612', '300636', '600007', '600067', '600077', '600082', '600127', '600137', '600156', '600207', '600313', '600340', '600511', '600512', '600598', '600603', '600630', '600665', '600689', '600698', '600731', '600860', '600866', '601012', '601088', '601318', '601607', '601952', '603056', '603058', '603070', '603108', '603188', '603219', '603335', '603399', '603538', '603999', '605138', '605507']

#%%

df_allgp=pd.DataFrame()
url_pre='http://www.aigu5.com/cccb/'
# gp=['600982', '002176', '000862', '600396', '002432']#测试用
# gp=['600000']
number=0
for gp_code in gp:
    try:
        url=url_pre+gp_code +'.html'
        os.chdir(r'D:\chromedriver')
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')#无界面
        browser=webdriver.Chrome(chrome_options=chrome_options)
        # browser=webdriver.Chrome()
        browser.get(url)
        time.sleep(random.randint(3,8))
        
        #获取网页信息
        html=browser.page_source
        soup = BeautifulSoup(html, 'html.parser')#内置的标准库进行网页的解析
        #获取其股票名称及股票代码
        gp_name=soup.find('td',class_='font').text
        #将其信息拆分到列表
        gp_info=gp_name.strip(')').split('(')
        
        #把持仓成本那部分信息找出来
        gp_df=soup.find('table',class_="lable_tab01").tbody
        gp_df_info=gp_df.find_all('tr')
        gp_df_info_columns=gp_df_info[0]
        # gp_df_info_columns.contents  #遍历成列表     
        
        # data_columns=[]
        # for child in gp_df_info_columns.children: 
        #     try:
        #         data_columns.append(child.text)
        #     except:
        #          pass #已改成一次性返回所有
        
        data_all=[]
        for child in gp_df.children:
            data=[]
            for chi in child:           
                
                try:
                      
                    data.append(chi.text)
                except:
                     pass
            if len(data) >1 :  
                data_all.append(data)
    
    
    
        data=pd.DataFrame(data_all)
        
        headers = data.iloc[0]
        new_df  = pd.DataFrame(data.values[1:], columns=headers)
        
        new_df=new_df[new_df['日期']!='平均']
        new_df['股票代码']=gp_info[1]
        new_df['股票名称']=gp_info[0]
        print('已完成'+gp_info[0])
        browser.close()#关闭浏览器
        time.sleep(random.randint(3,8))
        
        df_allgp=pd.concat([df_allgp,new_df],axis=0)
        
        number+=1
        aroumt=format(number/len(gp), '.4%') 

        
        process=time.time()#计算结束时间 
        process_time= process-star
        process_time=strftime("%H:%M:%S", gmtime(process_time))
        print('已经成功运行代码，用时'+ str(process_time)+'s,完成百分比：'+aroumt)
    except:
         pass
         continue


#整理数据，重新排列名
df_allgp = df_allgp.reindex(['日期', '股票代码', '股票名称','全部', 
                             '散户', '中户', '大户','超大','成交额','涨跌幅'], axis = 1)
#删去百分号
df_allgp['涨跌幅']=df_allgp['涨跌幅'].apply(lambda i :i.strip('%'))
#修改列名
df_allgp.rename(columns={'涨跌幅':'涨跌幅百分比'},inplace=True)

df_allgp['股票名称']=df_allgp['股票名称'].apply(lambda i :i.replace(" ",""))
#%%以后整理为只取前五个工作日的数据进去




#%%开始对数据入库
import pymysql
# 打开数据库连接
db = pymysql.connect(host='127.0.0.1',
                     user='root',
                     port=3306,
                     password='ZXCVBNM123',
                     database='science',
                     charset='utf8')



import pandas as pd
import pymysql
from sqlalchemy import create_engine
from sqlalchemy import types, create_engine
 
#engine = create_engine("mysql+pymysql://user:password@host:port/databasename?charset=utf8") 
engine = create_engine("mysql+pymysql://root:ZXCVBNM123@127.0.0.1:3306/science?charset=utf8") 


from sqlalchemy.dialects.oracle import \
            BFILE, BLOB, CHAR, CLOB, DATE, \
            DOUBLE_PRECISION, FLOAT, INTERVAL, LONG, NCLOB, \
            NUMBER, NVARCHAR, NVARCHAR2, RAW, TIMESTAMP, VARCHAR, \
            VARCHAR2
            
#此函数次包可用于导表
def mapping_df_types(df):
		    dtypedict = {}
		    for i, j in zip(df.columns, df.dtypes):   
		        if "object" in str(j):
		            dtypedict.update({i: VARCHAR(20)}) #可以在这里对字符串的长度进行修改
		        if "float" in str(j):
		            dtypedict.update({i: NUMBER(19,0)})
		        if "int" in str(j):
		            dtypedict.update({i: VARCHAR(19)})
		    return dtypedict




dtypedict = mapping_df_types(df_allgp)

#到时候需要更改为 "tmp_repeat_zyq_cipimp_20210519"
# df_allgp.to_sql("cyq",engine,index=False,if_exists='append',dtype=dtypedict,chunksize=None) 

    
# df_allgp.to_sql(name ='cyq',con = engine,if_exists = 'append',index = False,index_label = False)
df_allgp.to_sql("cyq",engine,index=False,if_exists='append',dtype=dtypedict,chunksize=None)  
    
    
#%%删除库中重复数据
sql="""create table tmp  as 
    select distinct *  from cyq;
    drop table cyq;
    ALTER table  tmp rename cyq; """

sql_list = sql.split(';')   


 
# 使用 execute()  方法执行 SQL 查询 
for i in range(len(sql_list)):
    c=db.cursor()# 使用 cursor() 方法创建一个游标对象 cursor
    time.sleep(1)
    try:
        c.execute(sql_list[i])
    except:
        	pass
        	continue

end=time.time()#计算结束时间 
use_time= end-star
use_time=strftime("%H:%M:%S", gmtime(use_time))
from zyq_send_msg import *
send_msg('已成功获取持仓成本，已经成功运行代码，用时'+ str(use_time)+'s')
    
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

#%%

