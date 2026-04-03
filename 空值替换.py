# -*- coding: utf-8 -*-
"""
Created on Fri Feb  3 20:15:31 2023

@author: Viki
"""

import pandas as pd
import akshare as ak
import matplotlib.pyplot as plt
from datetime import date   
import time
import random
from time import strftime
from time import gmtime
#%%抽取推荐代码
import mysql.connector as sql
today=date.today()
import datetime
#编写一个时间查询函数
import mysql.connector as sql



#先从底表将时间都找出来
import mysql.connector as sql
db_connection = sql.connect(host='localhost', database='science', 
user='root', password='ZXCVBNM123')
sql_time="""select *from recommend_new2
where 观察日价格= 0
or 观察日价格 is null;"""
df_replace= pd.read_sql(sql_time, con=db_connection)

df_r=df_replace=[['股票名称','股票代码']]

df_hic=pd.DataFrame()

for i in df_replace.index:
    print(i)

    replace_date=df_replace.iloc[i,4]
    replace_code=df_replace.iloc[i,1]
    stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=replace_code, period="daily", 
                                    start_date=replace_date, 
                                    end_date=replace_date, adjust="qfq")
    
    df_a=stock_zh_a_hist_df[['日期','收盘']]
    df_a['股票代码']=replace_code
    df_hic=pd.concat([df_hic,df_a],axis=0)
    
df_hic=pd.merge(df_hic,df_r,how='left',on='股票代码')

df_hic = df_hic.reindex(['股票代码', '股票名称','日期', '收盘'], axis = 1)
    

            
