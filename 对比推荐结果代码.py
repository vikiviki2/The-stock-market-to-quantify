# -*- coding: utf-8 -*-
"""
Created on Thu Oct 13 17:14:21 2022

@author: Viki
"""

import mysql.connector as sql
import pandas as pd
import numpy as np
from datetime import date
import datetime
import time
from time import strftime
from time import gmtime

#获得进行比较的df #可调整调查时间
def rc_df(days):
    today=date.today()
    select_day = today - datetime.timedelta(days=days)
    
    #导入需要比较的excel
    io=r'C:\Users\Viki\Desktop\python\python路径文件（勿动）\股票\email'
    df_rc=pd.read_excel(io+'\%s股票提醒.xlsx'%(select_day),converters={'股票代码':str})
    return  df_rc




#获取需要计算的日期
def date_list(rc_date,days_end):
    start_day=str(rc_date+ datetime.timedelta(days=1))[0:10:1]#fix
    end_day = str(rc_date+ datetime.timedelta(days=days_end))[0:10:1]#填写n天后的结束日期
    
    #返回两个日期简的日期
    df_date=pd.date_range(start_day, end_day, freq='B').tolist()#放入开始日期与结束日期参数
    return  df_date


#获得进行比较的df
df_rc=rc_df(15)


#获取需要计算的日期
rc_date=df_rc.iloc[0,1]

df_date=date_list(rc_date,7)



df_rc_s=df_rc[['日期', '股票代码', '股票名称', '最新价']]
for i in df_date:
    a=str(i)[0:10:1]
    test_date=a.replace('-', '_')
    print (test_date)
    #进行循环的东西
    import mysql.connector as sql
    db_connection = sql.connect(host='localhost', database='science', 
    user='root', password='ZXCVBNM123')
    sql="""select t.`股票代码`,t.今开 比较%s from stock_dm_fresh_detail1 t
    where t.`日期`='%s'"""%(test_date,test_date)
    df_a= pd.read_sql(sql, con=db_connection)
    df_rc_s=pd.merge(df_rc_s,df_a,"left",on='股票代码')
    



#对该表进行加工

#获得用于比较的历史最新价
gg=df_rc_s[['最新价']].values.tolist()
gg =sum(gg,[]) 

#获得每一期的实际价格
df_rc_s_c=df_rc_s.copy()
df_sub21=df_rc_s_c.set_index(['日期', '股票代码', '股票名称','最新价'])


#开始进行矩阵运算-减法
df_sub=df_sub21.sub(gg,axis=0)

#开始进行矩阵运算-涨跌幅
df_div=df_sub.div(gg,axis=0)

#转置结果对模型进行打分
df_score=df_div.T
df_score=df_score.astype(float)


mask = df_score.gt(0)
df_score['正分数']= df_score.where(mask).sum(1)
df_score['负分数']= df_score.mask(mask).sum(1)
df_score['分值']=df_score.apply(lambda x:x.sum(),axis=1)-df_score['正分数']-df_score['负分数']


#保留两份小数
df_score_a=df_score.round(2)
