# -*- coding: utf-8 -*-
"""
Created on Sat Aug 20 16:55:36 2022

@author: Viki
"""

import mysql.connector as sql
import pandas as pd
from datetime import date
import time
from time import strftime
from time import gmtime

db_connection = sql.connect(host='localhost', database='science', 
user='root', password='ZXCVBNM123')
sql="""select * from stock_hic_s """
df= pd.read_sql(sql, con=db_connection)
gp=df.股票代码.tolist()

gp = list(set(gp))
df_hiclabel=pd.DataFrame()  




#寻找时间范围 #使用max 和min的那两个
import mysql.connector as sql
db_connection = sql.connect(host='localhost', database='science', 
user='root', password='ZXCVBNM123')
sql_time="""SELECT
	t.`股票名称`,
	T.`股票代码`,
	MAX( T.`日期` ) 最新日期,
	MAX( T.`日期` ) + 1 最新起始日期,
	MAX( T.`日期` ) - 7 最新日期开始回滚,
	min( t.`日期` )+0 最初更新日期,
	CURRENT_DATE()-1 昨天日期
FROM
	`stock_hic_s` t 
GROUP BY
	t.`股票名称`,
	T.`股票名称`;"""
df_time= pd.read_sql(sql_time, con=db_connection)


def time_range_f(df_time,gp_code):#存在该股票时
    ex_key=gp_code in df_time.股票代码.values
    if ex_key == True:
        #从获得日期的明天开始当起点
        update_std_1=str(df_time[df_time['股票代码']==gp_code]['最新起始日期'].iloc[0] )
        #从当前日期往前num天作为起点 默认7天，请在sql代码中改
        update_std_2=str(df_time[df_time['股票代码']==gp_code]['最初更新日期'].iloc[0] )
        #从当前日期往前num天作为起点 默认7天，请在sql代码中改
        update_end=str(df_time[df_time['股票代码']==gp_code]['昨天日期'].iloc[0] )
    else:#不存在该股票时
        update_std_1='20210101' 
        #从当前日期往前num天作为起点 默认7天，请在sql代码中改
        update_std_2=(datetime.datetime.now()-datetime.timedelta(days=1)).strftime("%Y%m%d")
        #截止昨天
        update_end=(datetime.datetime.now()-datetime.timedelta(days=1)).strftime("%Y%m%d")
    return update_std_1,update_std_2,update_end



today=date.today()

star=time.time()#计算结束时间
number=0#计算完成率
for gp_code in gp:
    # gp_code='002340'
    
    dfa=df[df['股票代码']==gp_code]

        
    dfa1=dfa[['日期','收盘']]
    df_ad=dfa1.describe()
          
    df_ad1=df_ad.T
    df_ad1['股票代码']=dfa.iloc[0,1]
    df_ad1['股票名称']=dfa.iloc[0,0]

    df_hiclabel=pd.concat([df_hiclabel,df_ad1],axis=0)
    
    number+=1
    aroumt=format(number/len(gp), '.4%')
    process=time.time()#计算结束时间 
    process_time= process-star
    process_time=strftime("%H:%M:%S", gmtime(process_time))
    print('已完成'+str(gp_code)+'，用时'+ str(process_time)+'s,完成百分比：'+aroumt)
    
    
    
    
    update_std_1,update_std_2,update_end=time_range_f(df_time,gp_code) 
    
    df_hiclabel['窗口开始日期']=update_std_2
    df_hiclabel['窗口结束日期']=update_std_1
    
    
df_hiclabel['更新日期']=today

df_hiclabel['cv']=df_hiclabel['std']/df_hiclabel['mean']


df_hiclabel=df_hiclabel.reindex(['股票代码', '股票名称','更新日期','count', 'mean', 'std', 'min', '25%', '50%', '75%', 'max','cv','窗口开始日期','窗口结束日期'], axis = 1)


from zyq_df_mysql import *
df_mysql(df_hiclabel,'stock_hic_label','append')
    
    
    
    