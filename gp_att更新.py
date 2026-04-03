# -*- coding: utf-8 -*-
"""
Created on Sun Mar 27 16:36:40 2022

@author: Viki
"""
import pandas as pd

#由自己添加的代码

gp_num=pd.read_excel(r'C:\Users\Viki\Desktop\python\python路径文件（勿动）\股票\att\关注的股票自加.xlsx'
                     ,converters = {'股票代码':str}
                     ,header=0)

#从数据库添加的代码
import mysql.connector as sql
#由人圈排行榜添加的代码
db_connection = sql.connect(host='localhost', database='science', 
user='root', password='ZXCVBNM123')
sql="""select distinct t.`股票代码`,t.`股票名称` from rqlhb_new t """
df_rqlhb= pd.read_sql(sql, con=db_connection)


#实现周累计龙虎榜
from time import time,localtime,strftime
x=localtime(time())
a=strftime("%a",x)
df_rqlhb.set_index('股票代码',inplace=True)
df_rqlhb['股票名称']=df_rqlhb['股票名称'].apply(lambda i :i.replace(" ",""))
df_rqlhb.to_excel(r'C:\Users\Viki\Desktop\python\python路径文件（勿动）\股票\att\关注的股票%s.xlsx'%(a))


#%%寻找文件夹下同后缀
import os, re    
# 获取目录下的全部文件名称
file_names = os.listdir('C:/Users/Viki/Desktop/python/python路径文件（勿动）/股票/att/')
# 使用列表解析式遍历文件名并组成数组，使用正则表达式匹配文件名称
data_all=pd.DataFrame()
for i  in file_names:
    df_gd=pd.read_excel(r'C:/Users/Viki/Desktop/python/python路径文件（勿动）/股票/att/%s'%(i),converters = {'股票代码':str}
                     ,header=0)
    print(i)
    data_all=pd.concat([data_all,df_gd],axis=0)

data_all['股票名称']=data_all['股票名称'].apply(lambda i :i.replace(" ",""))

# new_name_1 = name.drop_duplicates(subset='name1',keep='last') #有名字重复的



#%%将牛散股票同时放入

#从数据库添加的代码
import mysql.connector as sql
#由人圈排行榜添加的代码
db_connection = sql.connect(host='localhost', database='science', 
user='root', password='ZXCVBNM123')
sql="""select distinct t.股票代码,t.`股票名称`
from stock_holder_detail_dup_new t
where t.动作 like '%增加%' 
AND substr( t.`股票名称`, 1, 2 ) <> '退市' 
AND substr( t.`股票名称`, 1, 2 ) <> 'ST' 
and t.`股票代码` not in (select DISTINCT 股票代码  from stock_holder_detail t
where 动作  like '%减少%'  
and `更新日期` >  SUBDATE(curdate(),30 ))"""
df_retail= pd.read_sql(sql, con=db_connection)

data_all=pd.concat([data_all,df_retail],axis=0)#添加到主表

print('将牛散股票放入')

#%%将最近推荐过得股票也同时放入

import mysql.connector as sql
import numpy as np
from datetime import date
import datetime
import time
from time import strftime
from time import gmtime
import os, re 
import pandas as pd   


import pymysql
conn = pymysql.connect(host="localhost", user='root', password='ZXCVBNM123', database='science')
cursor = conn.cursor()



# 收集历史数据
# 获取目录下的全部文件名称
file_names_1 = os.listdir('C:/Users/Viki/Desktop/python/python路径文件（勿动）/股票/email/')

#选取近30日最新推荐
file_names=file_names_1[-30:]

#储存推荐信息
data_rec=pd.DataFrame()


#获取当前观察的样本
for i  in file_names:
    try:
        df_gd=pd.read_excel(r'C:/Users/Viki/Desktop/python/python路径文件（勿动）/股票/email/%s'%(i)
                        ,converters = {'股票代码':str}
                         ,header=0)
        df_gd1=df_gd[[ '股票代码', '股票名称']]
             
        data_rec=pd.concat([data_rec,df_gd1],axis=0)
    except:
        print(i+':error')
        pass


 #删除空行
data_rec = data_rec.dropna() 

data_all=pd.concat([data_all,data_rec],axis=0)#添加到主表

print('将最近30日推荐的股票放入')

#%%

data_all=data_all.drop_duplicates(subset='股票代码',keep='last')#去重
data_all = data_all.apply(lambda x: x.str.strip())


sql="""drop table `science`.`gp_att`;
CREATE TABLE `science`.`gp_att`  (
  `股票代码` varchar(255) NULL,
  `股票名称` varchar(255) NULL
  
)"""

import pymysql
conn = pymysql.connect(host="localhost", user='root', password='ZXCVBNM123', database='science')
cursor = conn.cursor()

sql_list = sql.split(';')
for i in range(len(sql_list)):
    time.sleep(1)
    try:
        cursor.execute(sql_list[i])
        
        print('成功运行'+str(i))
    except:
        from zyq_send_msg import *
        send_msg('不成功运行建立gp_att,错误代码：'+str(i))
        pass
        continue



from zyq_df_mysql import *
df_mysql(data_all,'gp_att','append')

from zyq_send_msg import *
send_msg('已完成att更新')