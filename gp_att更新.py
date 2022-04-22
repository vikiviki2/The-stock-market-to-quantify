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
sql="""select distinct t.`股票代码`,t.`股票名称` from rqlhb t """
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
data_all=data_all.drop_duplicates()



#
from zyq_df_mysql import *
df_mysql(data_all,'gp_att','replace')

from zyq_send_msg import *
send_msg('已完成att更新')