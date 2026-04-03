# -*- coding: utf-8 -*-
"""
Created on Sun Oct  8 20:50:15 2023

@author: Viki
"""

#%%寻找文件夹下同后缀
import pandas as pd
import os, re    
# 获取目录下的全部文件名称
file_names = os.listdir('C:/Users/Viki/Desktop/python/python路径文件（勿动）/股票/email/')
# 使用列表解析式遍历文件名并组成数组，使用正则表达式匹配文件名称
data_all=pd.DataFrame()
for i  in file_names:
    try:
        df_email=pd.read_excel(r'C:/Users/Viki/Desktop/python/python路径文件（勿动）/股票/email/%s'%(i),converters = {'股票代码':str}
                         ,header=0)
        print(i)
        data_all=pd.concat([data_all,df_email],axis=0)
    except:
        pass
        continue
    
data_extract=data_all.iloc[:,1:]


# organize the format
#替换空值为0
data_extract=data_extract.fillna(0)


#find the exsit table structure
def reindex_table(data_extract,table_name):   
    #data_extract which table need to be reindexed
    #table_name    follow this table struture
    import mysql.connector as sql
    db_connection = sql.connect(host='localhost', database='science', 
    user='root', password='ZXCVBNM123')
    sql_s="""SELECT * FROM %s  limit 1"""%(table_name)
    df_s= pd.read_sql(sql_s, con=db_connection)
    df_s_c=df_s.columns
    data_extract_new = data_extract.reindex(df_s_c, axis = 1)
    return data_extract_new

data_extract_new=reindex_table(data_extract,'stock_rec_holder_all')






#delete the exist one and recreate a new one
import pymysql
conn = pymysql.connect(host="localhost", user='root', password='ZXCVBNM123', database='science')
cursor = conn.cursor()

sql="""DELETE  FROM stock_rec_holder_all;
"""

sql_list = sql.split(';')
for i in range(len(sql_list)):
    try:
        cursor.execute(sql_list[i])
        print("已删除stock_rec_holder_all")
        
    except:
        pass
        continue
    
# 提交事务
conn.commit()
# 关闭游标
cursor.close()
# 关闭数据库连接
conn.close()
    
from zyq_df_mysql import *
df_mysql(data_extract_new,'stock_rec_holder_all','append')


