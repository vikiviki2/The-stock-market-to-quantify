# -*- coding: utf-8 -*-
"""
Created on Sun Sep  4 16:40:30 2022

@author: Viki
"""
import mysql.connector as sql
import pandas as pd
db_connection = sql.connect(host='localhost', database='science', 
user='root', password='ZXCVBNM123')
sql="""SELECT
	*
FROM
	`stock_hic_s` t """
df= pd.read_sql(sql, con=db_connection)

df1=df.copy()

df1.drop_duplicates(subset = ['股票代码', '股票名称', '日期', '收盘'],
                   keep = 'first', 
                   inplace = True, 
                   ignore_index = False)


from zyq_df_mysql import *
df_mysql(df1,'stock_hic_s','replace')



