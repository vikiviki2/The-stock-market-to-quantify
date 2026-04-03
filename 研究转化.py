# -*- coding: utf-8 -*-
"""
Created on Sat Jan  6 13:13:02 2024

@author: Viki
"""
import numpy as np
import matplotlib.pyplot as plt

import pandas as pd
import mysql.connector as sql


db_connection = sql.connect(host='localhost', database='science', 
user='root', password='ZXCVBNM123')
sql="""select * from stock_hic_s t """
df_gp= pd.read_sql(sql, con=db_connection)
df_gp['date'] = pd.to_datetime(df_gp['日期'])

df_gp.set_index('date', inplace=True)


df_gp1=df_gp[(df_gp['股票代码']=='002906')]

#%%算其平移曲线
# 计算移动平均值
moving_avg = df_gp1["收盘"].rolling(window=180).mean() # 选取周期为7天的移动平均
 
# 绘制原始数据及移动平均曲线
plt.figure()
plt.title("Trend Line")
plt.xlabel("Date")
plt.ylabel("Value")
plt.plot(df_gp1.index, df_gp1["收盘"], label="Original Data")
plt.plot(df_gp1.index, moving_avg, color='red', linestyle='--', label="Moving Average")
plt.legend()
plt.show()


#%%计算可能收益率
