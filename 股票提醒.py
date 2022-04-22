# -*- coding: utf-8 -*-
"""
Created on Sat Apr 16 15:26:59 2022

@author: Viki
"""

import mysql.connector as sql
import datetime 
import pandas as pd
from datetime import date     
today=date.today()

db_connection = sql.connect(host='localhost', database='science', 
user='root', password='ZXCVBNM123')
sql="""with  x1 as (SELECT
	t.`日期`,
	t1.`股票名称`,
	t.`股票代码`,
	t1.`看跌`,
	t1.看涨,
	case when t2.排名 is null then 101 else t2.排名 end 排名,
	t.今开,
	t.`今开` - t3.`全部` 低于全部持仓,
	t.`换手` 
FROM
	stock_daily_detial_new t
	LEFT JOIN market_s_new t1 ON t.`股票代码` = t1.`股票代码`
	LEFT JOIN rqlhb_new t2 ON t.`股票代码` = t2.`股票代码`
	LEFT JOIN cyq_new t3 ON t.`股票代码` = t3.`股票代码`) 
	select * from x1
	order by x1.排名 """

df_remind= pd.read_sql(sql, con=db_connection)
io=r'C:\Users\Viki\Desktop\python\python路径文件（勿动）\股票\email\%s股票提醒.xlsx'%(today)
df_remind.to_excel(io)


from zyq_send_email import *
title=str(today)+'股票提醒'
send_email(title,io)


from zyq_send_msg import *
send_msg('已成功发送邮件')
