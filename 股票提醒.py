# -*- coding: utf-8 -*-
"""
Created on Sun Apr  9 09:49:24 2023

@author: Viki
"""

import mysql.connector as sql
import datetime 
import pandas as pd
from datetime import date
import time
from time import strftime
from time import gmtime     
today=date.today()



from zyq_create_or_run_sql import *
df_remind=zyq_create_run_sql('stock_rec_holder.sql', 'stock_rec_holder')








#检查是否有重复数据

from datetime import date  
today=date.today()

import pymysql
conn = pymysql.connect(host="localhost", user='root', password='ZXCVBNM123', database='science')
cursor = conn.cursor()

sql="""DELETE  FROM `stock_rec_holder_all`
where `推荐更新日期`='%s';
create table stock_rec_holder_all_t as
select *from (
select * from stock_rec_holder
union all
select * from stock_rec_holder_all) t
where t.推荐更新日期 >  date_add(CURRENT_DATE(), interval -90 day);

drop table  stock_rec_holder_all;
create table stock_rec_holder_all as  select * from stock_rec_holder_all_t;
drop table  stock_rec_holder_all_t;


"""%(today)

sql_list = sql.split(';')
for i in range(len(sql_list)):
    time.sleep(1)
    try:
        cursor.execute(sql_list[i])
        
        
    except:
        pass
        continue
    
# 提交事务
conn.commit()
# 关闭游标
cursor.close()
# 关闭数据库连接
conn.close()
    
print("已检查stock_rec_holder_all是否有重复数据,上传最新数据")






#保存表
io=r'C:\Users\Viki\Desktop\python\python路径文件（勿动）\股票\email\%s股票提醒.xlsx'%(today)
df_remind.to_excel(io)


from zyq_send_email import *
title=str(today)+'股票提醒'
send_email(title,io)