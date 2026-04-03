# -*- coding: utf-8 -*-
"""
Created on Sat Nov 19 11:34:42 2022

@author: Viki
"""
import mysql.connector as sql

import numpy as np
from datetime import date
import datetime
import time
from time import strftime
from time import gmtime
import os, re 
import pandas as pd   



#
import pymysql
conn = pymysql.connect(host="localhost", user='root', password='ZXCVBNM123', database='science')
cursor = conn.cursor()


star=time.time()#计算结束时间

#%%1.收集历史数据
# 获取目录下的全部文件名称
file_names = os.listdir('C:/Users/Viki/Desktop/python/python路径文件（勿动）/股票/email/')
# 使用列表解析式遍历文件名并组成数组，使用正则表达式匹配文件名称
data_all=pd.DataFrame()


#获取需要计算的日期
def date_list(rc_date,days_end):
    start_day=str(rc_date+ datetime.timedelta(days=1))[0:10:1]#fix
    end_day = str(rc_date+ datetime.timedelta(days=days_end))[0:10:1]#填写n天后的结束日期
    
    #返回两个日期简的日期
    df_date=pd.date_range(start_day, end_day, freq='B').tolist()#放入开始日期与结束日期参数
    return  df_date


#获取当前观察的样本
for i  in file_names:
    try:
        df_gd=pd.read_excel(r'C:/Users/Viki/Desktop/python/python路径文件（勿动）/股票/email/%s'%(i)
                        ,converters = {'股票代码':str}
                         ,header=0)
        print(i)
        df_gd1=df_gd[['日期', '股票代码', '股票名称', '最新价']]
       
        #观察未来14天的情况
        rc_date=df_gd1.iloc[0,0]
        df_date=date_list(rc_date,14) 

        df_date_t=pd.DataFrame(data=df_date,columns=['观察日期'])
        
        #将日期调整为年月日
        df_date_t['观察日期'] = df_date_t['观察日期'].astype('str').str.slice(0, 11)
        df_gd1['日期'] = df_gd1['日期'].astype('str').str.slice(0, 11)
        
        #制作假key
        df_gd1['key']="key"
        df_date_t['key']="key"
        df_gd_new=df_gd1.merge(df_date_t,"inner",on="key")     
        
        #删除空行
        df_gd_new = df_gd_new.dropna()    
        
        
        data_all=pd.concat([data_all,df_gd_new],axis=0)
    except:
        print(i+':error')
        pass


 #删除空行
data_all = data_all.dropna() 

data_all=data_all.drop('key',axis=1)




sql="""drop table `science`.`recommend_new`;
CREATE TABLE `science`.`recommend_new`  (
  `日期` date NULL,
  `股票代码` varchar(255) NULL,
  `股票名称` varchar(255) NULL,
  `最新价` float(255, 2) NULL,
  `观察日期` date NULL
  
)"""

sql_list = sql.split(';')
for i in range(len(sql_list)):
    time.sleep(1)
    try:
        cursor.execute(sql_list[i])
    except:
        pass
        continue




#将历史推荐数据收集完成
from zyq_df_mysql import *
df_mysql(data_all,'recommend_new','append')
print("成功创建recommend_new,插入历史推荐数据")

#%%在数据库中获取观察日期数据

import pymysql
conn = pymysql.connect(host="localhost", user='root', password='ZXCVBNM123', database='science')
cursor = conn.cursor()

sql="""drop table science.recommend_new1;
CREATE TABLE science.recommend_new1 AS
with x1 as(
select a.*,B.`最新价` 观察日价格 FROM recommend_new a left join stock_dm_fresh_detail1 b
on a.股票代码=b.股票代码
and a.观察日期=b.日期),
x2 as(
select x1.日期,x1.股票代码 ,x1.股票名称, x1.最新价,x1.观察日期
,b.`收盘` 观察日价格 from x1 
left join stock_hic_s b
on x1.股票代码=b.股票代码
and x1.观察日期=b.日期
where  x1.观察日价格 is null),
 X3 AS(
select x2.日期,x2.股票代码 ,x2.股票名称, x2.最新价,x2.观察日期
,b.`今开` 观察日价格 from x2
left join stock_dm_fresh_detail2 b
on x2.股票代码=substr(b.股票代码,3,9)
and x2.观察日期=b.日期
where  x2.观察日价格 is null)
SELECT * FROM x1 WHERE X1.观察日价格 IS NOT NULL
UNION 
SELECT * FROM x2 WHERE X2.观察日价格 IS NOT NULL
UNION
SELECT * FROM x3 
ORDER BY 1,2,5"""

sql_list = sql.split(';')
for i in range(len(sql_list)):
    time.sleep(1)
    try:
        cursor.execute(sql_list[i])
    except:
        pass
        continue
print("成功替换recommend_new1,补充观察日价格")

#读取在sql中成功加工的表
import mysql.connector as sql
db_connection = sql.connect(host='localhost', database='science', 
user='root', password='ZXCVBNM123')
sql="""SELECT *,
dense_rank() over(partition by t.日期,t.股票代码 order by t.观察日期 asc ) as rk  
FROM `recommend_new1` T"""
df_re= pd.read_sql(sql, con=db_connection)



#针对重复的rk去重

df_re_d=df_re.drop_duplicates(subset = ['日期', '股票代码', '股票名称', '最新价', '观察日期', 'rk'],
                   keep = 'first', 
                   inplace = False, 
                   ignore_index = False)

#%%获取观察日期价格结束，开始加工展示用的表


#%%%1.将未unstack 的表放入mysql
df_re_d_sql=df_re_d.copy()
df_re_d_sql['rk']=df_re_d_sql['rk'].apply(lambda x:"观察日期"+str(x))

df_re_d_sql.rename(columns={'日期':'推荐日期'
                            ,'最新价':'推荐时价格'
                            ,'观察日期':'详细观察日期'
                            ,'rk':'观察日期'},inplace=True)



sql="""drop table `science`.`recommend_new2`;
CREATE TABLE `science`.`recommend_new2`  (
  `推荐日期` date NULL,
  `股票代码` varchar(255) NULL,
  `股票名称` varchar(255) NULL,
  `推荐时价格` float(255, 2) NULL,
  `详细观察日期` date NULL,
  `观察日价格` float(255, 2) NULL,
   `观察日期`varchar(255) NULL
 
)"""

sql_list = sql.split(';')
for i in range(len(sql_list)):
        time.sleep(1)
        try:
            cursor.execute(sql_list[i])
        except:
            pass
            continue




#将历史推荐数据收集完成
from zyq_df_mysql import *
df_mysql(df_re_d_sql,'recommend_new2','append')
print("成功创建recommend_new2,整理好未stack的表")




#%%2.对表进行unstack 加工sub 与 div



df_re_d1=df_re_d.drop('观察日期',axis=1)






df_re_d2=df_re_d1.set_index(['日期', '股票代码', '股票名称', '最新价','rk'])



df_re_d3=df_re_d2.unstack()

df_re_d4=df_re_d3.copy()

df_re_d4=df_re_d4.reset_index(3)
gg=df_re_d4[['最新价']].values.tolist()
gg =sum(gg,[]) 


#开始进行矩阵运算-减法
df_re_sub=df_re_d3.sub(gg,axis=0)



#开始进行矩阵运算-涨跌幅
df_re_div=df_re_d3.div(gg,axis=0)
df_re_div2=df_re_div.sub(1,axis=0)




#转置结果对模型进行打分
df_re_div2['总数']=df_re_div2.apply(lambda x:x.mean(),axis=1)
df_re_sub['总数']=df_re_sub.apply(lambda x:x.mean(),axis=1)
df_re_d3['总数']=df_re_d3.apply(lambda x:x.mean(),axis=1)


#保留两位小数
df_re_sub2=df_re_sub.round(2)
df_re_div3=df_re_div2.round(2)
df_re_d3=df_re_d3.round(2)



#整理好数据放入数据库

#整理索引
df_re_sub_s=df_re_sub2.reset_index()
df_re_div_s=df_re_div3.reset_index()
df_re_d3_s=df_re_d3.reset_index()

df_re_sub_s1=df_re_sub_s.droplevel(level=0,axis=1)
df_re_div_s1=df_re_div_s.droplevel(level=0,axis=1)
df_re_d3_s1=df_re_d3_s.droplevel(level=0,axis=1)


#制作字典不再手动赋值(如果观察阈值超过99要调整字典)
dic_c = {i:'观察日期'+str(i) for i in range(100)}



#根据字典替换列名
df_re_sub_s1.rename(columns=dic_c,inplace=True)

df_re_div_s1.rename(columns=dic_c,inplace=True)

df_re_d3_s1.rename(columns=dic_c,inplace=True)



#将可以成功替换的部分找出来，再拼接成大的list
sub_c=df_re_sub_s1.columns
sub_c=sub_c.to_list()
sub_c= list(filter(None, sub_c))
all_c=['推荐日期', '股票代码', '股票名称', '推荐时价格']+sub_c+['总数']

#将列名全部替换
df_re_sub_s1.columns =all_c

df_re_div_s1.columns =all_c

df_re_d3_s1.columns = all_c


#整理日期

df_re_sub_s1['推荐日期'] = df_re_sub_s1['推荐日期'].astype('str').str.slice(0, 11)
df_re_div_s1['推荐日期'] = df_re_div_s1['推荐日期'].astype('str').str.slice(0, 11)
df_re_d3_s1['推荐日期'] = df_re_div_s1['推荐日期'].astype('str').str.slice(0, 11)

df_re_sub_s1['类型']='差值'
df_re_div_s1['类型']='百分比'
df_re_d3_s1['类型']='平均值'


df_re_all=pd.concat([df_re_sub_s1,df_re_div_s1,df_re_d3_s1],axis=0)


















#将历史推荐数据收集完成

#删除历史表
sql="""drop table `science`.`recommend_new3`)"""

sql_list = sql.split(';')
for i in range(len(sql_list)):
        try:
            cursor.execute(sql_list[i])
        except:
            pass
            continue



#这种方式不需要建表直接dataframe 导入！
# if_exists:
# 1.fail:如果表存在，啥也不做
# 2.replace:如果表存在，删了表，再建立一个新表，把数据插入
# 3.append:如果表存在，把数据插入，如果表不存在创建一个表！！
from sqlalchemy import create_engine
engine = create_engine('mysql+pymysql://root:ZXCVBNM123@localhost:3306/science')

# 将新建的DataFrame储存为MySQL中的数据表，储存index列

df_re_all.to_sql('recommend_new3', con=engine, if_exists='replace')

print("成功创建recommend_new3,div&sub")





#将历史推荐数据收集完成


# from zyq_df_mysql import *
# df_mysql(df_re_all,'recommend_new3','append')
# print("成功创建recommend_new3,div&sub")

process=time.time()#计算结束时间 
process_time= process-star
process_time=strftime("%H:%M:%S", gmtime(process_time))



print('用时'+ str(process_time)+'s')