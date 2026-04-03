# -*- coding: utf-8 -*-
"""
Created on Wed Jun 15 17:41:57 2022

@author: Viki
"""

import pandas as pd
import akshare as ak
import matplotlib.pyplot as plt
from datetime import date   
import time
import random
from time import strftime
from time import gmtime
#%%抽取推荐代码
import mysql.connector as sql
today=date.today()
import datetime
#编写一个时间查询函数
import mysql.connector as sql


#先从底表将时间都找出来
import mysql.connector as sql
db_connection = sql.connect(host='localhost', database='science', 
user='root', password='ZXCVBNM123')
sql_time="""SELECT
	t.`股票名称`,
	T.`股票代码`,
	DATE_FORMAT(date_add(MAX( T.`日期` ) , interval +1 day),'%Y%m%d') 最新起始日期,
	DATE_FORMAT(date_add(MAX( T.`日期` ) , interval -7 day),'%Y%m%d') 最新日期开始回滚,
	DATE_FORMAT(date_add(min( T.`日期` ) , interval +0 day),'%Y%m%d') 最初更新日期,
	DATE_FORMAT(date_add(CURRENT_DATE() , interval -1 day),'%Y%m%d') 昨天日期
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
        #从当前日期往前num天作为起点 默认7天，请在sql代码中改 （代码中不使用）
        update_std_2=str(df_time[df_time['股票代码']==gp_code]['最新日期开始回滚'].iloc[0] )
        #从当前日期往前num天作为起点 默认7天，请在sql代码中改
        update_end=str(df_time[df_time['股票代码']==gp_code]['昨天日期'].iloc[0] )
    else:#不存在该股票时
        update_std_1='20210101' 
        #从当前日期往前num天作为起点 默认7天，请在sql代码中改
        update_std_2=(datetime.datetime.now()-datetime.timedelta(days=1)).strftime("%Y%m%d")
        #截止昨天
        update_end=(datetime.datetime.now()-datetime.timedelta(days=1)).strftime("%Y%m%d")
    return update_std_1,update_std_2,update_end


update_std_1,update_std_2,update_end=time_range_f(df_time,'000001')




 

#寻找需要查找的代码
db_connection = sql.connect(host='localhost', database='science', 
user='root', password='ZXCVBNM123')
sql="""select distinct t.`股票代码`,t.`股票名称` from gp_att t """
df_remind= pd.read_sql(sql, con=db_connection)
df_r=df_remind[['股票名称','股票代码']]
gp=df_r.股票代码.tolist()





#将上述的gp列表尽量均匀分为四份用于后面多线程
def round_robin_sublists(l, n=6):
    lists = [[] for _ in range(n)]
    i = 0
    for elem in l:
        lists[i].append(elem)
        i = (i + 1) % n
    return lists

gp_t = round_robin_sublists(gp) 



#%%开始循环提取数据


star=time.time()#计算结束时间



df_hic=pd.DataFrame()

def fetch(gp):
    global df_hic
    # global df_hiclabel
    number=0#计算完成率
    for gp_code in gp:
        time.sleep(random.randint(3,30))
        
        update_std_1,update_std_2,update_end=time_range_f(df_time,gp_code)
        stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=gp_code, period="daily", 
                                            start_date=update_std_1, 
                                            end_date=update_end, adjust="qfq")
            
        
  
        # stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=gp_code, period="daily", 
        #                                         start_date=update_std_1, 
        #                                         end_date=update_end, adjust="qfq")
        #{"hfq",'qfq',''}后复权前复权，不复权,股票价格一般采取前复权
        #下面统一采取去盘价
        
        
        df_a=stock_zh_a_hist_df[['日期','收盘']]
        df_a['股票代码']=gp_code
        
        df_hic=pd.concat([df_hic,df_a],axis=0)
        
        
        number+=1
        aroumt=format(number/len(gp), '.4%')
        process=time.time()#计算结束时间 
        process_time= process-star
        process_time=strftime("%H:%M:%S", gmtime(process_time))
        print('已完成'+str(gp_code)+'，用时'+ str(process_time)+'s,完成百分比：'+aroumt)
    
        #%%开始画图
        #设置图标格式
        # plt.rcParams['font.sans-serif']=['SimHei'] #指定默认字体
        # plt.rcParams['axes.unicode_minus']=False  #解决保存图像时符号-显示为方块的2问题
        # print(1)
        
        # #totalSeed 为对应的日期列
        # totalSeed = df_a.日期.tolist()
        # fig1, ax = plt.subplots()
        # ax.plot(totalSeed, df_a.loc[:,'收盘'])
        
        # plt.legend(loc='best')
        
        # xticks=list(range(0,len(totalSeed),40))
        # xlabels=[totalSeed[x] for x in xticks]  
        # xticks.append(len(totalSeed))
        # xlabels.append(totalSeed[-1])
        # ax.set_xticks(xticks)
        # ax.set_xticklabels(xlabels, rotation=90)
        # # ax.xaxis.set_major_locator(ticker.MultipleLocator(40))
        # plt.title("收盘价"+'-mean:{:.2f}-std:{:.2f}-cv:{:.2f}'.format(mean,std,cv))    
    
    return df_hic










#%%开始进行多线程
from threading import Thread
if __name__ == '__main__':   
    start = time.perf_counter()

    df_hic=pd.DataFrame()
    # df_hiclabel=pd.DataFrame()    
    

    thread_list = []
    
    for gp_code in gp_t :
        try:
            thread =Thread(target=fetch, args=[gp_code])
            thread.start()
            thread_list.append(thread)
        except Exception as e:
            print (e)
            pass
            continue

    for t in thread_list:
        t.join()
        
finish = time.perf_counter()     
print(f"全部任务执行完成，耗时 {round(finish - start,2)} 秒")














df_hic=pd.merge(df_hic,df_r,how='left',on='股票代码')




df_hic = df_hic.reindex(['股票代码', '股票名称','日期', '收盘'], axis = 1)


df_hic['股票名称']=df_hic['股票名称'].apply(lambda x:x.strip())

df_hic = df_hic.reindex(['股票代码', '股票名称','日期', '收盘'], axis = 1)

#放入数据库
from zyq_df_mysql import *
df_mysql(df_hic,'stock_hic_s','append')


end=time.time()#计算结束时间 
use_time= end-star
use_time=strftime("%H:%M:%S", gmtime(use_time))



# 放入数据库(不要每天重复跑，没有另外设置去重)
# from zyq_df_mysql import *
# df_mysql(df_hiclabel,'stock_hic_label','append')


from zyq_send_msg import *
send_msg('已成功捕捉历史收盘信息，已经成功运行代码，用时'+ str(use_time)+'s')





import sys
sys.path.append(r"C:\Users\Viki\Desktop\python\stock代码仓库")
from 变异系数分析 import * 


from zyq_send_msg import *
send_msg('完成变异系数分析')



