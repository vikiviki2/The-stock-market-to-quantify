# -*- coding: utf-8 -*-
"""
Created on Thu Oct 13 12:01:13 2022

@author: Viki
"""

import akshare as ak
from datetime import date

today=date.today()


#东方财富A股实时价格
stork_dm=ak.stock_zh_a_spot_em()


#添加日期
stork_dm['日期']=today
#删除多余的列
stork_dm=stork_dm.drop('序号',axis=1) 
stork_dm=stork_dm.drop('市盈率-动态',axis=1) 

#重新排序
stork_dm= stork_dm.reindex(['日期', '代码', '名称', '最新价', '涨跌幅', '涨跌额', '成交量', '成交额', '振幅', '最高', '最低',
       '今开', '昨收', '量比', '换手率', '市盈率-动态','市净率'], axis = 1)

#改名字
stork_dm.rename(columns={'代码':'股票代码'},inplace=True)
stork_dm.rename(columns={'名称':'股票名称'},inplace=True)
stork_dm.rename(columns={'市盈率-动态':'市盈率_动态'},inplace=True)

stork_dm=stork_dm.drop('市盈率_动态',axis=1) 

#替换空值
stork_dm1=stork_dm.fillna(0) 

#放入数据库
from zyq_df_mysql import *
df_mysql(stork_dm1,'stock_dm_fresh_detail1','append') #该数据源已经关闭



#新数据源

stork_dm_other=ak.stock_zh_a_spot()

stork_dm_other.rename(columns={'代码':'股票代码'},inplace=True)
stork_dm_other.rename(columns={'名称':'股票名称'},inplace=True)

#添加日期
stork_dm_other['日期']=today
from zyq_df_mysql import *
df_mysql(stork_dm_other,'stock_dm_fresh_detail2','append')


from zyq_send_msg import *
send_msg('已成功更新fresh detail')