# -*- coding: utf-8 -*-
"""
Created on Tue Sep  6 21:37:28 2022

@author: Viki
"""

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver import ChromeOptions
from selenium.webdriver.chrome.service import Service
from lxml import etree
import time
import os
# import json
# import requests
import re
import random
# import datetime 
# from selenium.webdriver.support.ui import WebDriverWait
from time import strftime
from time import gmtime
from threading import Thread
# import threading
from lxml import etree
from lxml.html import fromstring,tostring



# df_hoder_name='全国社保基金'
# url='http://cwzx.shdjt.com/cwcx.asp?gdmc=%C9%E7%B1%A3%BB%F9%BD%F0'


#df_hoder_name='张武'
#url='http://cwzx.shdjt.com/cwcx.asp?gdmc=%D5%C5%CE%E4'

df_hoder_name='重庆建工'
url='http://cwzx.shdjt.com/cwcx.asp?gdmc=%D6%D8%C7%EC%BD%A8%B9%A4'

print(url)
os.chdir(r'D:\chromedriver')
chrome_options = webdriver.ChromeOptions()
#chrome_options.add_argument('--headless')#无界面
#browser=webdriver.Chrome(chrome_options=chrome_options)
browser=webdriver.Chrome()
print('开始')
browser.get(url)
browser.implicitly_wait(4)
time.sleep(random.randint(3,8))
html=browser.page_source
print('开始1')
#开始收集数据

#截取某部分节点的代码，找出其table所在节点的代码
selector=etree.HTML(html)
content=selector.xpath("//table[@class='tb0td1']//tbody")[0]
original_html=tostring(content)


#对所在节点的代码寻找某个分快
html_str=str(original_html)
listRegex=re.compile(r'onmouseover="this.style.backgroundColor.*?height="25"')
gp_info_list=listRegex.findall(html_str)
print('开始2')
   




columns=['排名','股票代码','股票名称','散户大家庭','千古千评','视讯大家庭','股东','F10','原价','现价','区间涨幅','股东类型','更新日期','持股数_万','市值_亿','比例','类型','动作','数量_万股']        



n_list=len(gp_info_list)
df_result=pd.DataFrame()
for n in range (0,n_list):
    try:
        import html
        uncode_html = html.unescape(gp_info_list[n]) 
        #'排名','股票代码','股票名称'
        Regex1=re.compile(r'<td>(\d*)</td><td>(\d*)</td>.*?class=.*?>(.*?)<.*?href',re.S)
        gp_info_item1=Regex1.findall(uncode_html)
        gp_info1=pd.DataFrame(gp_info_item1)
        
        #'散户大家庭','千古千评','视讯大家庭','股东','F10'
        Regex2=re.compile(r'href="(.*?)".*?href="(.*?)".*?href="(.*?)".*?href="(.*?)".*?href="(.*?)"',re.S)
        gp_info_item2=Regex2.findall(uncode_html)
        gp_info2=pd.DataFrame(gp_info_item2)
        
        #'原价','现价','区间涨幅','股东类型','更新日期'
        Regex3=re.compile(r'>F10</font></a></td><td>(.*?)</td><td>(.*?)</td>.*?>(.*?)</td><td>(.*?)</td><td>(.*?)</td>',re.S)
        gp_info_item3=Regex3.findall(uncode_html)
        gp_info3=pd.DataFrame(gp_info_item3)
        
        #'持股数（万）','市值（亿）','比例','类型'
        # Regex4=re.compile(r'>%s<.*?><td>(.*?)</td><td>(.*?)</td><td>(.*?)</td><td>(.*?)</td><td title="(.*?)">'%(df_hoder_name),re.S)
        Regex4=re.compile(r'tdleft1.*?</td><td>(.*?)</td><td>(.*?)</td><td>(.*?)</td><td>(.*?)</td><td title="(.*?)">',re.S)
        gp_info_item4=Regex4.findall(uncode_html)
        gp_info4=pd.DataFrame(gp_info_item4)
        gp_info4=gp_info4.drop(2,axis=1)#删除拿不到数据的那一列
        
        #'动作','数量（万股）'
        Regex5=re.compile(r'tdleft1.*?<td class=".*?">(.*?)</td><td class=".*?">(.*?)</td></tr><tr height="25"',re.S)
        gp_info_item5=Regex5.findall(uncode_html)
        gp_info5=pd.DataFrame(gp_info_item5)
        
        gp_info=pd.concat([gp_info1,gp_info2,gp_info3,gp_info4,gp_info5],axis=1)

        df_result=pd.concat([df_result,gp_info],axis=0)
    except Exception as e:
                print (e)
                print(n)
                print(gp_info1.iloc[0,2])
                pass
                continue
    


    
df_result.columns = columns


#整理数据，放入数据库
from datetime import date  
today=date.today()
df_result['数据入库日期']=today

#删去百分号
df_result['区间涨幅']=df_result['区间涨幅'].apply(lambda i :i.strip('%'))

#添加股东名称
df_result['股东名称']=df_hoder_name

#替换空值为0
df_result=df_result.fillna(0)

df_result=df_result.replace('',0)

#from zyq_df_mysql import *
#df_mysql(df_result,'stock_holder_detail','append')



# from zyq_send_msg import *
# send_msg('已成功捕捉%s的信息，已经成功运行代码'%(df_hoder_name))
print('已成功捕捉%s的信息，已经成功运行代码'%(df_hoder_name))

    
