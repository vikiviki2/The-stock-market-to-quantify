# -*- coding: utf-8 -*-
"""
Created on Tue Dec 13 20:28:39 2022

@author: Viki
"""

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
import time
import os
import json
import requests
import re
import random

url_list=['https://item.yiyaojd.com/100017559686.html'
          ,'https://item.yiyaojd.com/100017559634.html'
          ,'https://item.jd.com/10032355610436.html'
          ,'https://item.yiyaojd.com/3156948.html'
          ,'https://item.jd.com/10035264383091.html'
          ,'https://item.jd.com/10042468001301.html'
          ,'https://item.jd.com/10067085110663.html'
          ,'https://item.yiyaojd.com/4094591.html'
          ,'https://item.yiyaojd.com/100006152321.html']


#url='https://item.yiyaojd.com/100017559686.html'



url_compare=''

keep=0
while True:
    for url in url_list:
        os.chdir(r'D:\chromedriver')
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')#无界面
        browser=webdriver.Chrome(chrome_options=chrome_options)
        #print(url)
        browser.get(url)
        browser.implicitly_wait(6)
        try:
            browser.find_element_by_id('InitCartUrl').click()#回车#
            url_next=browser.current_url
            url_next=url_next.replace('#none','')
            if url_next==url:
                print("none")
                keep=0 
                time.sleep(4)
                
            else :

                print("successfully find it")
                print(url)
                from zyq_send_msg import *
                send_msg("买药啊:"+url)
                keep =1
                browser.close()
        except:
             pass
    time.sleep(random.randint(3*60,10*60))
    if keep==1:
        break


