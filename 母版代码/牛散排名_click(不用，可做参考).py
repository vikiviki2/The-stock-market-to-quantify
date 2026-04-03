# -*- coding: utf-8 -*-
"""
Created on Tue Sep  6 20:34:52 2022

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


from time import strftime
from time import gmtime
from selenium.webdriver.common.keys import Keys
os.chdir(r'D:\chromedriver')
# chrome_options = webdriver.ChromeOptions()
# chrome_options.add_argument('--headless')#无界面
# browser=webdriver.Chrome(chrome_options=chrome_options)
browser=webdriver.Chrome()
url='http://cwzx.shdjt.com/top500.asp'
browser.get(url)
 
time.sleep(5)

data_all=[]


#################################################################################
#进入
from selenium.webdriver.common.keys import Keys
# driver = webdriver.Chrome()
holder='赵建平'
# browser.find_element_by_id('page_code').send_keys(gp_code)  #在搜索框中输入"selenium"
input=browser.find_element_by_xpath('//input[@class="inputtext"]')
input.clear()
input.send_keys(holder)
# browser.find_element_by_class_name('button').click()#回车
browser.find_element_by_xpath('//input[@class="input2"]').click()
time.sleep(3)


html=browser.page_source
soup = BeautifulSoup(html, 'html.parser')#内置的标准库进行网页的解析


print('suessful')


