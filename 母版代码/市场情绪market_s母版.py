# -*- coding: utf-8 -*-
"""
Created on Sun Aug 14 09:44:03 2022

@author: Viki
"""
from lxml import etree
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver import ChromeOptions
from selenium.webdriver.chrome.service import Service

url='http://quote.eastmoney.com/sz002340.html'

options = webdriver.ChromeOptions()
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--no-sandbox')
options.add_argument('--headless')
options.add_argument('--ignore-certificate-errors')
browser = webdriver.Chrome(r'chromedriver', options=options)

browser.get(url)
browser.implicitly_wait(2)
# time.sleep(random.randint(3,8))

html=browser.page_source
# soup = BeautifulSoup(html, 'html.parser')#内置的标准库进行网页的解析
# clean_text = BeautifulSoup(html).get_text()

#获取其股票名称及
selector=etree.HTML(html)
gp_up=str(selector.xpath("//span[@class='percentbar price_up_bg']/following::span/text()""")[0])
gp_down=str(selector.xpath("//span[@class='percentbar price_up_bg']/following::span/text()""")[1])









