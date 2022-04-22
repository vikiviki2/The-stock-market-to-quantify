# -*- coding: utf-8 -*-
"""
Created on Wed Feb 23 20:58:39 2022

@author: Viki
"""
#%% 加签 通用写法
import time
import hmac
import hashlib
import base64
import urllib.parse

timestamp = str(round(time.time() * 1000))
secret = 'SEC7a6fa1592881d7611bb09791cafc6e66266bb6769bc1dd5ab9b4aa39d9f88828'
secret_enc = secret.encode('utf-8')
string_to_sign = '{}\n{}'.format(timestamp, secret)
string_to_sign_enc = string_to_sign.encode('utf-8')
hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
# print(timestamp)
# print(sign)

# 拼接url得到签名
# webhook_url='https://oapi.dingtalk.com/robot/send?access_token=c6fe0b1aecad756815de05fb1bf6a68027631257e85341da3c9c000a54f926e9&timestamp={timestamp}&sign={sign}'.format(timestamp=timestamp,sign=sign)
url='https://oapi.dingtalk.com/robot/send?access_token=c6fe0b1aecad756815de05fb1bf6a68027631257e85341da3c9c000a54f926e9'
webhook_url=url+'&timestamp={timestamp}&sign={sign}'.format(timestamp=timestamp,sign=sign)



#%%  加签后钉钉写法
import requests
import time
import json
request_params = {
"headers": {
"User-Agent":
"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"
},
"timeout": 10
}

def send_msg(text,webhook_url):
    r = requests.post(webhook_url, json={
    "msgtype": "text",
    "text": {
    "content": text
    },
        "at":{
            # "atMobiles":[
            #     "13132051130"       #需要填写自己的手机号，钉钉通过手机号@对应人
            # ],
            "isAtAll": True        #是否@所有人，默认否
        }
    }, **request_params)
    
    
   
    
    
    
    
#%%开始取数  
    

import requests
import time
import json


def get_today_bonds():
    r = requests.get("http://data.hexin.cn/ipo/bond/cate/info/",
    **request_params)
    for bond in r.json():
        # print(f"{bond['zqName']}: 申购日期:{bond['sgDate']}  溢价率{bond['disRate']} {bond['today']} {bond['sgDate']}")
    #if bond['sgDate']=='2021-11-29':#测试用
        if bond['today'] == bond['sgDate']:
            text = f"""今日打新: {bond['zqName']} 发行量{bond['issue']}亿  """
            send_msg(text,webhook_url)
            continue
            
 

text= get_today_bonds()







