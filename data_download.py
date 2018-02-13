#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 14 22:07:17 2018

@author: ayx
"""

#import importlib
#importlib.reload('gateAPI')

from gateAPI import GateIO


# 填写 APIKEY APISECRET
apikey = ''
secretkey='' 
API_URL = 'data.gate.io'


# Data download script
import http.client
import urllib
import json
from hashlib import sha512
import hmac
import pandas as pd
import time

conn = http.client.HTTPSConnection(API_URL, timeout=30)

def get1000tradedata(url):
    #返回从[TID]往后的最多1000历史成交记录：
    conn.request("GET",url )
    response = conn.getresponse()
    data = response.read().decode('utf-8')
    data=json.loads(data)
    if data['result']=='true':
        df=pd.DataFrame(data['data'])
    else:
        print('Error at url', url)
    return df

def get80tradedata(param):
    URL = "/api2/1/tradeHistory"
    conn.request("GET",URL+ '/' + param)
    response = conn.getresponse()
    data = response.read().decode('utf-8')
    data=json.loads(data)
    if data['result']=='true':
        df=pd.DataFrame(data['data'])
    else:
        print('Error at param', param)
    return df


## get past 10000 trade data for a certain pair
conn = http.client.HTTPSConnection(API_URL, timeout=10)
# first get the most recent tradeID
#paramlst=['bcd_usdt','bnty_usdt','eos_usdt','eth_usdt']
#paramlst=['nas_usdt','trx_usdt','fil_usdt']
# param='bcd_usdt'
paramlst=['eth_usdt']
for param in paramlst:
    print('start param: ' + param)
    emptycnt=0
    for i in range(100000):
        if i==0:
            #df_i=get80tradedata(param)
            #trdid_i=min(df_i.tradeID)
            trdid_i='1'
            #print('starting trade id: ' , trdid_i)
            df=get1000tradedata('http://data.gate.io/api2/1/tradeHistory/'+param + '/' + trdid_i )
            trdid_i=str(int(max(df.tradeID))+1)
            #print('ending trade id: ' , str(int(trdid_i)-1))
        else:
            #print('starting trade id: ' , trdid_i)
            df_i=get1000tradedata('http://data.gate.io/api2/1/tradeHistory/'+param + '/' + trdid_i )
            if df_i.empty==False:
                trdid_i=str(int(max(df_i.tradeID))+1)
                #print('ending trade id: ' , str(int(trdid_i)-1))
                #print('')
                df=df.append(df_i)
            else:
                emptycnt+=1
                if emptycnt>5:
                    print('stop pulling data for ' + param + '\n')
                    break
                else:
                    print('tradeid: ' + trdid_i + ' for param: ' + param + ' and next 1000 trades is empty')
                    print('i equals :' + str(i) + '\n')
        time.sleep(0.2)
    print('max df date' , max(df.date))
    print('min df date' , min(df.date))
    df.to_pickle(param + '_tradehist.pkl')


#df.to_pickle('ada_usdt_tradehist.pkl')
#df.to_pickle('nas_usdt_tradehist.pkl')
