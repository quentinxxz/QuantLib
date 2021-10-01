#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 21 15:14:53 2021

@author: quentinxxz
"""

#!/usr/bin/env python3

# -*- coding: utf-8 -*-

"""

Created on Sun Sep 19 20:42:39 2021

@author: quentinxxz

"""

import pandas as pd
import datetime  
import matplotlib.pyplot as plt



# 数据准备处理

def prepareBeixiangData():

    #爬下来的北向资金数据
    df = pd.read_csv('beixiang_20211001.csv')
    
    df['datetime']= pd.to_datetime(df['datetime'] ,format="%Y-%m-%d")
    
    # 1表示沪股通， 3为深股通, 4为
    df_1 = df[df['marketType'] == 1]
    df_3 = df[df['marketType'] == 3]
    
    df = pd.merge(df_1,df_3,suffixes=('_1', '_3'),how='left',on='datetime' )


    # 合并沪股通与深股通数据 
    df['total_in']=df['total_in_1']+df['total_in_3']
    df['total_out']=df['total_out_1']+df['total_out_3']
    df['total_net_in']=df['total_net_in_1']+df['total_net_in_3']
    df['grand_total_in']=df['grand_total_in_1']+df['grand_total_in_3']
    df['today_balance']=df['today_balance_1']+ df['today_balance_3']
    df=df.loc[:,['datetime','total_in','total_out','total_net_in','grand_total_in','today_balance']]
    
    
    df= df_1
    
    # 按日期排序
    df.set_index('datetime',drop=False,append=False,inplace=True)
    df.sort_index(ascending=True,inplace=True)
    #df.sort_values(by='datetime',ascending=True,inplace= True) 

    # 计算布林线
    df['std'] = df['total_net_in'].rolling(100).std()
    df['mean'] = df['total_net_in'].rolling(100).mean()
    df['lower_interval'] = df['mean'] - 1.5*df['std']
    df['upper_interval'] = df['mean'] + 1.5*df['std']
        
    
    # 选取一段数据做展示
    #s_date = datetime.datetime.strptime('20190401', '%Y%m%d')
    #e_date = datetime.datetime.strptime('20200508', '%Y%m%d')

    # d_t = df[(df['datetime']<e_date) & (df['datetime']>s_date) ]
    # d_t.plot.line(x='datetime',y=['total_net_in','mean','lower_interval','upper_interval'])
    return df



# 准备沪深300指数
def prepareShangZheng():
    df= pd.read_csv('market_index_20211001.csv')
    df['datetime']= pd.to_datetime(df['datetime'])
    df.set_index('datetime',drop=False,append=False,inplace=True)
    df=df[df['code']==300]
    df.sort_index(ascending=True,inplace=True)
    return df





# 计算买卖时机，deprecated
def updateBuyAndSell(bx):  
    bx.loc[bx['lower_interval']>bx['total_net_in'], 'trade']=1
    bx.loc[bx['upper_interval']<bx['total_net_in'], 'trade']=-1

    

# 交易回测 
# bx为北向资金数据，
# benchmark为 回测参考 本例中采用上证指烽，
# cost_ration为手续费比例，默认万二， 
# init_cap为初始资金，
# delay代表 基于北向资金分析数据，几日后实施交易，至少为1天
def backTrade(bx,benchmark,cost_ration=0.0002,init_cap=10000,delay=1,trade_interval=7):
    count=0
    trade_records = pd.DataFrame(columns=['operation','trade_val','trade_vol','acc_val_end','acc_vol_end','total_value','capital','order_cost'],index=['datetime'])
    for pos in range(len(bx)) :
        #取对应的交易日
        next_trade_pos = pos+ delay
        if next_trade_pos >= len(bx):
            break
        trade_day = bx.iloc[next_trade_pos]['datetime']
    
        # 如果北向资金净流入，低于布林线，做卖出操作 operation -1代表卖出
        if pd.isnull(bx.iloc[pos]['lower_interval'])==False and bx.iloc[pos]['lower_interval']> bx.iloc[pos]['total_net_in']:
            record=pd.DataFrame({'operation':-1},index=[trade_day])  
            count=count+1

        # 如果北向资金净流入，高于布林线，做买入操作 operation 1代表买入
        elif pd.isnull(bx.iloc[pos]['upper_interval'])==False and bx.iloc[pos]['upper_interval'] < bx.iloc[pos]['total_net_in']:
            count=count+1
            record=pd.DataFrame({'operation':1},index=[trade_day]) 
        # 其他情况，不做操作    
        else:
            record=pd.DataFrame({'operation':0},index=[trade_day]) 
            
        trade_records= pd.concat([trade_records, record])

    # 删除首行
    trade_records.drop(['datetime'],inplace=True)  
    
    # 初始时持币
    holdCash =True
    capital =  init_cap
    acc_vol_end =0
    acc_val_end =0
    total_val = capital
    sellCount =0
    buyCount = 0
    delaySell=False

    for index ,row in trade_records.iterrows():
        
        if  row['operation'] ==1: 
            delaySell=False

    
        # 持币且operation 为1时，买入
        if  row['operation'] ==1 and holdCash==True:
            
            #按开盘价买入，收盘价计算价格，每次交易90%本金,

            trade_val = capital*0.9  # 交易额
            trade_vol =  trade_val/benchmark.loc[index,'price_end']  # 交易量
            order_cost = trade_val * cost_ration # 交易手续费
            capital = capital-trade_val-order_cost  #剩余本金
            acc_vol_end = acc_vol_end +  trade_vol # 收盘账户持仓数量
            acc_val_end = acc_vol_end * benchmark.loc[index,'price_end']  # 收盘账户持仓金额
            total_val= acc_val_end + capital # 收盘总金额

            print( "buy, date:%s, trade_val：%.2f, acc_val_end:%.2f total_val:%.2f"%(index, trade_val,acc_val_end,total_val))

            record=pd.DataFrame({'operation':1,
                  'trade_val':trade_val, 
                  'trade_vol': trade_vol,
                  'acc_vol_end': acc_vol_end,
                  'acc_val_end': acc_val_end,
                  'total_value':total_val,
                  'capital':capital,
                  'order_cost':order_cost},
                 index=[index])   
            trade_records.update(record)
            holdCash =False
            buyCount=buyCount+1
            buyDay= index #纪录购买时
        elif  (delaySell or row['operation'] == -1) and holdCash== False:
            
            if  index-buyDay<  datetime.timedelta(days=trade_interval) :
                print('delaySell,buyDay:%s, currentDay:%s'%(buyDay,index))
                delaySell =True
                acc_val_end = acc_vol_end * benchmark.loc[index,'price_end']  # 按收盘计算
                total_val= acc_val_end + capital
                record=pd.DataFrame({'operation':0,
                     'trade_val':0, 
                     'trade_vol': 0,
                     'acc_vol_end': acc_vol_end ,
                     'acc_val_end': acc_val_end,
                     'total_value':total_val,
                     'capital':capital,
                     'order_cost':0},
                    index=[index])   
                trade_records.update(record)
                
            else : 
                #按收盘价卖出，收盘价计算金额，卖出则卖空
                trade_val = acc_vol_end *benchmark.loc[index,'price_end'] # 按开盘价卖出全部
                trade_vol = acc_val_end
                order_cost = trade_val * cost_ration
                capital =capital+trade_val-order_cost
                acc_vol_end =   acc_val_end- trade_vol
                acc_val_end = acc_vol_end * benchmark.loc[index,'price_end']  # 按收盘计算
                total_val= acc_val_end + capital
                record=pd.DataFrame({'operation':-1,
                     'trade_val':trade_val, 
                     'trade_vol': trade_vol,
                     'acc_vol_end': acc_vol_end,
                     'acc_val_end': acc_val_end,
                     'total_value':total_val,
                     'capital':capital,
                     'order_cost':order_cost},
                    index=[index])   
                print( "sell, date:%s, trade_val：%.2f, acc_val_end:%.2f total_val:%.2f"%(index, trade_val,acc_val_end,total_val))
                trade_records.update(record)
                holdCash= True
                sellCount=sellCount+1

        else :
            acc_val_end = acc_vol_end * benchmark.loc[index,'price_end']  # 按收盘计算
            total_val= acc_val_end + capital
            record=pd.DataFrame({'operation':0,
                 'trade_val':0, 
                 'trade_vol': 0,
                 'acc_vol_end': acc_vol_end ,
                 'acc_val_end': acc_val_end,
                 'total_value':total_val,
                 'capital':capital,
                 'order_cost':0},
                index=[index])   
            trade_records.update(record)
            #print("common_day, date:%s,acc_val_end:%.2f,total_val: %.2f "%(index,acc_val_end,total_val))     
    print("sellCount:%d,buy_count:%d "%(sellCount,buyCount))     
    return trade_records



# 获取北向数据
bx= prepareBeixiangData()
# 绘制布林线
#bx.plot(x='datetime',y=['total_net_in','lower_interval','upper_interval','mean'] )

# 获取上证指数为参考
bm= prepareShangZheng()
# 进行回测
records= backTrade(bx,bm,delay=0)

#开始比较时间
s_date = datetime.datetime.strptime('20161206', '%Y%m%d')
bm_selected =bm[bm['datetime']>s_date]
# 将参考指数归一化，初始为10000
norm_bm_price = bm_selected['price_end']/ bm_selected.iloc[0]['price_end']*10000
# 将操作纪录归一化，初始为10000
records_select = records[records.index>s_date]
norm_records_total_val = records_select['total_value']/ records_select.iloc[0]['total_value']*10000

norm_bm_price.plot.line(subplots= True,x='datetime',y='price_end',style='b',label='benchMark')
norm_records_total_val.plot.line(subplots=True,style='r',label='beixiang')
plt.grid(True)
plt.show()



#records.plot.line(x='datetime',y=['total_value','capital'])

