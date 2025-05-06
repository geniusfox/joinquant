#-*- coding: utf-8 -*-
from jqdata import *
from enum import Enum

import pandas as pd

# 前一个开盘日期，用以提取历史数据
def get_previous_trade_day(trade_day):
    trade_days = get_trade_days(end_date=trade_day,count=2)
    return(trade_days[0])


# 根据历史数据获取对应的最低价格区间&最高价格区间
def get_low_and_high(end_date, security_list):
    cols = ['p_close','open','close','high','low','low_after_high', 'high_after_low',
            'hc','hd','hx','ha','lc','ld','lx','la',
            'no']
    rs = pd.DataFrame({}, columns = cols, index = security_list,dtype=float)
    rs = rs.fillna(0)
    rs.index.name = 'security'
    drop_list = []
    for security in security_list:
        line = rs.loc[security]
        
        #获取到截止日期T-N交易日的数据
        df = get_price(security, count=2, end_date= end_date, 
                   frequency='daily', fields=['open','close','high','low']) 

        #最后一个交易日K线中的最高价后的对低价，最低价后的最高价
        td = get_price(security, count = 240 ,end_date=str("%s 16:00:00" % end_date),
                   frequency='minute',fields=['high','low'])
        #实际分钟K线的最高&最低价格
        high = max(td.high)
        low = min(td.low)

        #最高&最低的分钟位置
        #如果有多个最高价或最低价，则选择最后一个
        high_line = (td[td.high == high]).tail(1)
        low_line = (td[td.low == low]).tail(1)
        
        if np.isnan(high) or np.isnan(low):
            drop_list.append(security)
            continue
        
        high_after_low = max(td.loc[low_line.index[0]:, 'high'])
        low_after_high = min(td.loc[high_line.index[0]:,'low'])
        
        c = df.tail(1) #当日数据
        p = df.head(1) #前一个交易日数据
        
        line.p_close = p.close[0] 
        line.open, line.close, line.high, line.low = c.open, c.close, high, low
        line.low_after_high = low_after_high
        line.high_after_low = high_after_low

        hc = (c.high[0]/p.close[0]) * c.close[0]
        hd = (c.high[0]/c.open[0]) * c.close[0]
        # hx =  (最低价后高点/最低价) * 当日收盘
        hx = (high_after_low/c.low[0]) * c.close[0]
        ha = (hc+hd+hx)/3
        

        lc = (c.low[0]/ c.open[0]) * c.close[0]
        ld = (c.low[0]/ p.close[0]) * c.close[0]
        #lx =  (最高价后低点/当日最高) * 当日收盘
        lx = (low_after_high/c.high[0] ) * c.close[0] 
        la = (lc+ld+lx)/3
        #print('lc:¥ %.2f, ld: ¥%.2f, lx:¥%.2f, la:¥ %.2f' % (lc, ld, lx, la))
        line.hc, line.hd, line.hx, line.ha = round(hc,2), round(hd,2), round(hx,2), round(ha,2)
        line.lc, line.ld, line.lx, line.la = round(lc,2), round(ld,2), round(lx,2), round(la,2)     
        line.no = round((line.open/line.p_close)* line.close,2)
    rs = rs.drop(index=drop_list)
    return(rs)