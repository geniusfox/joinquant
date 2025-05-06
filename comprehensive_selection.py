#-*- coding: utf-8 -*-
from jqdata import *
import pandas as pd
import os
from datetime import datetime
import sqlite3


def init_database():
    """初始化数据库"""
    os.makedirs('daily_rs', exist_ok=True)
    db_path = 'daily_rs/stock_selection.db'
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 创建表（如果不存在）
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS comprehensive_selections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,           -- 股票代码
            name TEXT,                    -- 股票名称
            price REAL,                   -- 最新价
            change_percent REAL,          -- 涨跌幅
            turnover_rate REAL,           -- 换手率
            volume_ratio REAL,            -- 量比
            total_score REAL,             -- 总分
            selection_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- 选股时间
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS outperform_stocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,           -- 股票代码
            selection_time TIMESTAMP,     -- 选股时间
            lc REAL,                      -- LC值
            ld REAL,                      -- LD值
            lx REAL,                      -- LX值
            la REAL,                      -- LA值
            UNIQUE(code, selection_time)  -- 确保每个股票在同一天只有一条记录
        )
    ''')
    
    conn.commit()
    return conn

def save_low_and_high(conn, result_df, date=None):
    """
    保存低位和高位数据到数据库
    
    Parameters:
    -----------
    conn : sqlite3.Connection
        数据库连接对象
    result_df : DataFrame
        包含lc,ld,lx,la等数据的DataFrame
    date : str, optional
        指定的日期，格式为'%Y-%m-%d'。如果为None则使用当前日期
    """
    # 获取日期
    if date is None:
        date = datetime.now().strftime('%Y-%m-%d')
        
    # 删除同一天的数据
    cursor = conn.cursor()
    cursor.execute('''
        DELETE FROM outperform_stocks 
        WHERE DATE(selection_time) = ?
    ''', (date,))
    
    # 准备数据
    data = []
    for security in result_df.index:
        row = result_df.loc[security]
        data.append((
            security,      # 股票代码
            date,         # 选股时间
            row['lc'],    # LC值
            row['ld'],    # LD值
            row['lx'],    # LX值
            row['la']     # LA值
        ))
    
    # 插入数据
    cursor.executemany('''
        INSERT INTO outperform_stocks
        (code, selection_time, lc, ld, lx, la)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', data)
    
    conn.commit()



def save_to_database(conn, result_df, date=None):
    """
    保存结果到数据库
    
    Parameters:
    -----------
    conn : sqlite3.Connection
        数据库连接对象
    result_df : DataFrame
        选股结果数据
    date : str, optional
        指定的日期，格式为'%Y-%m-%d'。如果为None则使用当前日期
    """
    # 获取日期
    if date is None:
        date = datetime.now().strftime('%Y-%m-%d')
    
    # 删除同一天的数据
    cursor = conn.cursor()
    cursor.execute('''
        DELETE FROM comprehensive_selections 
        WHERE DATE(selection_time) = ?
    ''', (date,))
    
    # 准备数据
    data = []
    for _, row in result_df.iterrows():
        data.append((
            row['code'],          # 股票代码
            row.get('name', None),  # 股票名称
            row['close'],         # 最新价
            row['increase'],      # 涨跌幅
            row['turnover'],      # 换手率
            row['volume_ratio'],  # 量比
            row.get('total_score', 0),  # 总分
            date                  # 选股时间
        ))
    
    # 插入数据
    cursor.executemany('''
        INSERT INTO comprehensive_selections 
        (code, name, price, change_percent, turnover_rate, volume_ratio, total_score, selection_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', data)
    
    conn.commit()


def run_stock_selection(specified_date):
    
    # 获取所有股票的基本信息
    all_stocks = get_all_securities(types=['stock']).index.tolist()

    # 获取交易日所有股票的行情数据
    df = get_price(all_stocks, end_date=specified_date, count=1, frequency='1d', 
                   fields=['close', 'open', 'high', 'low', 'volume', 'money'], panel=False)
    df = df.reset_index()

    # 计算涨幅
    pre_close_df = get_price(all_stocks, end_date=specified_date, count=2, frequency='1d', 
                             fields=['close'], panel=False)
    pre_close_df = pre_close_df.reset_index()
    pre_close = pre_close_df.groupby('code')['close'].nth(0)
    df['increase'] = (df['close'] - df['code'].map(pre_close)) / df['code'].map(pre_close) * 100

    # 计算量比
    prev_volume = get_price(all_stocks, end_date=specified_date, count=2, frequency='1d', 
                            fields=['volume'], panel=False)
    prev_volume = prev_volume.reset_index().groupby('code')['volume'].nth(0)
    df['volume_ratio'] = df['volume'] / df['code'].map(prev_volume)

    # 获取换手率
    turnover_df = get_fundamentals(query(valuation.code, valuation.turnover_ratio).filter(valuation.code.in_(all_stocks)), 
                                   date=specified_date)
    turnover_dict = dict(zip(turnover_df['code'], turnover_df['turnover_ratio']))
    df['turnover'] = df['code'].map(turnover_dict)

    # 获取流通市值
    cap_df = get_fundamentals(query(valuation.code, valuation.circulating_market_cap).filter(valuation.code.in_(all_stocks)), 
                              date=specified_date)
    cap_dict = dict(zip(cap_df['code'], cap_df['circulating_market_cap']))
    df['circulating_market_cap'] = df['code'].map(cap_dict)

    print(f"\n开始筛选，初始股票池数量: {len(df)} 只")

    # 第一步：涨幅筛选
    filtered_df1 = df[(df['increase'] >= 3) & (df['increase'] <= 5)]
    print(f"1. 涨幅在 3-5% 之间的股票数量: {len(filtered_df1)} 只")

    # 第二步：量比筛选
    filtered_df2 = filtered_df1[filtered_df1['volume_ratio'] >= 1]
    print(f"2. 量比大于等于1的股票数量: {len(filtered_df2)} 只")

    # 第三步：换手率筛选
    filtered_df3 = filtered_df2[(filtered_df2['turnover'] >= 5) & (filtered_df2['turnover'] <= 10)]
    print(f"3. 换手率在 5-10% 之间的股票数量: {len(filtered_df3)} 只")

    # 第四步：流通市值筛选
    filtered_df4 = filtered_df3[(filtered_df3['circulating_market_cap'] >= 50) & 
                               (filtered_df3['circulating_market_cap'] <= 200)]
    print(f"4. 流通市值在 50-200亿 之间的股票数量: {len(filtered_df4)} 只")

    # 第五步：成交量筛选
    valid_stocks = []
    for stock in filtered_df4['code'].tolist():
        # 获取前5日成交量数据（保持原日期逻辑）
        volumes_data = get_price(
            stock, 
            end_date=specified_date, 
            count=5, 
            frequency='1d', 
            fields=['volume'], 
            panel=False
        )
    
        # 数据完整性检查
        if volumes_data is None or len(volumes_data) != 5:
            continue
    
        # 处理数据顺序（确保旧→新）
        dates = volumes_data.index.tolist()
        volumes = volumes_data['volume'].tolist()
        if dates[0] > dates[-1]:  # 日期降序时反转
            volumes = volumes[::-1]
        
        # 规则1：计算5日移动平均线判断整体趋势
        sma5 = sum(volumes) / 5  # 简单移动平均
        trend_up = volumes[-1] > sma5  # 最新成交量高于均线
        
        # 规则2：检查连续两天增长后第三天跌幅>5%的情况
        has_bad_pattern = False
        for i in range(len(volumes)-2):
            # 连续两天增长
            cond1 = (volumes[i+1] > volumes[i]) 
            cond2 = (volumes[i+2] > volumes[i+1])
            # 第三天跌幅超过5%
            cond3 = (volumes[i+2] < 0.95 * volumes[i+1])
            if cond1 and cond2 and cond3:
                has_bad_pattern = True
                break
    
        # 综合判断
        if trend_up and not has_bad_pattern:
            valid_stocks.append(stock)
    # for stock in filtered_df4['code'].tolist():
    #     volumes = get_price(stock, end_date=prev_trading_date, count=5, frequency='1d', 
    #                        fields=['volume'], panel=False)['volume']
    #     if all(volumes[i + 1] > volumes[i] for i in range(len(volumes) - 1)) == True:
    #         valid_stocks.append(stock)

    filtered_df5 = filtered_df4[filtered_df4['code'].isin(valid_stocks)]
    print(f"5. 符合趋势条件且无异常波动的股票剩余: {len(filtered_df5)} 只")

    # 第六步：均线及K线形态筛选
    valid_stocks = []
    for stock in filtered_df5['code'].tolist():
        hist_data = get_price(stock, end_date=specified_date, count=60, frequency='1d', 
                             fields=['close'], panel=False)
        ma5 = hist_data['close'][-5:].mean()
        ma10 = hist_data['close'][-10:].mean()
        ma20 = hist_data['close'][-20:].mean()
        ma60 = hist_data['close'][-60:].mean()
        #print(stock, ma5,ma10,ma20,ma60)
        if ma5 > ma10 > ma20 > ma60:
            last_close = hist_data['close'][-1]
            if last_close > ma5 and last_close > ma10 and last_close > ma20 and last_close > ma60:
                valid_stocks.append(stock)

    filtered_df6 = filtered_df5[filtered_df5['code'].isin(valid_stocks)]
    print(f"6. 均线多头向上发散的股票数量: {len(filtered_df6)} 只")

    # 第七步：分时图及热点题材筛选
    benchmark_data = get_price('000300.XSHG', end_date=specified_date, count=1, frequency='1d', 
                              fields=['close'], panel=False)
    benchmark_pre_close = get_price('000300.XSHG', end_date=specified_date, count=2, frequency='1d', 
                                   fields=['close'], panel=False)['close'][0]
    benchmark_increase = (benchmark_data['close'][-1] - benchmark_pre_close) / benchmark_pre_close * 100

    valid_stocks = []
    for stock in filtered_df6['code'].tolist():
        if filtered_df6[filtered_df6['code'] == stock]['increase'].values[0] > benchmark_increase:
            valid_stocks.append(stock)

    filtered_df7 = filtered_df6[filtered_df6['code'].isin(valid_stocks)]
    print(f"7. 跑赢大盘的股票数量: {len(filtered_df7)} 只")
    print("\n筛选完成！")

    # 保存结果到数据库
    os.makedirs('daily_rs', exist_ok=True)
    conn = init_database()
    save_to_database(conn, filtered_df7, specified_date)
    conn.close()
    print('结果已保存到数据库')
    
    final_stock_codes = filtered_df7['code'].tolist()
    return final_stock_codes

def get_recent_selections(days=7):
    """
    获取最近N天的选股结果
    
    Parameters:
    -----------
    days : int, default 7
        要查询的天数
        
    Returns:
    --------
    DataFrame
        包含日期和选股数量的DataFrame
    """
    conn = sqlite3.connect('daily_rs/stock_selection.db')
    
    query = '''
    SELECT 
        DATE(selection_time) as date,
        COUNT(DISTINCT code) as stock_count
    FROM comprehensive_selections 
    WHERE DATE(selection_time) >= DATE('now', ?)
    GROUP BY DATE(selection_time)
    ORDER BY date DESC
    '''
    
    date_condition = f'-{days} days'
    df = pd.read_sql_query(query, conn, params=(date_condition,))
    conn.close()
    
    # 重命名列
    df.columns = ['日期', '股票数量']
    
    return df

def get_stocks_by_date(date):
    """
    获取指定日期的所有选中股票代码
    
    Parameters:
    -----------
    date : str
        指定的日期，格式为'%Y-%m-%d'
        
    Returns:
    --------
    list
        选中的股票代码列表
    """
    conn = sqlite3.connect('daily_rs/stock_selection.db')
    
    query = '''
    SELECT code
    FROM comprehensive_selections 
    WHERE selection_time = ?
    ORDER BY total_score DESC
    '''
    
    df = pd.read_sql_query(query, conn, params=(date,))
    conn.close()
    
    return df['code'].tolist()

if __name__ == '__main__':
    # 测试运行
    result = run_stock_selection()
    print("\n最终筛选的股票代码：")
    print(result)
    
    # 查询最近7天的选股结果
    recent_results = get_recent_selections(days=7)
    print("\n最近7天的选股结果：")
    print(f"共有 {len(recent_results)} 只股票被选中")
    print("\n按出现天数排序的结果：")
    print(recent_results.sort_values('出现天数', ascending=False).head(10))
