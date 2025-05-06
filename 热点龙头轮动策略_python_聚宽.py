# -*- coding: utf-8 -*-
from jqdata import *
import pandas as pd
import numpy as np

def initialize(context):
    # 策略参数设置
    g.trade_days = ['Monday', 'Wednesday', 'Friday']  # 每周一三五调仓
    g.max_positions = 8         # 最大持仓数量
    g.single_position = 0.15    # 单票最大仓位
    g.stop_loss = -0.08         # 个股止损线
    g.take_profit = 0.20        # 个股止盈线
    
    # 设置基准和交易成本
    set_benchmark('000300.XSHG')
    set_option('use_real_price', True)
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, 
                           open_commission=0.0003, close_commission=0.0003,
                           min_commission=5), type='stock')
    
    # 定时任务
    run_daily(market_open, time='9:30')
    run_daily(check_sell_signals, time='14:50')

def market_open(context):
    """开盘时执行选股和建仓"""
    # 只在指定交易日操作
    if current_weekday(context) not in g.trade_days:
        return
    
    # 获取候选股票池
    candidates = get_candidates(context)
    
    # 执行调仓
    rebalance_portfolio(context, candidates)

def get_candidates(context):
    """生成候选股票列表"""
    # 获取当日涨幅前3的行业
    industries = get_industry_ranking(context, top_n=3)
    
    # 获取行业成分股
    all_stocks = []
    for industry in industries:
        stocks = get_industry_stocks(industry)
        all_stocks.extend(stocks)
    
    # 技术指标筛选
    candidates = []
    for stock in set(all_stocks):
        if is_valid_candidate(context, stock):
            candidates.append(stock)
    
    return candidates[:100]  # 取前100只防止超限

def is_valid_candidate(context, stock):
    """技术面筛选"""
    # 获取历史数据
    hist = get_price(stock, end_date=context.current_dt, 
                    count=60, fields=['close','volume'])
    
    # 计算技术指标
    ma20 = hist['close'].rolling(20).mean().iloc[-1]
    ma5_vol = hist['volume'].rolling(5).mean().iloc[-2]  # 前一日5日均量
    
    # 当日数据
    current_data = get_price(stock, end_date=context.current_dt, 
                            count=1, fields=['close','volume'])
    
    # 量价条件
    cond1 = current_data['close'].iloc[-1] > ma20        # 站上20日线
    cond2 = current_data['volume'].iloc[-1] > ma5_vol*1.5 # 量能1.5倍
    cond3 = check_macd_golden_cross(stock)               # MACD金叉
    
    return cond1 and cond2 and cond3

def check_macd_golden_cross(stock):
    """MACD金叉判断"""
    data = get_price(stock, end_date=context.current_dt, 
                    count=35, fields=['close'])
    closes = data['close'].values
    
    # 计算MACD
    ema12 = pd.Series(closes).ewm(span=12).mean().values
    ema26 = pd.Series(closes).ewm(span=26).mean().values
    dif = ema12 - ema26
    dea = pd.Series(dif).ewm(span=9).mean().values
    
    return dif[-1] > dea[-1] and dif[-2] <= dea[-2]  # 当日金叉

def rebalance_portfolio(context, candidates):
    """执行调仓"""
    # 计算可用资金
    cash = context.portfolio.available_cash
    max_buy_value = cash * g.single_position
    
    # 卖出处理
    for stock in context.portfolio.positions:
        if should_sell(context, stock):
            order_target_value(stock, 0)
    
    # 买入处理
    for stock in candidates:
        if len(context.portfolio.positions) >= g.max_positions:
            break
        if stock not in context.portfolio.positions:
            order_target_value(stock, max_buy_value)

def should_sell(context, stock):
    """卖出条件判断"""
    position = context.portfolio.positions[stock]
    current_data = get_price(stock, count=1, fields=['close','volume'])
    
    # 止盈止损条件
    profit = position.value / position.total_cost - 1
    if profit >= g.take_profit or profit <= g.stop_loss:
        return True
    
    # 技术面条件
    hist = get_price(stock, count=10, fields=['close','volume'])
    ma5 = hist['close'].rolling(5).mean().iloc[-1]
    ma10 = hist['close'].rolling(10).mean().iloc[-1]
    if ma5 < ma10 and current_data['volume'].iloc[-1] < hist['volume'].mean()*0.8:
        return True
    
    return False

def check_sell_signals(context):
    """收盘前最后检查"""
    for stock in context.portfolio.positions:
        if should_force_sell(context, stock):
            order_target_value(stock, 0)

def should_force_sell(context, stock):
    """强制卖出条件"""
    # 板块热度检查
    industry = get_stock_industry(stock)
    if not is_top_industry(industry, top_n=5):
        return True
    
    # 分时图检查
    if not check_minute_chart(stock):
        return True
    
    return False

# ---------- 工具函数 ----------
def current_weekday(context):
    """获取当前星期名称"""
    return context.current_dt.weekday()

def get_industry_ranking(context, top_n=3):
    """获取当日行业涨幅排名"""
    industries = get_industries(name='sw_l1')
    industry_returns = {}
    
    for code, name in industries.items():
        stocks = get_industry_stocks(code)
        if len(stocks) == 0:
            continue
        returns = get_price(stocks, count=1, fields='close').pct_change().mean()
        industry_returns[name] = returns
    
    return sorted(industry_returns, key=industry_returns.get, reverse=True)[:top_n]

def check_minute_chart(stock):
    """分时图检查"""
    # 获取当日分钟数据
    minutes = get_price(stock, frequency='minute', count=240, 
                       fields=['close','volume'])
    
    # 早盘成交量检查
    morning_vol = minutes[:120]['volume'].sum()
    yesterday_vol = get_price(stock, count=2, fields='volume')['volume'].iloc[0]
    if morning_vol < yesterday_vol * 0.5:
        return False
    
    # 价格稳定性检查
    price_series = minutes['close'].values
    if (np.max(price_series) / np.min(price_series) - 1) > 0.07:  # 波动超过7%
        return False
    
    return True