# 导入函数库
from jqdata import *
#from jqdatasdk import *
from six import BytesIO
import os
import re
from collections import defaultdict
import math

# 初始化函数，设定基准等等
def initialize(context):
    # 开发环境
    g.evn = 'development'
    # 设定沪深300作为基准
    set_benchmark('000300.XSHG')
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
    # 输出内容到日志 log.info()
    log.info('初始函数开始运行且全局只运行一次')
    # 过滤掉order系列API产生的比error级别低的log
    # log.set_level('order', 'error')
    ### 股票相关设定 ###
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')

    #读取资源文件dragon_equity_list.txt
    data=pd.read_csv(BytesIO(read_file('dragon_equity_list.txt')), index_col=[0])
    #遍历data，将data的每一行转换为字典，并添加到g.dragon_equity_map中
    g.dragon_equity_map = defaultdict(list)
    # g.concept_equity_map = {}
    for index, row in data.iterrows():
        g.dragon_equity_map[index].append(row[0])
    #log.debug("股票概念映射："+str(g.dragon_equity_map))

    df = get_concepts()
    g.concepts= set()
    for code, row in df.iterrows():
      if re.match(r'^(2024|2025)', str(row['start_date'])):
          continue
        # 过滤 name 包含"预增"的
      if re.search(r'预增', str(row['name'])):
            continue
      g.concepts.add(code)
    #log.debug("目标板块列表："+str(g.concepts))

    # 最大持仓数量
    g.max_equity_num = 8
    # 最大下单金额
    g.max_order_amount = int(context.portfolio.starting_cash * 0.15 // 100) * 100
    #账号暂停天数，如果是0则可以交易
    g.freezed_days = 0
    #最大回撤金额
    g.max_loss_amount = int(context.portfolio.starting_cash * 0.1 // 100) * 100 * -1
    # 记录建仓股票的可被交易次数
    g.trade_times = {}

    ## 运行函数（reference_security为运行时间的参考标的；传入的标的只做种类区分，因此传入'000300.XSHG'或'510300.XSHG'是一样的）
    run_daily(frash_freezed_days, time = 'before_open')
    run_daily(sell_loss, time='14:50:00')
    run_daily(sell_profit, time='14:50:00')
    run_daily(open_position, time='14:50:00')
      # 收盘后运行
    #run_daily(after_market_close, time='after_close', reference_security='000300.XSHG')

#刷新冻结天数，每次开盘前运行。没调用一次，冻结天数-1.最小值为0
def frash_freezed_days(context):
    g.freezed_days = max(0, g.freezed_days - 1) 

#开仓买入
def open_position(context):
    #获得当前日期，如果不是周1,3,5，则不买入
    if context.current_dt.weekday() not in [0,2,4]:
        return
    if g.freezed_days > 0:
        return
    #获得当前持仓的股票
    current_positions = set(context.portfolio.positions.keys())
    #如果超过最大持仓数量，则不买入
    if len(current_positions) >= g.max_equity_num:
        return
    
    # g.dragon_equity_map: {股票代码: 概念编码}
    not_in_position = {code: concept for code, concept in g.dragon_equity_map.items() if code not in current_positions}
    
    concept_equity_map = defaultdict(list)
    for code, concepts in not_in_position.items():
        #遍历concepts，将concepts的每个元素添加到concept_equity_map中
        for concept in concepts:
            concept_equity_map[concept].append(code)  
    #log.debug("概念股票列表："+str(concept_equity_map))
    #方法计时,输出用取整毫秒做单位
    start_time = time.time()
    #调用get_top3_concepts_increase函数，获取前3的概念
    top3_concepts = get_top3_concepts_increase(g.concepts, context.current_dt)
    start_time = time.time()
    log.debug("前3的概念："+str(top3_concepts) + "，耗时："+str(int((time.time() - start_time) * 1000)) + "毫秒")
    #筛选not_in_position的股票对应的concept在top3_concepts中的股票，加入到代买入buy_list
    buy_list = []
    for concept in top3_concepts:
        equity_list = concept_equity_map.get(concept, [])
        # 后续处理
        buy_list.extend(equity_list)
    # 去重
    buy_list = list(set(buy_list))
    log.debug("在板块涨幅top3，待买入的股票："+str(buy_list)+"，耗时："+str(int((time.time() - start_time) * 1000)) + "毫秒")

    trade_dt = context.current_dt.strftime('%Y-%m-%d')
    #过滤掉股价没有站上20天线的股票
    start_time = time.time()
    buy_list = [code for code in buy_list if is_above_20_day_line(code, trade_dt)]
    log.debug("20天线的股票，待买入的股票："+str(buy_list)+"，耗时："+str(int((time.time() - start_time) * 1000)) + "毫秒")
    #过滤掉最近3天成交量没有放大至1.5倍以上的股票
    start_time = time.time()
    buy_list = [code for code in buy_list if is_volume_increased_150(code, trade_dt)]
    log.debug("1.5倍交易量，待买入的股票："+str(buy_list)+"，耗时："+str(int((time.time() - start_time) * 1000)) + "毫秒")
    #过滤掉MACD没有出现金叉的股票
    start_time = time.time()
    buy_list = [code for code in buy_list if is_macd_gold_cross(code, trade_dt)]
    log.debug("MACD金叉，待买入的股票："+str(buy_list)+"，耗时："+str(int((time.time() - start_time) * 1000)) + "毫秒")
    #过滤掉开盘后30分钟~1小时内，成交量没有达到昨日全天成交量的50%甚至更高的股票
    start_time = time.time()
    buy_list = [code for code in buy_list if is_volume_increased_50(code, trade_dt)]
    log.debug("开盘30分钟交易量："+str(buy_list)+"，耗时："+str(int((time.time() - start_time) * 1000)) + "毫秒")
    #过滤掉分时图中的黄色均价线（即当日成交均线）没有呈现温和向上的趋势的股票
    start_time = time.time()
    buy_list = [code for code in buy_list if is_yellow_average_line_upward(code, trade_dt)]
    log.debug("温和向上："+str(buy_list)+"，耗时："+str(int((time.time() - start_time) * 1000)) + "毫秒")
    
    #循环buy_list，买入，如果持仓大于g.max_equity_num，则不买入
    for code in buy_list:
        if len(context.portfolio.positions) >= g.max_equity_num:
            continue
        #下单金额以当前可用现金和最大订单交易额的较小值为准
        order_amount = min(context.portfolio.available_cash, g.max_order_amount)
        o = order_value(code, order_amount)
        if o is not None:
            g.trade_times[code] = 2
        log.info("买入："+code+"，金额："+str(order_amount))


#获利卖出    
def sell_profit(context):
    #获取当前时间，如果不是周1,3,5，则不卖出
    if context.current_dt.weekday() not in [0,2,4]:
        return
    if g.freezed_days > 0:
        return
    trade_dt = context.current_dt.strftime('%Y-%m-%d')
    # 获取前5的概念
    top5_concepts = top5_concept_monitor(g.concepts, trade_dt)
    # 判断持仓股票对应的板块是否在top5_concepts中，如果不在则全部卖出
    for position in context.portfolio.positions:
        #dragon_equity_map 获取股票对应的板块
        concept = g.dragon_equity_map.get(position.security)
        if concept not in top5_concepts:
            order_target(position.security, 0)
            g.trade_times[position.security] = 0
            log.info("持仓股票对应的板块不在top5_concepts中，卖出")
    
    #循环持仓列表，判断跌幅是否超过5%。如果满足条件则卖出50%
    for position in context.portfolio.positions:
        amount = position.total_amount
        #涨幅超过20%的目标迈卖出价格
        sell_profit_price = position.avg_cost * 1.25
        if g.trade_times[position.security] == 2:
          #计算交易手数，amount/100 * 50% 
          amount = math.ceil(position.total_amount /100 * 0.5) * 100
          sell_profit_price = position.avg_cost * 1.20
        if is_stock_down_5(position.security, trade_dt):
            #如果trade_times ==2 则卖出50%，如果==1 则卖出份额
            order(position.security, amount)
            g.trade_times[position.security]  = g.trade_times[position.security] - 1
            log.info("单只股票跌幅超过5%，卖出")
        elif is_above_target_price(position.security, trade_dt, sell_profit_price):
            order(position.security, amount)
            g.trade_times[position.security]  = g.trade_times[position.security] - 1
            log.info("单只股票涨幅超过20%，卖出")
        

#止损卖出
def sell_loss(context):
    #获取当前的所有持仓
    current_positions = context.portfolio.positions
    #计算所有的持仓收益
    total_profit = sum(position.value - position.hold_cost for position in current_positions.values())
    #如果总收益小于最大回撤金额，则卖出所有持仓
    if total_profit < g.max_loss_amount:
        for position in current_positions:
            order_target(position.security, 0)
        log.warning("总账户回撤超过10%暂停交易一周")
        g.freezed_days = 7
        # 清空交易次数
        g.trade_times = {}
    else:
        #遍历持仓，如果单只股票亏损超过8%，则清仓卖出该只股票
        for position in current_positions:
            if position.value < position.hold_cost * 0.92:
                order_target(position.security, 0)
                g.trade_times[position.security] = 0 
                log.info("单只股票亏损超过8%，清仓卖出")

def get_top3_concepts_increase(concepts, date):
    concept_avg_increase = {}
    for concept in concepts:
        stock_list = get_concept_stocks(concept_code=concept, date=date)
        if not stock_list:
            continue
        # 获取开盘价和最高价
        price_df = get_price(stock_list, end_date=date, count=1, frequency='1d', fields=['open', 'high'], panel=False)
        if price_df is None or price_df.empty:
            continue
        # 计算涨幅 (最高-开盘)/开盘
        price_df = price_df.reset_index()
        price_df['increase'] = (price_df['high'] - price_df['open']) / price_df['open'] * 100
        avg_increase = price_df['increase'].mean()
        concept_avg_increase[concept] = avg_increase
    # 按平均涨幅排序，取前3
    top3 = sorted(concept_avg_increase.items(), key=lambda x: x[1], reverse=True)[:3]
    return [c[0] for c in top3]

#股价站上20天线
def is_above_20_day_line(code, date):
  #获取20天线
  price_df = get_price(code, end_date=date, count=20, frequency='1d', fields=['close'], panel=False)
  if price_df is None or price_df.empty:
    return False
  # 计算20天线
  price_df['20_day_line'] = price_df['close'].rolling(window=20).mean()
  return price_df['close'].iloc[-1] > price_df['20_day_line'].iloc[-1]

#最近3天成交量放大至1.5倍以上
def is_volume_increased_150(code, date):
  #获取最近3天成交量
  price_df = get_price(code, end_date=date, count=3, frequency='1d', fields=['volume'], panel=False)
  if price_df is None or price_df.empty:
    return False
  # 计算3天成交量
  price_df['volume_increased'] = price_df['volume'].rolling(window=3).mean()
  return price_df['volume'].iloc[-1] > price_df['volume_increased'].iloc[-1] * 1.5  

#MACD出现金叉（白线上穿黄线）
def is_macd_gold_cross(code, date):
    # 获取最近35天的收盘价
    price_df = get_price(code, end_date=date, count=35, frequency='1d', fields=['close'], panel=False)
    if price_df is None or price_df.empty or len(price_df) < 35:
        return False
    close = price_df['close']
    # 计算EMA12和EMA26
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    # 计算DIFF
    diff = ema12 - ema26
    # 计算DEA
    dea = diff.ewm(span=9, adjust=False).mean()
    # 判断金叉：DIFF从下向上穿越DEA
    if diff.iloc[-2] < dea.iloc[-2] and diff.iloc[-1] > dea.iloc[-1]:
        return True
    return False  

#开盘后30分钟~1小时内，成交量已达到昨日全天成交量的50%甚至更高
def is_volume_increased_50(code, date):
  #获取昨日成交量
  yesterday_volume = get_price(code, end_date=date, count=1, frequency='1d', fields=['volume'], panel=False)
  min_time = date + ' 10:00:00'
  today_volume = get_price(code, end_date=min_time, count=30, frequency='1m', fields=['volume'], panel=False)
  today_volume = today_volume['volume'].sum()
  #log.debug(code+ ":今日成交量："+str(today_volume)+ "昨日成交量："+str(yesterday_volume['volume'].iloc[-1]))
  if yesterday_volume is None or yesterday_volume.empty:
    return False
  return today_volume >= yesterday_volume['volume'].iloc[-1] * 0.5  
  #return True


#分时图中的黄色均价线（即当日成交均线）呈现温和向上的趋势
def is_yellow_average_line_upward(code, date):
  return True


# 判断当日股票跌幅是否超过5%
def is_stock_down_5(code, date):
  start_time = date + ' 09:31:00'
  end_time = date + ' 14:50:00'
  price_start = get_price(code, end_date=start_time, count=1, frequency='1m', fields=['open'], panel=False)
  price_end = get_price(code, end_date=end_time, count=1, frequency='1m', fields=['close'], panel=False)
  #判断price_end/price_start是否小于0.95
  return price_end['close'].iloc[-1] / price_start['open'].iloc[-1] <= 0.95

#top5概念板块,以平均涨跌幅计算
def top5_concept_monitor(concepts, date):
  concept_avg_change_pct = {}
  for concept in concepts:
        stock_list = get_concept_stocks(concept_code=concept, date=date)
        #调用get_money_flow得到上一个交易日的涨跌幅 change_pct字段。求平均值为概念板块的涨跌幅 
        money_flow = get_money_flow(stock_list, end_date=date, count=1, fields=['change_pct'])
        if money_flow is None or money_flow.empty:
            continue
        avg_change_pct = money_flow['change_pct'].mean()
        concept_avg_change_pct[concept] = avg_change_pct
  # 按平均涨幅排序，取前5 
  top5 = sorted(concept_avg_change_pct.items(), key=lambda x: x[1], reverse=True)[:5]
  # 返回对应的板块代码
  return [c[0] for c in top5]

# 判断股票当前价格是否超过目标价格
def is_above_target_price(code, date, target_price):
  curt_time = date + ' 14:50:00'
  price_df = get_price(code, end_date=curt_time, count=1, frequency='1m', fields=['close'], panel=False)
  if price_df is None or price_df.empty:
    return False
  return price_df['close'].iloc[-1] >= target_price
