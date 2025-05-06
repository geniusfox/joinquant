from jqdata import *
from six import BytesIO
from daily_bottom_finder import *
import os

# 初始化函数，设定基准等等
def initialize(context):
    # 设定沪深300作为基准
    set_benchmark('000300.XSHG')
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
    
    # 过滤掉order系列API产生的比error级别低的log
    # log.set_level('order', 'error')
    
    # 加载选中的股票列表，每行是一天的数据。csv格式用,分割，以第一列日期为key，2～n列是股票代码
    # 读取daily_rs/selected_list.txt文件
    body = read_file("daily_rs/selected_list.txt")
    # 创建BytesIO对象
    bio = BytesIO(body)
    # 读取所有行
    lines = bio.read().decode('utf-8').split('\n')
    # 将每行内容按,分割，以第一列日期为key，2～n列是股票代码
    # 将解析结果存储到context.selected_stocks字典中
    #context.selected_stocks = {}
    g.selected_stocks = {}
    for line in lines:
        if line:
            # 需要过滤只有日期，没有后继数据的行
            ts = line.split(',')
            if len(ts) >1 and len(ts[1]) >0:
                date, stocks = ts[0], ts[1:]
                g.selected_stocks[date] = stocks
    #设置最大股票只数
    g.max_stock_num = 10
    #根据context.portfolio.starting_cash和最大股票只数计算每次买入股票的金额
    g.max_single_position = context.portfolio.starting_cash / g.max_stock_num
    g.today_sell_orders = []

    ### 股票相关设定 ###
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')
    
    ## 运行函数（reference_security为运行时间的参考标的；传入的标的只做种类区分，因此传入'000300.XSHG'或'510300.XSHG'是一样的）
      # 开盘前运行
    run_daily(before_market_open, time='9:30:01', reference_security='000300.XSHG') 
      # 开盘后运行
    run_daily(before_market_close, time='14:55', reference_security='000300.XSHG')
      # 收盘后运行
    run_daily(after_market_close, time='after_close', reference_security='000300.XSHG')
    
## 开盘前运行函数     
def before_market_open(context):
    # 获取前一个交易日
    yesterday = context.previous_date.strftime('%Y-%m-%d')
    # 检查g.selected_stocks字典中是否存在昨天日期
    # sell_stocks = {} 
    #用get_trade_days(start_date=None, end_date=None, count=None)获取context.current_dt前3个交易日
    three_days_ago = get_trade_days(end_date=context.current_dt, count=3)[0]
    three_days_ago = three_days_ago.strftime('%Y-%m-%d')    
    #向前计算第5个交易日
    five_days_ago = get_trade_days(end_date=context.current_dt, count=5)[0]
    five_days_ago = five_days_ago.strftime('%Y-%m-%d')
    #遍历context.portfolio.positions，
    for security, postion in context.portfolio.positions.items():
        #以init_time为基准和three_days_ago的日期是否相等，按天比较 
        if postion.init_time.strftime('%Y-%m-%d') == three_days_ago:
            #获取三个交易日的收盘价，用attribute_history(security, count, unit='1d',fields=['close'],skip_paused=True, df=True, fq='pre')
            close_prices = attribute_history(security , 3, unit='1d', fields=['close'], skip_paused=True, df=True, fq='pre')
            #判断三个交易日收盘价是否逐步走跌，且都低于买入价postion.acc_avg_cost，如果满足条件，则卖出
            if close_prices.iloc[0]['close'] > close_prices.iloc[1]['close'] and close_prices.iloc[1]['close'] > close_prices.iloc[2]['close'] and close_prices.iloc[0]['close'] < postion.acc_avg_cost:
                #卖出股票
                order_target(security, 0)
                # sell_stocks[postion.security] = postion.acc_avg_cost
                log.info(f"持有{security}超过3个交易日，判断三个交易日收盘价是否逐步走跌，且都低于买入价，如果满足条件，则卖出")
            else:
                # log.debug(f"持有{security} 不满足连续三天下跌，继续持有！")
                #按照买入价*1.05目标卖出股票
                o = order_target(security,0, LimitOrderStyle(postion.acc_avg_cost*1.05))
                log.info(f"持有{security} 不满足连续三天下跌，继续持有！，按照买入价*1.05目标卖出股票")
        elif postion.init_time.strftime('%Y-%m-%d') == five_days_ago:
            #如果持有时间超过5个交易日，则按照买入价平仓
            order_target(security, 0, LimitOrderStyle(postion.acc_avg_cost))
            log.info(f"持有{security}超过5个交易日，则按照买入价平仓")
        else:
            #按照买入价*1.05目标卖出股票
            o = order_target(security,0, LimitOrderStyle(postion.acc_avg_cost*1.05))
            #log.info(f"持有{postion.security}不足3个交易日，不进行卖出操作")
    
    if yesterday in g.selected_stocks:
        # 获取昨天选股结果
        stocks = g.selected_stocks[yesterday]
        #遍历portfolio.positions,讲postion.security存储到set对象
        postion_set = context.portfolio.positions.keys()
        #检查stocks中是否有代码已经在 postion_set
        for stock in stocks:
            if stock in postion_set:
                stocks.remove(stock)
        #调用get_low_and_high函数，获取stocks中每个股票的最低价格和最高价格
        data = get_low_and_high(yesterday, stocks)
        #遍历data，计算每个股票的买入价格
        for stock in stocks:
            #计算买入价格
            #判断持仓股票只数是否超过限制
            if len(context.portfolio.positions) >= g.max_stock_num:
                log.info(f"持仓股票只数超过限制，不买入{stock}")
                continue
            #判断data中是否有对应的stock数据
            if stock in data.index:
                buy_price = data.loc[stock]['la']
                #通过order_value买入股票
                order = order_value(stock, g.max_single_position, LimitOrderStyle(buy_price))
                log.info(f"下入限价单:{stock}，价格为{buy_price}，数量为{g.max_single_position}")
    else:
        log.debug(f"{yesterday}没有选中的股票")
    
            
    
## 收盘前运行函数
def before_market_close(context):
    #向前计算第5个交易日
    five_days_ago = get_trade_days(end_date=context.current_dt, count=5)[0]
    five_days_ago = five_days_ago.strftime('%Y-%m-%d')
    #遍历context.portfolio.positions，如果持有时间超过5个交易日，按照市价平仓（模拟Close价格）
    for security, postion in context.portfolio.positions.items():
        if postion.init_time.strftime('%Y-%m-%d') <= five_days_ago:
            #按照市价平仓（模拟Close价格）
            order_target(security, 0, MarketOrderStyle())
            log.info(f"持有{postion.security}超过5个交易日，按照市价平仓（模拟Close价格）")
 
## 收盘后运行函数  
def after_market_close(context):
    #输出持仓
    log.info(f"持仓: {context.portfolio.positions}")
    for id, order in get_orders().items():
        if order.is_buy == False:
            log.debug(f"订单: {order}")
    pass
