#-*- coding: utf-8 -*-
class StockBacktesting:
    def __init__(self, init_cash=1000000, max_stock_num=5):
        """
        初始化回测系统
        
        Parameters:
        -----------
        init_cash : float
            初始资金，默认1000000
        max_stock_num : int 
            最大持仓数量，默认5只
        """
        self.init_cash = init_cash
        self.max_stock_num = max_stock_num
        
        # 当前现金和持仓市值
        self.current_cash = init_cash
        self.current_market_value = 0
        
        # 持仓股票字典，key为股票代码
        self.holding_stocks = {}
        
        # 单次最大可用资金
        self.max_single_position = init_cash / max_stock_num
        
    def buy_stock(self, stock_code, price, clear_date, target_price):
        """
        买入股票
        
        Parameters:
        -----------
        stock_code : str
            股票代码
        price : float 
            买入价格
        clear_date : str
            清仓日期
        target_price : float
            目标价格
            
        Returns:
        --------
        bool
            买入是否成功
        """
        # 检查是否可以买入新的股票
        if len(self.holding_stocks) >= self.max_stock_num:
            print(f"已达到最大持仓数量 {self.max_stock_num}")
            return False
            
        # 计算本次可用资金
        available_cash = min(self.current_cash, self.max_single_position)
        if available_cash < price * 100:  # 至少要能买100股
            print(f"可用资金不足，当前可用: ¥{available_cash:.2f}")
            return False
        
        # 计算可买股数(向下取整到100的倍数)
        max_shares = int(available_cash / price)
        shares = (max_shares // 100) * 100
        
        if shares == 0:
            print("可买股数为0")
            return False
            
        # 计算实际花费
        cost = shares * price
        
        # 更新现金和市值
        self.current_cash -= cost
        self.current_market_value += cost
        
        # 添加到持仓字典
        self.holding_stocks[stock_code] = {
            'code': stock_code,
            'price': price,
            'shares': shares,
            'cost': cost,
            'clear_date': clear_date,
            'market_price': price,
            'target_price': target_price
        }
        print(f"买入 {stock_code}: {shares}股，价格 ¥{price:.2f}，总花费 ¥{cost:.2f}")
        return True

    def is_stock_in_holdings(self, stock_code):
        """
        检查股票是否已在持仓字典中
        
        Parameters:
        -----------
        stock_code : str
            要检查的股票代码
            
        Returns:
        --------
        bool
            True表示股票已在持仓中，False表示不在持仓中
        """
        return stock_code in self.holding_stocks

    def sell_stock(self, stock_code, price, sell_date):
        """
        按指定价格卖出股票
        
        Parameters:
        -----------
        stock_code : str
            要卖出的股票代码
        price : float
            卖出价格
        sell_date : str
            卖出日期
            
        Returns:
        --------
        float
            交易收益率。如果股票不在持仓中返回None
        """
        if stock_code in self.holding_stocks:
            stock = self.holding_stocks[stock_code]
            # 计算收益
            buy_cost = stock['price'] * stock['shares']
            sell_income = price * stock['shares']
            profit_rate = (sell_income - buy_cost) / buy_cost * 100
            
            # 更新现金和市值
            self.current_cash += sell_income
            self.current_market_value -= sell_income
            
            # 从持仓字典中移除
            del self.holding_stocks[stock_code]
            
            print(f"卖出 {stock_code}: {stock['shares']}股，价格 ¥{price:.2f}，"
                f"总收入 ¥{sell_income:.2f}，收益率 {profit_rate:.2f}%")
            
            return profit_rate
                
        print(f"未找到股票 {stock_code} 的持仓记录")
        return None
    
    def get_holdings(self):
        """
        获取当前所有持仓数据
        
        Returns:
        --------
        list
            持仓股票信息列表
        """
        return list(self.holding_stocks.values())

    def summary_status(self):
        """
        生成账户状态信息的字符串摘要，包括收益率和资金利用率
        
        Returns:
        --------
        str
            账户状态信息的字符串摘要
        """
        # 根据持仓字典计算持仓市值
        self.current_market_value = sum([stock['market_price'] * stock['shares'] 
                                       for stock in self.holding_stocks.values()])
        
        # 计算总资产(现金 + 持仓市值)
        total_assets = self.current_cash + self.current_market_value
        
        # 计算收益率
        return_rate = (total_assets / self.init_cash - 1) * 100
        
        # 计算资金利用率
        cash_utilization = (self.current_cash / total_assets) * 100
        
        status_summary = (
            "\n=== 账户状态 ===\n"
            f"总资产: ¥{total_assets:,.2f}\n"
            f"现金余额: ¥{self.current_cash:,.2f}\n"
            f"持仓市值: ¥{self.current_market_value:,.2f}\n"
            f"收益率: {return_rate:.2f}%\n"
            f"资金空置率: {cash_utilization:.2f}%"
        )
        
        return status_summary
