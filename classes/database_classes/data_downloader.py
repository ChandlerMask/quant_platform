'''
Description: 
Author: ymgu
Date: 2023-07-11 14:48:42
LastEditTime: 2023-08-13 10:51:28
LastEditors:  
'''
"""
the defination of data downloader
which is used to download different types of data from jq
"""

# import the Third party libraries
import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import jqdatasdk as jqdata
from tqdm import tqdm
import sqlite3
from abc import abstractmethod

# import outer libriaies
import sys
sys.path.append('./')
from const import *   # import the const.

# import inner libriaries
from classes.basic_classes.basic_calculator import DateCalculator, index_columns_check
from classes.basic_classes.basic_calculator import STOCK_LIST, TRADE_DATE_LIST



# login jq
jqdata.auth('15626416411', '31455Wwm')

# basic setting
year_list = DateCalculator().get_year_list(START_DATE, END_DATE)

finance = jqdata.finance
income = jqdata.income
balance = jqdata.balance
cash_flow = jqdata.cash_flow
indicator = jqdata.indicator


# defination
class DataDownloaderTemplate(object):
    """
    下载数据的基类，定义如下功能的虚拟函数：
    1. 数据下载
    2. 格式修改
    3. 数据保存
    """
    db_name = None
    table_name = None
    start_date = START_DATE
    end_date = END_DATE
    fq = REINSTATEMENT  # reinstatement method
    years_list = year_list
    stock_list = STOCK_LIST
    trade_date_list = TRADE_DATE_LIST


    @abstractmethod
    def get_data(self):
        """
        get data from jq
        """
        pass

    @abstractmethod
    def process_data(self):
        """
        process the strcution of data
        1. reindex
        2. datetime
        3. ...
        """
        pass

    @abstractmethod
    def sort_data(self):
        """
        sort the data
        """
        pass

    @abstractmethod
    def download_data(self):
        """
        use the functions of above
        connect to the db and close it in this function
        
        do not define this function in template: sometimes the data are download by stock, while other would be download by features
        """
        pass


class DailyFinancialDataDownloader(DataDownloaderTemplate):
    """
    download the daily financial data:
    1. roa：indicator.roa
    2. 经营活动现金净流量：cash_flow.net_operate_cash_flow
    3. 总资产：balance.total_assests
    4. 营业利润：income.operating_profit
    5. 长期负债：balance.total_non_current_liability
    6. 流动资产：balance.total_current_assets
    7. 流动负债：balance.total_current_liability
    8. 销售毛利率：indicator.gross_profit_margin
    9. 营业收入：income.operating_revenue
    """

    db_name = PATH_DB_DAILYFINANCIAL
    table_name = TABLE_NAME_DAILY_FINANCIAL
    financial_targets = [indicator.roa, cash_flow.net_operate_cash_flow, balance.total_assets, 
                         income.operating_profit, balance.total_non_current_liability, balance.total_current_assets, 
                         balance.total_current_liability, indicator.gross_profit_margin, income.operating_revenue]
    
    def process_data(self, df_data, year):
        """
        process the daily financial data:
        1. change the type of index to pd.datetime
        2. name the index after "date"
        """
        df_data.index = pd.to_datetime(df_data["day"])
        query_df_filtered = df_data.loc['{}-01-01'.format(year):'{}-12-31'.format(year),:]
        query_df_filtered.index.name = "date"
        return query_df_filtered
    
    def get_data(self, stock):
        """
        down load the daily financial data of single stock from jq
        """
        list_data = []
        q = jqdata.query(*self.financial_targets).filter(indicator.code.in_([stock])) #不同filter中的表明影响速度
        for year in self.years_list:
            query_df =  jqdata.get_fundamentals_continuously(q, end_date='{}-12-31'.format(year), count=365)
            # if query_df.empty :
            #     continue

            list_data.append(self.process_data(query_df, year))
        
        if len(list_data) == 0:  # 该股票无数据
            return None
        
        else:
            df_stock = pd.concat(list_data,axis = 0)
            return df_stock
    
    def sort_data(self, df_data, stock, con):
        """
        sort the data to database:
        1. df_data: the data that are supposed to be sorted
        2. stock: the name of stock, used to complete the table name
        3. con: the connection to db
        """
        if df_data is not None:
            table_name = self.table_name.format(stock)
            # df_data = df_data.iloc[0:-1, :]  # the inititalized date is later than the END_DATE
            df_data.to_sql(table_name, con=con, if_exists="replace")
        else:
            print(stock, "is None")
        pass

    def download_data(self):
        """
        download the daily financial data of all stocks
        """
        con = sqlite3.connect(self.db_name)

        for stock in tqdm(self.stock_list):
            data = self.get_data(stock)
            self.sort_data(df_data=data, stock=stock, con=con)
        con.close()
        pass


class DailyPriceDataDownloader(DataDownloaderTemplate):
    """
    download the daily price data:
    1. close
    2. open
    3. low
    4. high
    5. volume
    """

    db_name = PATH_DB_DAILYPRICE
    table_name = TABLE_NAME_DAILY_PRICE

    def get_data(self, stock):
        """
        down load the daily price data of single stock of single year from jq
        """
        price_data_df = jqdata.get_price(stock, 
                                         start_date=self.start_date, end_date=self.end_date,
                                         fq=self.fq)
        return price_data_df
    
    def process_data(self, df_data):
        """
        process the daily price data:
        1. change the type of index to pd.datetime
        2. name the index after "date"
        """
        df_data.index = pd.to_datetime(df_data.index)
        df_data.index.name = "date"
        return df_data
    
    def sort_data(self, df_data, stock, con):
        """
        sort the data to database:
        1. df_data: the data that are supposed to be sorted
        2. stock: the name of stock, used to complete the table name
        3. con: the connection to db
        """
        table_name = self.table_name.format(stock)
        df_data.to_sql(table_name, con=con, if_exists="replace")
        pass

    def download_data(self):
        """
        download the daily price data of all stocks
        """
        con = sqlite3.connect(self.db_name)

        for stock in tqdm(self.stock_list):
            data = self.get_data(stock)
            data = self.process_data(data)
            self.sort_data(df_data=data, stock=stock, con=con)
        
        con.close()
        pass


class DailyValuationDataDownloader(DataDownloaderTemplate):
    """
    download the daily valuation data:

    1. code	股票代码	带后缀.XSHE/.XSHG	
    2. day	日期	取数据的日期	
    3. capitalization	总股本(万股)	公司已发行的普通股股份总数(包含A股，B股和H股的总股本)	
    4. circulating_cap	流通股本(万股)	公司已发行的境内上市流通、以人民币兑换的股份总数(A股市场的流通股本)	
    5. market_cap	总市值(亿元)	A股收盘价*已发行股票总股本（A股+B股+H股）	
    6. circulating_market_cap	流通市值(亿元)	流通市值指在某特定时间内当时可交易的流通股股数乘以当时股价得出的流通股票总价值。	A股市场的收盘价*A股市场的流通股数
    7. turnover_ratio	换手率(%)	指在一定时间内市场中股票转手买卖的频率，是反映股票流通性强弱的指标之一。	换手率=[指定交易日成交量(手)100/截至该日股票流通股本(股)]100%
    8. pe_ratio	市盈率(PE, TTM)	每股市价为每股收益的倍数，反映投资人对每元净利润所愿支付的价格，用来估计股票的投资报酬和风险	市盈率（PE，TTM）=（股票在指定交易日期的收盘价 * 当日人民币外汇挂牌价* 截止当日公司总股本）/归属于母公司股东的净利润TTM。
    9. pe_ratio_lyr	市盈率(PE)	以上一年度每股盈利计算的静态市盈率. 股价/最近年度报告EPS	市盈率（PE）=（股票在指定交易日期的收盘价 * 当日人民币外汇牌价 * 截至当日公司总股本）/归属母公司股东的净利润。
    10. pb_ratio	市净率(PB)	每股股价与每股净资产的比率	市净率=（股票在指定交易日期的收盘价 * 截至当日公司总股本）/(归属母公司股东的权益MRQ-其他权益工具)。
    11. ps_ratio	市销率(PS, TTM)	市销率为股票价格与每股销售收入之比，市销率越小，通常被认为投资价值越高。	市销率TTM=（股票在指定交易日期的收盘价 * 当日人民币外汇牌价 * 截至当日公司总股本）/营业总收入TTM
    12. pcf_ratio	市现率(PCF, 现金净流量TTM)	每股市价为每股现金净流量的倍数	市现率=（股票在指定交易日期的收盘价 * 当日人民币外汇牌价 * 截至当日公司总股本）/现金及现金等价物净增加额TTM
    """

    db_name = PATH_DB_DAILYVALUATION
    table_name = TABLE_NAME_DAILY_VALUATION
    valuation_features = ["code", "day", "capitalization", "circulating_cap", 
                          "market_cap", "circulating_market_cap", "turnover_ratio", 
                          "pe_ratio", "pe_ratio_lyr", "pb_ratio", "ps_ratio", "pcf_ratio"]

    def get_data(self, stock):
        """
        down load the valuation price data of single stock of single year from jq
        """
        valuation_data_df = jqdata.get_valuation(stock, 
                                         start_date=self.start_date, end_date=self.end_date,
                                         fields=self.valuation_features)
        return valuation_data_df
    
    def process_data(self, df_data):
        """
        process the daily price data:
        1. change the type of index to pd.datetime
        2. name the index after "date"
        """
        df_data.index = pd.to_datetime(df_data["day"])
        df_data.index.name = "date"
        return df_data
    
    def sort_data(self, df_data, stock, con):
        """
        sort the data to database:
        1. df_data: the data that are supposed to be sorted
        2. stock: the name of stock, used to complete the table name
        3. con: the connection to db
        """
        table_name = self.table_name.format(stock)
        df_data.to_sql(table_name, con=con, if_exists="replace")
        pass

    def download_data(self):
        """
        download the daily price data of all stocks
        """
        con = sqlite3.connect(self.db_name)

        for stock in tqdm(self.stock_list):
            data = self.get_data(stock)
            data = self.process_data(data)
            self.sort_data(df_data=data, stock=stock, con=con)
        
        con.close()
        pass


class CertainEquityDataDownloader(DataDownloaderTemplate):
    """
    used to download the equity_changed data
    finance.STK_CAPITAL_CHANGE(单独成表)
    """

    db_name = PATH_DB_CERTAINEQUITY
    table_name = TABLE_NAME_CERTAIN_EQUITY

    def get_data(self, stock):
        equity_data = finance.run_query(jqdata.query(finance.STK_CAPITAL_CHANGE).filter(finance.STK_CAPITAL_CHANGE.code==stock))

        return equity_data

    def process_data(self, equity_data):
        """
        process the equity data:
        1. use the change_date as index
        2. change the index type as datetime
        3. rename the index as "date"
        """
        equity_data.index = pd.to_datetime(equity_data["pub_date"])
        equity_data.index.name = "date"
        return equity_data

    def sort_data(self, df_data, con, stock):
        table_name = self.table_name.format(stock)
        df_data.to_sql(table_name, con=con, if_exists="replace")
        pass

    def download_data(self):
        """
        download the certain stock data of all stocks
        """
        con = sqlite3.connect(self.db_name)

        for stock in tqdm(self.stock_list):
            data = self.get_data(stock)
            data = self.process_data(data)
            self.sort_data(df_data=data, stock=stock, con=con)
        con.close()
        pass

    
