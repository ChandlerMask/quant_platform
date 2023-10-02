'''
Description: 
Author: czl
Date: 2023-08-10 14:53:17
LastEditTime: 2023-08-13 20:26:43
LastEditors:  
'''
"""
used to update the databases
"""

import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import jqdatasdk as jqdata
from tqdm import tqdm
import sqlite3
from abc import abstractmethod
import datetime

# import outer libriaies
import sys
sys.path.append('./')
from const import *   # import the const
from classes.basic_classes.basic_calculator import STOCK_LIST, TRADE_DATE_LIST, BasicCalculator
from classes.database_classes.data_downloader import DailyPriceDataDownloader, DailyValuationDataDownloader, DailyFinancialDataDownloader, CertainEquityDataDownloader
from classes.database_classes.data_reader import DailyPriceDataReader, DailyValuationDataReader, DailyFinancialDataReader, CertainEquityDataReader, CrossPriceDataReader, CrossValuationDataReader, FactorsDataReader

# variables setting
TODAY_DATE = str(datetime.date.today())
STOCK_LIST_TODAY = BasicCalculator(end_date=TODAY_DATE).get_stock_list()  # get the lastest stock list

finance = jqdata.finance
income = jqdata.income
balance = jqdata.balance
cash_flow = jqdata.cash_flow
indicator = jqdata.indicator

# defination of DataUpdaterTemplate
class DataUpdaterTemplate(object):
    """
    used to update the data:
    functions:
        1. get new data: get new data after the last day in database
        2. process data
        3. sort_data: append the new data into the database
    """

    datadownloader_name = None  # the according datadownloader, used to get db name, file path and download data for new stock
    data_reader = None
    # start_date = UPDATE_DATE
    end_date = TODAY_DATE
    # count = TIME_SPLIT
    stock_list = STOCK_LIST_TODAY
    # original_stock_list = LAST_STOCK_LIST  
    fq = REINSTATEMENT  # reinstatement method
    
    index_name = None

    def __init__(self):
        self.datadownloader = self.datadownloader_name()
        self.db_name = self.datadownloader.db_name
        self.table_name = self.datadownloader.table_name
        self.start_date, self.original_stock_list = self.get_last_situation()
        self.count = (pd.to_datetime(TODAY_DATE) - pd.to_datetime(self.start_date)).days  # used to find the new stocks: use the downloader rather than updater
        pass

    def get_last_situation(self):
        """
        used to get the last date and stock_list of the according database
        get the last date from the first table from daily_price database
        """
        stock = STOCK_LIST[0]
        df_price = self.data_reader.get_one_table(stock=stock)
        start_date = str(df_price.index[-1] + datetime.timedelta(days=1))
        
        last_table_name_list = self.data_reader.get_all_tables_names()
        last_stock_list = [table_name.split("___")[-1] for table_name in last_table_name_list]
        return start_date.split(" ")[0], last_stock_list  # delete the hour、mintue and second

    def process_data(self, df_data, index_name=None):
        """
        process the daily financial data:
        1. change the type of index to pd.datetime
        2. filter the new data: just for sure, this can be deleted
        2. name the index after "date"
        """
        if index_name is not None:   # use the index_name column as the index
            df_data.index = pd.to_datetime(df_data[index_name])
        else:  # just change the data type pf index
            df_data.index = pd.to_datetime(df_data.index)
            
        query_df_filtered = df_data[df_data.index > self.start_date]  # this can be a waste of time, as I use the count when get data from jq
        query_df_filtered.index.name = "date"
        return query_df_filtered
    
    @abstractmethod
    def get_new_data(self, stock):
        pass

    def sort_new_data(self, df_data, stock, con):
        """
        sort the data to database:
        1. df_data: the data that are supposed to be sorted
        2. stock: the name of stock, used to complete the table name
        3. con: the connection to db
        """
        if df_data is not None:
            table_name = self.table_name.format(stock)
            # df_data = df_data.iloc[0:-1, :]  # the inititalized date is later than the END_DATE
            df_data.to_sql(table_name, con=con, if_exists="append")  # use "append", while the datadownloader would use the method of "replace"
        else:
            print(stock, "is None")
        pass

    def update_data(self):
        """
        used to update the data:
        1. if the stock is in the original stock list: just upadte the data
        2. else: for the new stocks, we will use the downloader to download
        """
        print("the lastest stock list have {} stocks".format(len(self.stock_list)))
        print("the original stock list have {} stocks".format(len(self.original_stock_list)))
        con = sqlite3.connect(self.db_name)

        for stock in tqdm(self.stock_list):

            if stock in self.original_stock_list:
                # print(stock)
                data = self.get_new_data(stock)
                processed_data = self.process_data(data, index_name=self.index_name)
                self.sort_new_data(df_data=processed_data, 
                                   stock=stock, con=con)
            else:  # for new stock
                print("{} is a new stock".format(stock))
                data = self.datadownloader.get_data(stock)
                self.datadownloader.sort_data(df_data=data, stock=stock, con=con)
            
        con.close()
        pass    
    pass


# defination of different DataUpdater
class DailyFinancialDataUpadter(DataUpdaterTemplate):
   
    datadownloader_name = DailyFinancialDataDownloader
    data_reader = DailyFinancialDataReader()
    index_name = "day"

    def __init__(self):
        super().__init__()
        self.financial_targets = self.datadownloader.financial_targets
        pass

    def get_new_data(self, stock):
        """
        if the stock has already existed in the database,
        then use this function to get the new data from jq
        """
        q = jqdata.query(*self.financial_targets).filter(indicator.code.in_([stock]))
        query_df =  jqdata.get_fundamentals_continuously(q, end_date=self.end_date,  count=self.count)
        return query_df

    pass

class DailyPriceDataUpdater(DataUpdaterTemplate):

    datadownloader_name = DailyPriceDataDownloader
    data_reader = DailyPriceDataReader()
    index_name = None
    
    def __init__(self):
        super().__init__()
        pass
    
    def get_new_data(self, stock):
        """
        down load the daily price data of single stock of single year from jq
        """
        price_data_df = jqdata.get_price(stock, 
                                         start_date=self.start_date, end_date=self.end_date,
                                         fq=self.fq)
        return price_data_df

    pass


class DailyValuationDataUpadter(DataUpdaterTemplate):
   
    datadownloader_name = DailyValuationDataDownloader
    data_reader = DailyValuationDataReader()
    index_name = "day"

    def __init__(self):
        super().__init__()
        self.valuation_features = self.datadownloader.valuation_features
        pass

    def get_new_data(self, stock):
        """
        down load the valuation price data of single stock of single year from jq
        """
        valuation_data_df = jqdata.get_valuation(stock, 
                                         start_date=self.start_date, end_date=self.end_date,
                                         fields=self.valuation_features)
        return valuation_data_df

    pass


class CertainEquityDataUpdater(DataUpdaterTemplate):

    datadownloader_name = CertainEquityDataDownloader
    data_reader = CertainEquityDataReader()
    index_name = "pub_date"
    
    def __init__(self):
        super().__init__()
        pass

    def update_data(self):
        self.datadownloader.download_data()  # as the data vulume for each stock is not so large, therefore, redownload the whole database would be faster
        pass


# the functions that can update all the databases
def data_updating():
    """
    used to update all the databases from jqdata
    """
    print("start to update the daily financial data")
    DailyFinancialDataUpadter().update_data()
    print("start to update the daily price data")
    DailyPriceDataUpdater().update_data()
    print("start to update the daily valuation data")
    DailyValuationDataUpadter().update_data()
    print("start to update the certain equity data")
    CertainEquityDataUpdater().update_data()
    pass