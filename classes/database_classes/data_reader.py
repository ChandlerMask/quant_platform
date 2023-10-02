"""
defination of the data reader

which can be used to:
1. read all table names
2. read data for single stock
3. read data for all stock
"""

# import the Third party libraries
import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import jqdatasdk as jqdata
from tqdm import tqdm
import sqlite3
from abc import abstractmethod
import os

# import outer libriaies
import sys
sys.path.append('./')
from const import *   # import the const.

# import inner libriaries
from classes.basic_classes.basic_calculator import STOCK_LIST, TRADE_DATE_LIST



# defination of the template
class DataReaderTemplate(object):
    """
    DataReaderTempalte:
    used to read data from database:
    1. get all the table name
    2. get certain table
    3. get all table as a dict
    """
    def __init__(self):
        """
        1. get stock_list
        2. get trade_date_list
        """
        pass
    
    @abstractmethod
    def get_all_tables_names(self):
        pass
    
    @abstractmethod
    def data_process(self, data: pd.DataFrame):
        """
        process the struction of data from db:
        """
        pass
    
    @abstractmethod
    def get_one_table(self):
        """
        read table for certain stock from database
        """
        pass
    
    @abstractmethod
    def get_all_tables(self):
        """
        read all tables from database
        """
        pass
    
    pass


# the defination of db datareader
class DBDataReaderTemplate(object):
    """
    DataReaderTempalte:
    used to read data from database:
    1. get all the table name
    2. get certain table
    3. get all table as a dict
    """
    
    db_name = None
    table_name = None
    stock_list = STOCK_LIST
    trade_date_list = TRADE_DATE_LIST

    def __init__(self):
        """
        1. get stock_list
        2. get trade_date_list
        """
        pass
    
    def get_all_tables_names(self):
        con = sqlite3.connect(self.db_name)
        cursor = con.cursor()
        
        cursor.execute("select name from sqlite_master where type='table'")
        table_name=cursor.fetchall()
        table_name=[line[0] for line in table_name]

        con.close()
        return table_name
    
    def get_all_stocks(self):
        table_names = self.get_all_tables_names()
        stock_list = [table_name.split("___")[-1] for table_name in table_names]
        return stock_list
    
    def data_process(self, data: pd.DataFrame):
        """
        process the struction of data from db:
        1. change the index
        """
        data["date"] = pd.to_datetime(data["date"])
        data.set_index(keys="date", inplace=True, drop=True)
        return data 
    
    def get_one_table(self, stock):
        """
        read table for certain stock from database
        """
        con = sqlite3.connect(self.db_name)
        

        table_name = self.table_name.format(stock)
        df_stock = pd.read_sql("SELECT * FROM '{}'".format(table_name), con)
        df_stock = self.data_process(df_stock)

        con.close()
        return df_stock
    
    def get_all_tables(self):
        """
        read all tables from database
        """
        result_dict = {stock: self.get_one_table(stock) 
                       for stock in self.stock_list}
        return result_dict
    
    pass



# defination of different data reader
class DailyPriceDataReader(DBDataReaderTemplate):
    """
    used to read the daily price data from database
    """
    db_name = PATH_DB_DAILYPRICE
    table_name = TABLE_NAME_DAILY_PRICE
    pass


class DailyValuationDataReader(DBDataReaderTemplate):
    """
    used to read the daily valuation data from database
    """
    db_name = PATH_DB_DAILYVALUATION
    table_name = TABLE_NAME_DAILY_VALUATION
    pass


class DailyFinancialDataReader(DBDataReaderTemplate):
    """
    used to read the daily financial data from database
    """
    db_name = PATH_DB_DAILYFINANCIAL
    table_name = TABLE_NAME_DAILY_FINANCIAL
    pass


class CertainEquityDataReader(DBDataReaderTemplate):
    """
    used to read the daily financial data from database
    """
    db_name = PATH_DB_CERTAINEQUITY
    table_name = TABLE_NAME_CERTAIN_EQUITY
    pass



# the defination of folder datareader(cross_data)
# the defination of folder datareader(cross_data)
class FolderDataReaderTemplate(DataReaderTemplate):
    """
    the datareader template to read the cross_data in folder
    """
    db_name = None
    table_name = None
    stock_list = STOCK_LIST
    trade_date_list = TRADE_DATE_LIST

    def __init__(self):
        """
        1. get stock_list
        2. get trade_date_list
        """
        pass
    
    def get_all_tables_names(self):
        tables_names_list = []
        for file_name in os.listdir(self.db_name):
            tables_names_list.append(file_name)
        return tables_names_list
    

    def data_process(self, data: pd.DataFrame):
        """
        process the struction of data from db:
        1. change the index
        """
        data["date"] = pd.to_datetime(data["date"])
        data.set_index(keys="date", inplace=True, drop=True)
        return data 
    
    def get_one_table(self, feature):
        """
        read table for certain stock from database
        """
        table_name = self.table_name.format(feature)
        df_stock = pd.read_csv("{}/{}".format(self.db_name, table_name))
        df_stock = self.data_process(df_stock)
        return df_stock
    
    def _get_features_list(self):
        """
        get features list from tables_names_list
        """
        table_name_list = self.get_all_tables_names()
        features_list = [table_name.split("___")[-1].split(".")[0] for table_name in table_name_list]
        return features_list
    
    def get_all_tables(self):
        """
        read all tables from database
        """
        features_list = self._get_features_list()
        result_dict = {feature: self.get_one_table(feature) 
                       for feature in features_list}
        return result_dict
    
    pass


class CrossPriceDataReader(FolderDataReaderTemplate):
    db_name = PATH_FOLDER_CROSSPRICE
    table_name = TABLE_NAME_CROSS_PRICE

    # the defination of some simple processes
    def get_pct_data(self, data):
        return (data - data.shift(1)) / data.shift(1)  # today's data - yesterday's data
    
    def read_close(self):
        """
        read the close data
        """
        df_close = self.get_one_table(feature="close")
        return df_close
    
    def get_pct_close_data(self):
        df_close = self.read_close()
        df_pct_close = self.get_pct_data(df_close)
        return df_pct_close
    
    def read_open(self):
        """
        read the open data
        """
        df_close = self.get_one_table(feature="open")
        return df_close
    
    pass

class CrossValuationDataReader(FolderDataReaderTemplate):
    db_name = PATH_FOLDER_CROSSVALUATION
    table_name = TABLE_NAME_CROSS_VALUATION
    pass


# factors reader
class FactorsDataReader(FolderDataReaderTemplate):
    db_name = PATH_FOLDER_FACTORS
    table_name = TABLE_NAME_FACTORS

    factors_list = FACTORS_LIST

    def get_fscores(self):
        return self.get_one_table(feature="fscore")
    
    def get_reversals(self):
        return self.get_one_table(feature="reversal")
    
    def get_valuations(self):
        return self.get_one_table(feature="valuation")
    pass

class FscoreDataReader(FolderDataReaderTemplate):
    db_name = PATH_FOLDER_FACTORS
    table_name = TABLE_NAME_FACTORS

    factors_list = FACTORS_LIST

    def get_fscores(self):
        return self.get_one_table(feature="fscore")
    
    pass


class ReversalDataReader(FolderDataReaderTemplate):
    db_name = PATH_FOLDER_FACTORS
    table_name = TABLE_NAME_FACTORS

    factors_list = FACTORS_LIST

    def get_reversals(self):
        return self.get_one_table(feature="reversal")
    
    pass
