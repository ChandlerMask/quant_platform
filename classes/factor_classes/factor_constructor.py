'''
Description: 
Author: czl
Date: 2023-07-14 15:21:21
LastEditTime: 2023-07-24 12:37:51
LastEditors:  
'''
"""
factor_constructor
used to calculate and sort the factors for all stocks
1. connect to the datareader
2. use the factorcalculator

factor_initializer:
used to calculate all factors and then sort them in the folder
1. use the factor_constructor
2. sort the results
"""

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
from const import *   # import the const

# import inner libriaries
from classes.basic_classes.basic_calculator import STOCK_LIST, TRADE_DATE_LIST
from classes.database_classes.data_reader import DailyPriceDataReader, DailyValuationDataReader, DailyFinancialDataReader, CertainEquityDataReader, CrossPriceDataReader, CrossValuationDataReader
from classes.factor_classes.factor_calcualtor import FscoreCalculator, ReversalFactorCalculator, ValuationFactorCalculator


# class the factorconstructor template
class FactorConstructorTemplate(object):
    """
    the template used to calculate differents and the sort them for all stocks
    1. connect to all datareaders
    2. calculate factors
    """
    daily_price_data_reader = DailyPriceDataReader()
    daily_financial_data_reader = DailyFinancialDataReader()
    daily_valuation_data_reader = DailyValuationDataReader()
    certain_equity_data_reader = CertainEquityDataReader()

    factor_calcualtor = None

    def __init__(self, stock_list=STOCK_LIST) -> None:
          self.stock_list = stock_list
          pass
    
    @abstractmethod
    def calculate_factor_for_one(self, stock):
        """
        calculate the factor for one stock
        """
        pass

    def calculate_factor(self):
        result_list = []
        for stock in self.stock_list:
            result_list.append(self.calculate_factor_for_one(stock))

        return pd.concat(result_list, axis=1)
    pass


class FscoreConstructor(FactorConstructorTemplate):
    """
    calculate the f_score for all tocks
    """
    factor_calcualtor = FscoreCalculator()

    def calculate_factor_for_one(self, stock):
        """
        calcualte fscore for one stock
        """
        df_financial = self.daily_financial_data_reader.get_one_table(stock=stock)
        df_equity = self.certain_equity_data_reader.get_one_table(stock=stock)
        f_series = self.factor_calcualtor.calculate_factor(equity_data=df_equity, financial_data=df_financial)
        f_series.name = stock

        return f_series
    pass


class ReversalConstructor(FactorConstructorTemplate):
    """
    calculate the reversal for all stocks
    """
    factor_calcualtor = ReversalFactorCalculator()

    def calculate_factor_for_one(self, stock):
        """
        calcualte reversal for one stock
        """
        df_price = self.daily_price_data_reader.get_one_table(stock=stock)
        reversal_series = self.factor_calcualtor.calculate_factor(price_data=df_price)
        reversal_series.name = stock

        return reversal_series
    pass


class ValuationConstructor(FactorConstructorTemplate):
    """
    calculate the valuation for all stocks
    """
    factor_calcualtor = ValuationFactorCalculator()

    def calculate_factor_for_one(self, stock):
        """
        calcualte reversal for one stock
        """
        df_valuation = self.daily_valuation_data_reader.get_one_table(stock=stock)
        valuation_series = self.factor_calcualtor.calculate_factor(valuation_data=df_valuation)
        valuation_series.name = stock

        return valuation_series
    pass

# defination of Fastors_initializer
class FactorsInitializer(object):
    """
    used to calcualte all the factors for all stocks and sort the result
    """
    db_name = PATH_FOLDER_FACTORS
    table_name = TABLE_NAME_FACTORS

    def _get_file_path(self, factor_name: str):
        """
        used to the file_path to sort the factor file
        """
        factor_table_name = self.table_name.format(factor_name)
        path = "{}/{}".format(self.db_name, factor_table_name)
        return path

    def fscore_initializer(self):
        df_f_score = FscoreConstructor().calculate_factor()
        path = self._get_file_path("fscore")
        df_f_score.to_csv(path)
        pass

    def reversal_initalizer(self):
        df_reversal = ReversalConstructor().calculate_factor()
        path = self._get_file_path("reversal")
        df_reversal.to_csv(path)
        pass

    def valuation_initalizer(self):
        df_reversal = ReversalConstructor().calculate_factor()
        path = self._get_file_path("valuation")
        df_reversal.to_csv(path)
        pass

    # the integration of factors initialization
    def initialize_all_factors(self):
        self.fscore_initializer()
        self.reversal_initalizer()
        self.valuation_initalizer()
        pass

