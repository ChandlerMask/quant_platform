"""
used to update the factors
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
from classes.basic_classes.basic_functions import DateCalculator
from classes.basic_classes.basic_calculator import STOCK_LIST, TRADE_DATE_LIST
from classes.database_classes.data_downloader import DailyPriceDataDownloader, DailyValuationDataDownloader, DailyFinancialDataDownloader, CertainEquityDataDownloader
from classes.database_classes.data_reader import DailyPriceDataReader, DailyValuationDataReader, DailyFinancialDataReader, CertainEquityDataReader, CrossPriceDataReader, CrossValuationDataReader, FactorsDataReader
from classes.factor_classes.factor_calcualtor import FscoreCalculator, ReversalFactorCalculator, ValuationFactorCalculator

# the functions to get the time_split for 2 years
def get_date_last_two_year(date):
    """
    the the same date in last two year
    """
    date_calculator = DateCalculator()
    lasy_year_date = date_calculator.get_date_last_year(date)
    last_two_year_date = date_calculator.get_date_last_year(lasy_year_date)
    return last_two_year_date

TODAY_DATE = datetime.date.today()
LAST_TWO_YEAR_DATE = get_date_last_two_year(TODAY_DATE)

# the definaiton of FactorUpdaterTemplate
class FactorUpdaterTemplate(object):
    """
    the template used to calculate and the sort them for all stocks
    1. connect to all datareaders, slice the data
    2. calculate factors
    3. reader the original database
    4. append and get the new factor data
    """
    daily_price_data_reader = DailyPriceDataReader()
    daily_financial_data_reader = DailyFinancialDataReader()
    daily_valuation_data_reader = DailyValuationDataReader()
    certain_equity_data_reader = CertainEquityDataReader()
    factors_data_reader = FactorsDataReader()

    slice_date = LAST_TWO_YEAR_DATE
    factor_calcualtor = None

    def __init__(self, stock_list=STOCK_LIST) -> None:
          self.stock_list = stock_list
          pass

    def _slice_data_two_years(self, data: pd.DataFrame):
        """
        used to get the data just for last two years, 
        so as to reduce the culcalating expense
        """
        return data[data.index >= pd.to_datetime(self.slice_date)]
    
    @abstractmethod
    def calculate_last_factor_for_one(self, stock):
        """
        calculate the factor for one stock in the last two year
        """
        pass

    def calculate_last_factor(self):
        result_list = []
        for stock in tqdm(self.stock_list):
            result_list.append(self.calculate_last_factor_for_one(stock))

        return pd.concat(result_list, axis=1)

    @abstractmethod
    def get_original_factor(self) -> pd.DataFrame:
        pass

    def get_updated_factor(self)->pd.DataFrame:
        original_factor = self.get_original_factor()
        last_factor = self.calculate_last_factor()

        # merge the original and new factor
        slice_date = original_factor.index[-1]
        new_factor  = last_factor[last_factor.index>slice_date]
        return pd.concat([original_factor, new_factor], axis=0)
    pass

# the defination of different factor_updater
class FscoreUpdater(FactorUpdaterTemplate):
    """
    calculate the f_score for all tocks
    """
    factor_calcualtor = FscoreCalculator()

    def calculate_last_factor_for_one(self, stock):
        """
        calcualte fscore for one stock
        """
        df_financial = self._slice_data_two_years(self.daily_financial_data_reader.get_one_table(stock=stock))
        df_equity = self._slice_data_two_years(self.certain_equity_data_reader.get_one_table(stock=stock))
        f_series = self.factor_calcualtor.calculate_factor(equity_data=df_equity, financial_data=df_financial)
        f_series.name = stock

        return f_series

    def get_original_factor(self) -> pd.DataFrame:
        return self.factors_data_reader.get_fscores()
    pass


class ReversalUpdater(FactorUpdaterTemplate):
    """
    calculate the reversal for all stocks
    """
    factor_calcualtor = ReversalFactorCalculator()

    def calculate_last_factor_for_one(self, stock):
        """
        calcualte reversal for one stock
        """
        df_price = self._slice_data_two_years(self.daily_price_data_reader.get_one_table(stock=stock))
        reversal_series = self.factor_calcualtor.calculate_factor(price_data=df_price)
        reversal_series.name = stock

        return reversal_series

    def get_original_factor(self) -> pd.DataFrame:
        return self.factors_data_reader.get_reversals()
    pass


class ValuationUpdater(FactorUpdaterTemplate):
    """
    calculate the valuation for all stocks
    """
    factor_calcualtor = ValuationFactorCalculator()

    def calculate_last_factor_for_one(self, stock):
        """
        calcualte reversal for one stock
        """
        df_valuation = self._slice_data_two_years(self.daily_valuation_data_reader.get_one_table(stock=stock))
        valuation_series = self.factor_calcualtor.calculate_factor(valuation_data=df_valuation)
        valuation_series.name = stock

        return valuation_series
    
    def get_original_factor(self) -> pd.DataFrame:
        return self.factors_data_reader.get_valuations()
    pass


# defination of Fastors_updater: which would be used when update the factors
class FactorsUpdater(object):
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

    def fscore_updater(self):
        df_f_score = FscoreUpdater().get_updated_factor()
        path = self._get_file_path("fscore")
        df_f_score.to_csv(path)
        pass

    def reversal_updater(self):
        df_reversal = ReversalUpdater().get_updated_factor()
        path = self._get_file_path("reversal")
        df_reversal.to_csv(path)
        pass

    def valuation_updater(self):
        df_reversal = ReversalUpdater().get_updated_factor()
        path = self._get_file_path("valuation")
        df_reversal.to_csv(path)
        pass

    # the integration of factors initialization
    def update_all_factors(self):
        self.fscore_updater()
        self.reversal_updater()
        self.valuation_updater()
        pass
    pass