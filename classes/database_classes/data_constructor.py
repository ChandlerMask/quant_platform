"""
used to construct some structions on the data from datareader(Database)
"""

# import the Third party libraries
import pandas as pd 
import sqlite3
# import outer libriaies
import sys
sys.path.append('./')
from const import *   # import the const.

# import inner libriaries
from classes.basic_classes.basic_calculator import STOCK_LIST, TRADE_DATE_LIST
from classes.database_classes.data_reader import DailyPriceDataReader, DailyValuationDataReader

class DataConstructorTemplate(object):
    """
    the template used to process data:
    1. connected to certain datareader

    functions:
    1. get the same columns of different columns into the same table
    """
    datareader = None
    
    def __init__(self):
        self.stock_list = STOCK_LIST
        self.features_list = self.get_features_list()
        pass

    def get_features_list(self):
        """
        used to get the features list of the tables in the database
        """
        data = self.datareader.get_one_table(stock=self.stock_list[0])
        return list(data.columns)

    def get_one_feature_cross(self, feature_name: str):
        """
        used to concentrate the same feature in different tables into the same table
        """
        result_list = []
        for stock in self.stock_list:
            data_df = self.datareader.get_one_table(stock=stock)
            data_series = data_df[feature_name]
            data_series.name = stock
            result_list.append(data_series)
        df_feature = pd.concat(result_list, axis=1)
        return df_feature
    
    def get_all_features_cross(self):
        """
        used to concentrate the same features in different tables into the same table and sort them into a dict
        """
        result_list_dict = {feature: [] for feature in self.features_list}

        for stock in self.stock_list:
            data_df = self.datareader.get_one_table(stock=stock)  # read the data of a stock

            for feature_name in self.features_list:  # allocate the columns into differnet lists
                data_series = data_df[feature_name]
                data_series.name = stock
                result_list_dict[feature_name].append(data_series)
        
        result_dict = {feature: pd.concat(result_list_dict[feature], axis=1) 
                       for feature in self.features_list}
        return result_dict
    

class DailyPriceDataConstructor(DataConstructorTemplate):
    datareader = DailyPriceDataReader()
    pass

class DailyValuationDataConstructor(DataConstructorTemplate):
    datareader = DailyValuationDataReader()
    pass



class CrossDataInitializer(object):

    def __init__(self):
        self.cross_price_path = PATH_FOLDER_CROSSPRICE
        self.cross_valuation_path = PATH_FOLDER_CROSSVALUATION

        self.cross_price_table_name = TABLE_NAME_CROSS_PRICE
        self.cross_valuation_table_name = TABLE_NAME_CROSS_VALUATION
        pass

    def initialize_cross_price_data(self):
        cross_price_dict = DailyPriceDataConstructor().get_all_features_cross()
        for feature in cross_price_dict:
            table_name = self.cross_price_table_name.format(feature)
            path = "{}/{}".format(self.cross_price_path, table_name)
            cross_price_dict[feature].to_csv(path)
        pass

    def initialize_cross_valuation_data(self):
        """
        used to initialized the cross valuation data:
        """
        cross_valuation_dict = DailyValuationDataConstructor().get_all_features_cross()
        for feature in cross_valuation_dict:
            table_name = self.cross_valuation_table_name.format(feature)
            path = "{}/{}".format(self.cross_valuation_path, table_name)
            cross_valuation_dict[feature].to_csv(path)
        pass

    def all_cross_data_initialize(self):
        self.initialize_cross_price_data()
        self.initialize_cross_valuation_data()
        pass