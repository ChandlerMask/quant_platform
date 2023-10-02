"""
basic calcultor
which is used to calculate some basic settings, which can be changed by the setting of "const", like:
1. stock list
2. trade date list
"""

# import the Third party libraries
import pandas as pd 
import jqdatasdk as jqdata

# import outer libriaies
import sys
sys.path.append('./')
from const import *   # import the const.

jqdata.auth('15626416411', '31455Wwm')
# defination

class BasicCalculator(object):
    """
    get some basic settings:
    1. stock_list
    2. trade_date_list
    """
    def __init__(self, start_date=START_DATE, end_date=END_DATE) -> None:  # if do nott get the variables, then use the setting as consts as defaults
        self.start_date = start_date  # the first date of the window
        self.end_date = end_date  # the last date of the window
        pass

    def get_stock_list(self):
        """
        get the stock list
        """
        try: 
            stocks_df = pd.read_csv(PATH_STOCKS_LIST)
            stocks_df.set_index("Unnamed: 0", inplace=True, drop=True)
        except FileNotFoundError :
            stocks_df = jqdata.get_all_securities(date=self.end_date)
            stocks_df.to_csv(PATH_STOCKS_LIST)
        
        stocks_list = list(stocks_df.index)
        return stocks_list
    
    def get_trade_date_list(self):
        """
        get the list of trade dates of the window
        """
        trade_date_list = jqdata.get_trade_days(start_date=self.start_date, 
                                                end_date=END_DATE)
        return trade_date_list
    
    pass


baisc_calculator = BasicCalculator()
STOCK_LIST = baisc_calculator.get_stock_list()
TRADE_DATE_LIST = baisc_calculator.get_trade_date_list()