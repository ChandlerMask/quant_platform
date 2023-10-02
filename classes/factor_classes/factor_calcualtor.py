'''
Description: 
Author: czl
Date: 2023-07-13 11:33:57
LastEditTime: 2023-07-18 22:46:03
LastEditors:  
'''
"""
used to calculate different factors
"""

# import the Third party libraries
import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import datetime
from tqdm import tqdm
import sqlite3
from abc import abstractmethod

# import outer libriaies
import sys
sys.path.append('./')
from const import *   # import the const.

# import inner libriaries
from classes.basic_classes.basic_functions import DateCalculator, index_columns_check
from classes.basic_classes.basic_calculator import STOCK_LIST, TRADE_DATE_LIST


# defination
class FactorCalculatorTemplate(object):
    """
    used to calculate factor
    """
    stock_list = STOCK_LIST
    trade_date_list = TRADE_DATE_LIST 

    def lag_one_year(self, data):
        """
        Calculate one year lag
        """
        return (data - data.shift(250))
    
    @abstractmethod
    def calculate_factor(self):
        pass

    pass


class FscoreCalculator(FactorCalculatorTemplate):
    """
    used to calculate the f-score
    """
    ## the scoring of financial data
    def scoring_bigger_zero(self, data):
        """
        when the data bigger than 0, get 1
        """
        score = data.apply(lambda x: 1 if x > 0 else 0)
        return score
    
    def scoring_smaller_zero(self, data):
        """
        when the data smaller than 0, get 1
        """
        score = data.apply(lambda x: 1 if x < 0 else 0)
        return score
    
    def _cal_roa(self, financial_data):
        data_roa = financial_data["roa"]
        data_roa.name = "roa"
        return data_roa
    
    def _cal_delta_roa(self, financial_data):
        data_roa = financial_data["roa"]
        data_delta_roa = self.lag_one_year(data_roa)
        data_delta_roa.name = "delta_roa"
        return data_delta_roa
    
    def _cal_cfoa(self, financial_data):
        data_cofa = financial_data["net_operate_cash_flow"]/ financial_data["total_assets"]
        data_cofa.name = "cofa"
        return data_cofa
    
    def _cal_accured_profit(self, financial_data):
        data_profit = (financial_data["operating_profit"] - financial_data["net_operate_cash_flow"]) / financial_data["total_assets"]
        data_profit.name = "accured_profit"
        return data_profit
    
    def _cal_delta_lever(self, financial_data):
        data_lever = financial_data["total_non_current_liability"] - financial_data["total_assets"]
        data_lever_delta = self.lag_one_year(data_lever)
        data_lever_delta.name = "lever_delta"
        return data_lever_delta
    
    def _cal_delta_liquid(self, financial_data):
        data_liquid = financial_data["total_current_assets"]/financial_data["total_current_liability"]
        data_delta_liquid = self.lag_one_year(data_liquid)
        data_delta_liquid.name = "delta_liquid"
        return data_delta_liquid
    
    def _cal_delta_margin(self, financial_data):
        data_margin = financial_data["gross_profit_margin"]
        data_delta_margin = self.lag_one_year(data_margin)
        data_delta_margin.name = "delta_margin"
        return data_delta_margin
    
    def _cal_delta_turn(self, financial_data):
        data_turn = financial_data["operating_revenue"] - financial_data["total_assets"]
        data_delta_turn = self.lag_one_year(data_turn)
        data_delta_turn.name = "delta_turn"
        return data_delta_turn
    
    ## the scoring of the equity data 
    def _find_certain_equity_change(self, change_reason_series):
        """
        find the index of certain equity change
        """
        date_list = []
        for date in change_reason_series.index:
            reason = change_reason_series.loc[date]
            if (isinstance(reason, str)) and (("增发" in reason) or ("配售" in reason)):  # determine whether "增发" and "配售" has happend
                date_list.append(date)
        return date_list



    def scoring_equity(self, equity_data, financial_data):
        """
        get the score for the equity change
        1. find the date of certain changes
        2. change the scores in a year after certain changes
        """

        equity_score = pd.Series(index=financial_data.index, data=0, name="equity_change")  # initialize the scores

        change_reason_series = equity_data["change_reason"]

        change_date_list = self._find_certain_equity_change(change_reason_series)

        for date in change_date_list:
            
            change_date = date + datetime.timedelta(1)  # the change start in the next date
            year, month, day = change_date.year, change_date.month, change_date.day
            
            last_date = DateCalculator().get_date_next_year(date)  # the last date of the change
            equity_score.loc[change_date: last_date] = 1
        return equity_score


    def calculate_factor(self, financial_data, equity_data):
        result_list = []

        # get the score of financial data
        result_list.append(self.scoring_bigger_zero(self._cal_roa(financial_data)))
        result_list.append(self.scoring_bigger_zero(self._cal_delta_roa(financial_data)))
        result_list.append(self.scoring_bigger_zero(self._cal_cfoa(financial_data)))
        result_list.append(self.scoring_bigger_zero(self._cal_delta_margin(financial_data)))
        result_list.append(self.scoring_bigger_zero(self._cal_delta_turn(financial_data)))

        result_list.append(self.scoring_smaller_zero(self._cal_accured_profit(financial_data)))
        result_list.append(self.scoring_smaller_zero(self._cal_delta_lever(financial_data)))
        result_list.append(self.scoring_smaller_zero(self._cal_delta_liquid(financial_data)))
        
        
        
        # get the score of equity data
        result_list.append(self.scoring_equity(equity_data=equity_data, financial_data=financial_data))

        df_fscore = pd.concat(result_list, axis=1)
        return df_fscore.sum(axis=1)
    

class ReversalFactorCalculator(FactorCalculatorTemplate):
    """
    used to calculate reversal factor
    """
    window = REVERSAL_WINDOW  # the window used to calculate reversal
    def calculate_factor(self, price_data: pd.DataFrame):
        close_series = np.log(price_data["close"])
        return close_series - close_series.shift(self.window)
    
    pass


class ValuationFactorCalculator(FactorCalculatorTemplate):
    """
    used to calculate valuation factor
    """
    def calculate_factor(self, valuation_data: pd.DataFrame):
        valuation_series = valuation_data["market_cap"]
        return valuation_series
    
    pass
