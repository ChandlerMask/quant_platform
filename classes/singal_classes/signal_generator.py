"""
used to generate signal
"""

import pandas as pd 
import numpy as np
import os

# import outer libriaies
import sys
sys.path.append('./')
from const import *   # import the const .

# import inner libriaries
from classes.basic_classes.basic_calculator import STOCK_LIST, TRADE_DATE_LIST
from classes.basic_classes.basic_functions import index_columns_check

class SignalGeneratorTemplate(object):
    """
    the signal_generators would receive one or more factors, and used them to gernator the signal.
    the template to generate the signals
    the template havs these functions:
    1. rank the stocks in everyday, in the same day, every stock would not share the same rank
    2. allocate the stocks into differnet groups acoording to the rank
    3. set the threeshold for the first and last group.
    """

    stock_list = STOCK_LIST
    trade_date_list = TRADE_DATE_LIST

    def daily_rank(self, data: pd.DataFrame):
        """
        rank the stocks in everyday
        """
        return data.rank(axis=1, na_option="keep", method="first", ascending=True)
    
    def _group_by_rank(self, daily_data, groups_num:int):
        """
        used to allocate stocks in one day into different groups
        daily_data: a rows from the ranked data
        groups_num: the number of groups
        """

        max_num = daily_data.max()  # get the max rank
        split = max_num/groups_num  # the number of stocks in every group

        series_result = pd.Series(0, name=daily_data.name, index=daily_data.index)  # the place to sort the grouping result
        

        for i in range(groups_num):
            series_result[np.logical_and((daily_data > i *split), (daily_data <= (i+1)*split))] = i+1

        return series_result

    def group_by_rank(self, ranked_data, groups_num):
        """
        used to allocate stocks in many days into different groups
        ranked_data: the rank of stocks
        groups_num: the number of groups
        """

        return ranked_data.apply(lambda x: self._group_by_rank(x, groups_num=groups_num), axis=1)
    
    def get_signal(self, df_factor: pd.DataFrame, groups_num:int=5):
        ranked_data = self.daily_rank(df_factor)
        return self.group_by_rank(ranked_data=ranked_data, groups_num=groups_num)
    

class DoubleGenerator(SignalGeneratorTemplate):
    """
    used to practice the double ranking:
    1. independent double ranking
    2. dependent double ranking: the process of ranking is operated step by step
    """

    def _merge_signals(self, df_signal_1: pd.DataFrame, df_signal_2:pd.DataFrame):
        """
        used to merge the signal of two ranking:
        1. received the signals as input
        2. the output signals are tuples
        """
        index_columns_check(df_signal_1, df_signal_2)  # check if the indexes and the columns of the two dataframes are same
        # df_signal_merged = "("+df_signal_1.astype("str") + ","+df_signal_2.astype("str")+")"
        df_signal_merged = 100*df_signal_1 + df_signal_2  # transfer the str to tuple
        return df_signal_merged

    def get_signal_independent(self, df_factor_1: pd.DataFrame, df_factor_2: pd.DataFrame, groups_num:int=5):
        """
        used for independent ranking
        """
        signal_data_1 = self.get_signal(df_factor_1, groups_num=groups_num)
        signal_data_2 = self.get_signal(df_factor_2, groups_num=groups_num)
        df_result = self._merge_signals(signal_data_1, signal_data_2)
        return df_result

    def _get_second_signal(self, df_singal_first: pd.DataFrame, group: int, 
                           df_factor_second:pd.DataFrame, groups_num):
        """
        used to rank the second factor in each group of first setp
        """

        df_factor_group = df_factor_second[df_singal_first == group]  # get the factor values in certain group
        df_signal_group = self.get_signal(df_factor=df_factor_group, groups_num=groups_num)
        return df_signal_group

    def get_signal_dependent(self, df_factor_first: pd.DataFrame, df_factor_second: pd.DataFrame, groups_num:int=5):
        """
        used for dependent ranking
        1. ranking for the first factor
        2. ranking in each group of the second factor
        """
        signal_data_first = self.get_signal(df_factor_first, groups_num=groups_num)  # the first step

        signal_data_second = pd.DataFrame(0, index=df_factor_second.index, columns=df_factor_second.columns)
        for group in range(1, groups_num+1):  # second step of ranking in each group
            signal_data_second += self._get_second_signal(df_singal_first=signal_data_first, group=group, 
                                                          df_factor_second=df_factor_second, groups_num=groups_num)
        
        df_result = self._merge_signals(signal_data_first, signal_data_second)
        return df_result

    pass