"""
used to back_test the strategy:
1. used to estimate the result of the strategy:
    1. receive the open_data and signal_data
    2. return the dataframe which records the returns of each group in each column

2. used to calcualte IC and so on
    1. receive the open_pct_data and the factor_data
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


## bact_tester defination
class BackTesterTemplate(object):
    """
    used to estimate the result of the strategy:
    1. receive the open_data and signal_data
    2. return the dataframe which records the returns of each group in each column

    3. functions:
        1. calcualte the pct_open data
        2. get all groups
        3. calcualte the return of each group
    """

    stock_list = STOCK_LIST
    trade_date_list = TRADE_DATE_LIST

    def __init__(self, open_df: pd.DataFrame, singnal_df: pd.DataFrame) -> None:
        self.open_df = open_df
        self.signal_df = singnal_df
        self.return_df = self.get_return_df()  # get the return of each group, which can be used to calcualte the sharp and so on
        pass 

    @abstractmethod
    def cal_pct_return(self):
        """
        calculate the pct_open
        today's signal would be opened in tomorrow, and close in the day after tomorrow
        """
        pass
    
    def _get_all_groups(self):
        """
        get all groups
        """
        group_list = list(np.unique(self.signal_df))
        return group_list
    
    @abstractmethod
    def get_return_df(self):
        pass

    def get_cumprod_return(self):
        return (self.return_df+1).cumprod(axis=0)
    
    def get_tradable_cumprod_return(self):
        """
        drop the group 0
        """
        cumpord_return = self.get_cumprod_return()
        cumpord_return.drop([0], inplace=True, axis=1)
        return cumpord_return
    
    # calculate the calmar, sharp, IC
    def calculate_max_drawdown(self, return_series: pd.Series):
        """
        use the return_series(not cumpord) to culcalate the max_drawdown
        """
        return_series_drop = return_series.dropna()
        cumpord_return = (return_series_drop+1).cumprod()
        pre_max = cumpord_return.expanding(min_periods=1).max()  # find the top value before everyday
        return (1 - cumpord_return/pre_max).max()
    
    def calculate_yearly_return(self, return_series: pd.Series):
        """
        use the return_series(not cumpord) to culcalate the yearly_return
        """
        return_series_drop = return_series.dropna()
        return return_series_drop.mean() * 250
    
    def calculate_sharp(self, return_series: pd.Series):
        """
        use the return_series(not cumpord) to culcalate the sharp
        """
        return_series_drop = return_series.dropna()
        return return_series_drop.mean() / return_series_drop.std()
    
    def calculate_calmar(self, return_series: pd.Series):
        """
        use the return_series(not cumpord) to culcalate the calmar
        """
        return_series_drop = return_series.dropna()
        max_drawdown = self.calculate_max_drawdown(return_series=return_series_drop)
        return return_series_drop.mean()/max_drawdown
    
    def get_all_indicators(self):
        """
        used to get max_drawdown, yearly_return, sharp and calmar for ifferent groups
        """
        result_dict = {column: [] for column in self.return_df.columns}

        for column in self.return_df.columns:
            return_series = self.return_df.loc[:, column]
            result_dict[column].append(self.calculate_max_drawdown(return_series=return_series))
            result_dict[column].append(self.calculate_yearly_return(return_series=return_series))
            result_dict[column].append(self.calculate_sharp(return_series=return_series))
            result_dict[column].append(self.calculate_calmar(return_series=return_series))

        df_indicator = pd.DataFrame(result_dict, index=["max_drawdown", "yearly_return", "sharp", "calmar"])
        return df_indicator

    def get_return_plot(self):
        cumpord_return_plot = self.get_cumprod_return()

        fig = plt.figure(figsize=(12,4))
        plt.plot(cumpord_return_plot)
        plt.legend(cumpord_return_plot.columns)
        plt.title("cumpord_return")
        plt.show()

        pass

    def _get_untradable_columns(self, columns_list:list):
        """
        used to find untradabel columns in both single ranking and double ranking
        """ 
        if columns_list.max() > 100:  # double ranking
            drop_list = [column for column in columns_list if (column//100==0) or (column%10==0)] 
        else: 
            drop_list = [0]

        return drop_list

    def get_tradable_return_plot(self):
        """
        used to plot the cumpord returns of tradable groups
        """
        cumpord_return_plot = self.get_cumprod_return()

        # try:  # single rank
        #     cumpord_return_plot.drop([0], inplace=True, axis=1)
        # except KeyError:  # double rank
        drop_list = self._get_untradable_columns(columns_list=cumpord_return_plot.columns)
        cumpord_return_plot.drop(drop_list, inplace=True, axis=1)

        fig = plt.figure(figsize=(12,4))
        plt.plot(cumpord_return_plot)
        plt.legend(cumpord_return_plot.columns)
        plt.title("tradeable_cumprod_return")
        plt.show()

        pass  


class SimpleBackTester(BackTesterTemplate):
    """
    uesd to backtest the result of strategies without slippage and commission
    """

    def cal_pct_return(self):
        """
        calculate the pct_open
        today's signal would be opened in tomorrow, and close in the day after tomorrow
        """
        return (self.open_df.shift(-2) / self.open_df.shift(-1))-1  # today's signal would be opened in tomorrow, and close in the day after tomorrow

    def _get_return_one_group(self, group, pct_return, signal_df):
        """
        calcualte the return for one group
        """ 
        return pct_return[signal_df==group].mean(axis=1)  # get the mean
        
    def get_return_df(self):
        """
        calcualte the return for each group
        """

        # get the data
        group_list = self._get_all_groups()
        pct_return = self.cal_pct_return()

        result_dict = {group: self._get_return_one_group(group=group, 
                                                         pct_return=pct_return, 
                                                         signal_df=self.signal_df) 
                                                         for group in tqdm(group_list)}
        
        return pd.DataFrame(result_dict)
    

class CompletedBackTester(BackTesterTemplate):
    """
    used to backtest the result of strategies considering the slipage and commission
    1. find the date of open and close in each group
        sometimes, one stock would be opened in one group while closed in the other group
    2. adjust the open prices according to the open and close date
    """
    def __init__(self, open_df: pd.DataFrame, singnal_df: pd.DataFrame,
                 slippage=SLIPPAGE, commission=COMMISSION) -> None:
        self.slippage = slippage
        self.commission = commission
        super().__init__(open_df, singnal_df)

        pass

    def _find_open_date(self, signal_df: pd.DataFrame, group):
        """
        used to find the date of open:
        1. the signal is group
        2. the signal in the last date is group
        """
        open_signal = np.logical_and((signal_df == group), (signal_df.shift(1) != group))
        return open_signal
    
    def _find_close_date(self, signal_df: pd.DataFrame, group):
        """
        used to find the date of open:
        1. the signal is not group
        2. the signal in the last date is group
        """
        close_signal = np.logical_and((signal_df != group), (signal_df.shift(1) == group))
        return close_signal
    
    def _get_adjust_df(self, adjust_ratio, adjust_signal: pd.DataFrame):
        """
        used to transfer the the adjust_signal(open_signal or close_signal) to adjust_df
        for True in adjust_signal, the next date get the adjust_ratio,
        for False in the adjust_signal, the next date get 1(the price remain the same)
        """
        adjust_signal = adjust_signal.shift(1)  # the price would be adjusted in the next date
        adjust_signal.replace(True, adjust_ratio, inplace=True)  # the next date get the adjust_ratio
        adjust_signal.replace(False, 1, inplace=True)  # the next date get 1(the price remain the same)
        return adjust_signal
    
    def _get_adjusted_open_df(self, open_df: pd.DataFrame, 
                              open_signal: pd.DataFrame, close_signal: pd.DataFrame):
        """
        increas the price in the date after open_signal
        decrease the price in the date after close_signal
        """
        adjust_ratio = 1 + self.slippage + self.commission
        open_adjust_signal = self._get_adjust_df(adjust_ratio=adjust_ratio, adjust_signal=open_signal)
        close_adjust_signal = self._get_adjust_df(adjust_ratio=adjust_ratio, adjust_signal=close_signal)

        adjusted_open_df = open_df * open_adjust_signal / close_adjust_signal
        return adjusted_open_df
    
    def cal_pct_return(self, group):
        """
        calculate the pct_open
        today's signal would be opened in tomorrow, and close in the day after tomorrow

        since sometimes the one stock would be opened and closed in a siame date in different groups,
        therefore, the pct_return is different for group to group
        """
        open_df = self.open_df
        signal_df = self.signal_df

        # adjust the price according to the close and open
        open_signal = self._find_open_date(signal_df=signal_df,group=group)
        close_signal = self._find_close_date(signal_df=signal_df, group=group)

        adjusted_open_df = self._get_adjusted_open_df(open_df=open_df, open_signal=open_signal, 
                                                      close_signal=close_signal)

        return (adjusted_open_df.shift(-2) / adjusted_open_df.shift(-1))-1  # today's signal would be opened in tomorrow, and close in the day after tomorrow
    
    def _get_return_one_group(self, group, signal_df):
        """
        calcualte the return for one group
        """

        pct_return = self.cal_pct_return(group=group)
        return pct_return[signal_df==group].mean(axis=1)  # get the mean
        
    def get_return_df(self):
        """
        calcualte the return for each group
        """

        # get the data
        group_list = self._get_all_groups()

        result_dict = {group: self._get_return_one_group(group=group, signal_df=self.signal_df) 
                       for group in tqdm(group_list)}
        
        return pd.DataFrame(result_dict)


## IC definaiton
def get_IC(self, return_df: pd.DataFrame, factor_df: pd.DataFrame):
    """
    use the return_df and signal_df to calcualte the rank
    """
    return_rank = return_df.rank(axis=1)
    factor_rank = factor_df.rank(axis=1)

    result_list = []

    for index, row in return_rank.iterrows():
        return_rank_row = row.dropna()
        factor_rank_row = factor_rank.loc[index].dropna()
        result_list.append(return_rank_row.corr(factor_rank_row, method='spearman'))
    
    df_IC = pd.DataFrame(index=return_rank.index, data=result_list, columns=["IC"])

    return df_IC




