"""
used to transfer the time frequency of signals:
1. monthly and weekly
2. lastest and mean
"""

import pandas as pd
import numpy as np
from abc import abstractmethod
import datetime

# import outer libriaies
import sys
sys.path.append('./')
from const import *   # import the const.

# import inner libriaries
from classes.basic_classes.basic_functions import DateCalculator, index_columns_check
from classes.basic_classes.basic_calculator import STOCK_LIST, TRADE_DATE_LIST

# defination of SignalTransferTemplate

class SignalTransferTemplate(object):
    """
    used to tranfer the signal to a lower frequency
    """
    @abstractmethod
    def _get_calculating_dates(self, df_siganl: pd.DataFrame):
        """
        used to find the calculating date: DataCalculator
        """
        pass

    @abstractmethod
    def _transfering_the_signals(self, df_signal_window: pd.DataFrame):
        """
        defination the method of unifying the signals in a window
        """
        pass

    def signal_transfering(self, df_signal: pd.DataFrame):
        """
        tranfer the signals to a lower frequency
        """
        calculating_date_list = self._get_calculating_dates(df_signal=df_signal)
        df_result = pd.DataFrame(0, index=df_signal.index, columns=df_signal.columns)

        for i in range(len(calculating_date_list)-1):  # the first window and the last window are droped
            past_date = calculating_date_list[i-1]  # the start date of observing window
            now_date = calculating_date_list[i]  # the end date of observing window
            future_date = calculating_date_list[i+1]  # the end date of trading window

            if i == 0:
                df_signal_window = df_signal[df_signal.index<=now_date]
            else:
                df_signal_window = df_signal[np.logical_and(df_signal.index>past_date, df_signal.index<=now_date)]
                
            df_result[np.logical_and(df_result.index>now_date, df_result.index<=future_date)] = self._transfering_the_signals(df_signal_window)
        return df_result
    
    pass
    

class MonthlyLastSignalTransfer(SignalTransferTemplate):
    """
    used to transfer the daily signal to monthly frequency, 
    and the signal would be those in the last day of last month
    """
    @abstractmethod
    def _get_calculating_dates(self, df_signal: pd.DataFrame):
        """
        used to find the calculating date: DataCalculator
        """
        start_date = df_signal.index[0]
        end_date = df_signal.index[-1]
        return DateCalculator().get_monthly_last_date(start_date=start_date, end_date=end_date)

    @abstractmethod
    def _transfering_the_signals(self, df_signal_window: pd.DataFrame):
        """
        defination the method of unifying the signals in a window
        """
        return df_signal_window.iloc[-1, :]
    
    
class WeeklyLastFactorTransfer(SignalTransferTemplate):
    """
    used to transfer the daily factor to a weekly frquency,
    all the factor would be calculated after the close of friday,
    and the factor of a week would be calculated on the last friday
    """
    def _get_calculating_dates(self, df_signal: pd.DataFrame):
        """
        used to find the calculating date: DataCalculator
        """
        date_list = df_signal.index
        return DateCalculator().get_every_friday(date_list=date_list)
        
    def _transfering_the_signals(self, df_signal_window: pd.DataFrame):
        """
        defination the method of unifying the factors in a window
        """
        return df_signal_window.iloc[-1, :]
