'''
Description: 
Author: czl
Date: 2023-08-05 16:16:42
LastEditTime: 2023-08-08 10:32:41
LastEditors:  
'''
"""
defination of the basic functions
1. date calculator
2. dataframe check
"""

import pandas as pd
import datetime

# the defination of date_calculator
class DateCalculator(object):
    """
    used to analyse the date
    """

    def get_year_list(self, start_date, end_date):

        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

        start_year = start_date.year
        end_year = end_date.year

        year_list = [year for year in range(start_year, end_year+1)]
        return year_list
    
    def get_date_next_year(self, date):
        """
        use to the the same date in the next year:
        when the date in 2-29, get 2-28 in next year
        """

        year, month, day = date.year, date.month, date.day
        if month==2 and day==29:
            return pd.to_datetime("{}-{}-{}".format(year+1, month, 28))
        else:
            return pd.to_datetime("{}-{}-{}".format(year+1, month, day))
        
        pass

    def get_date_last_year(self, date):
        """
        use to the the same date in the next year:
        when the date in 2-29, get 2-28 in next year
        """

        year, month, day = date.year, date.month, date.day
        if month==2 and day==29:
            return pd.to_datetime("{}-{}-{}".format(year-1, month, 28))
        else:
            return pd.to_datetime("{}-{}-{}".format(year-1, month, day))
        
        pass

    def get_months_list(self, start_date, end_date):
        """
        used to find all months between the start_date and last_date
        """
        months = (end_date.year - start_date.year)*12 + (end_date.month - start_date.month)
        month_list = [(start_date.year+month//12, month%12+1) for month in range(start_date.month-1, start_date.month+months)]
        return month_list
    
    def get_monthly_first_date(self, start_date, end_date):
        """
        used to find first date of every month
        """
        month_list = self.get_months_list(start_date=start_date, end_date=end_date)
        first_date_list = pd.to_datetime([datetime.date(year=month[0], month=month[1], day=1) for month in month_list])
        return first_date_list
    
    def get_monthly_last_date(self, start_date, end_date):
        """
        used to find last date of every month
        """
        month_list = self.get_months_list(start_date=start_date, end_date=end_date)
        next_month_list = [(month[0]+(month[1])//12, month[1]%12+1) for month in month_list]
        last_date_list = pd.to_datetime([(datetime.date(year=month[0], month=month[1], day=1) - datetime.timedelta(days=1))for month in next_month_list])
        return last_date_list
    
    def get_every_friday(self, date_list):
        """
        used to find every friday in the date list
        """
        friday_list = [date for date in date_list if date.weekday()==4]
        return friday_list

    pass

# defination of dataframe checking
def index_columns_check(data_1: pd.DataFrame, data_2: pd.DataFrame):
    if (data_1.index != data_2.index).all() :
        raise IndexError("the indexes of two dataframe are not same, please have a check")
    elif (data_1.columns != data_2.columns).all() :
        raise IndexError("the columns of two dataframe are not same, please have a check")
    else:
        pass


