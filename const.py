'''
Description: 
Author: ymgu
Date: 2023-07-10 10:33:54
LastEditTime: 2023-07-14 15:20:39
LastEditors:  
'''
"""
defination of the const of the whole project
1. REINSTATEMENT: 复权
2. database
"""

# basic setting
REINSTATEMENT = "post"  # 价格复权方式：后复权
START_DATE = "2010-01-01"  # the start of the window
END_DATE = "2023-07-15"  # the end of the whole window

# database
ROOTPATH_DB = "database"  # the root path of databases, enable the change of the paths of all database

PATH_STOCKS_LIST = "{}/stock_list.csv".format(ROOTPATH_DB)  # the database for stock list

PATH_DB_DAILYPRICE = "{}/daily_price.db".format(ROOTPATH_DB)  # the database for daily price
PATH_DB_DAILYVALUATION = "{}/daily_valuation.db".format(ROOTPATH_DB)  # the database for daily valuation
PATH_DB_DAILYFINANCIAL = "{}/daily_financial.db".format(ROOTPATH_DB)  # the database for daily financial
PATH_DB_CERTAINEQUITY = "{}/certain_equity.db".format(ROOTPATH_DB)  # the database for daily financial

PATH_FOLDER_CROSSPRICE = "{}/cross_price".format(ROOTPATH_DB)  # the database for cross price
PATH_FOLDER_CROSSVALUATION = "{}/cross_valuation".format(ROOTPATH_DB)  # the database for cross valuation

PATH_FOLDER_FACTORS = "{}/factors".format(ROOTPATH_DB)  # the database for cross valuation

# tables names
TABLE_NAME_DAILY_PRICE = "daily___price___{}"
TABLE_NAME_DAILY_VALUATION = "daily__valuation___{}"
TABLE_NAME_DAILY_FINANCIAL = "daily___financial___{}"
TABLE_NAME_CERTAIN_EQUITY = "certain___equity___{}"

TABLE_NAME_CROSS_PRICE = "cross___price___{}.csv"
TABLE_NAME_CROSS_VALUATION = "cross___valuation___{}.csv"

TABLE_NAME_FACTORS = "features___{}.csv"

# factors list
FACTORS_LIST = ["fscore"] 

# factor params
REVERSAL_WINDOW = 50  # the window used to calculate reversal

# trading settings
SLIPPAGE = 0.003
COMMISSION = 0.0003


