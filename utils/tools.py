# -*- coding: utf-8 -*-
'''
@File    :   tools.py
@Time    :   2023/03/31 11:04:26
@Author  :   xoyojo 
@Version :   1.0
@Site    :   https://
@Desc    :   None
'''

import akshare as ak
import pandas as pd
import pickle
# from log import logger


def fetch_trading_calendar(start_date, end_date):
    """
    return trade dates between start_date and end_date
    Args:   type:str (e.g. '2021-01-01')
    Return: type:dataframe (timestamp index, no column) 
    """
    trade_dates_hist = ak.tool_trade_date_hist_sina()
    trade_dates_hist['trade_date'] = pd.to_datetime(
        trade_dates_hist['trade_date'])
    trade_dates_hist.rename(columns={'trade_date': 'date'}, inplace=True)
    trade_dates_hist.set_index('date', inplace=True)

    return trade_dates_hist[start_date:end_date]


def align_stock_data_to_trading_calendar(stock_df, start_date, end_date):
    """
    due to the suspension of stock market, the data of some days is missing
    this function is used to align the data to trading calendar
    Args:   stock_df
                type:dataframe
                date:str (e.g. '2021-01-01')
    Return: new_df
                type:dataframe
                index:timestamp of trading calendar
                columns:
                    trade_date:str (e.g. '2021-01-01')
        
    """
    trading_calendar = fetch_trading_calendar(start_date, end_date)

    stock_df['trade_date'] = stock_df['date']
    stock_df['date'] = pd.to_datetime(stock_df['date'])
    stock_df.set_index('date', inplace=True)

    new_df = trading_calendar.merge(stock_df,
                                    how='left',
                                    left_index=True,
                                    right_index=True)

    #close：如为Nan，有前值使用前值填充，无前值使用后值填充
    #date：如为Nan，使用suspension填充（停盘）
    #open,high,low：如为Nan，使用close填充
    #其它值：如为Nan，使用0填充

    new_df['close'] = new_df['close'].fillna(method='ffill')
    new_df['close'] = new_df['close'].fillna(method='bfill')
    new_df['trade_date'] = new_df['trade_date'].fillna('suspension')
    new_df['open'] = new_df['open'].fillna(new_df['close'])
    new_df['high'] = new_df['high'].fillna(new_df['close'])
    new_df['low'] = new_df['low'].fillna(new_df['close'])
    new_df = new_df.fillna(0)

    return new_df


def align_reprot_data_to_trading_calendar(stock_code,
                                          start_date=None,
                                          end_date=None,
                                          dates=None):
    """

    Args:

    Returns:

    """

    if (start_date is None or end_date is None) and dates is None:
        raise Exception('(start_date and end_date) or dates must be provided')

    with open('../temp_data/all_yjbb.pkl', 'rb') as f:
        all_reports = pickle.load(f)
    report_data = all_reports[all_reports['股票代码'] == stock_code].sort_values(
        by='最新公告日期', ascending=True)
    report_data.drop_duplicates(subset=['最新公告日期'], keep='last', inplace=True)
    
    report_data['最新公告日期'] = pd.to_datetime(report_data['最新公告日期'])
    report_data.set_index('最新公告日期', inplace=True)
    trade_dates = dates if dates is not None else fetch_trading_calendar(
        start_date, end_date)
    trade_dates['date'] = trade_dates.index

    new_df = pd.merge_asof(report_data.loc[start_date:end_date],
                                                 trade_dates,
                                                 left_index=True,
                                                 right_index=True,
                                                 direction='backward')

    new_df.reset_index(inplace=True)
    new_df.rename(columns={'最新公告日期': 'report_date'}, inplace=True)
    new_df.set_index('date', inplace=True)
    new_df['report_date'] = new_df['report_date'].dt.strftime('%Y-%m-%d')

    return new_df




if __name__ == "__main__":

    from fetch_stock_data import fetch_one_stock_hist_from_ak

    start_date = '20140101'
    end_date = '20140630'
    symbol = '000001'
    period = 'daily'

    stock_info = (('000001', '深圳银行'), period, start_date, end_date, '')
    # index_info = (('zs.000001', '上证指数'), period, start_date, end_date)

    stock_data = fetch_one_stock_hist_from_ak(stock_info)

    #删除stock_data中的0到20行,40到45行,用于测试
    stock_data = stock_data.drop(stock_data.index[0:20])
    stock_data = stock_data.drop(stock_data.index[40:45])

    stock_df = align_stock_data_to_trading_calendar(stock_data, start_date,
                                                    end_date)

    print(stock_df[stock_df['trade_date'] == 'suspension'].head(1))
    print(stock_df[stock_df['trade_date'] != 'suspension'].head(1))
    print(type(stock_df['trade_date'].iloc[-1]))
    print(type(stock_df.index[0]))

    print('----------------tet align_reprot_data_to_trading_calendar----------------')
    # dates = ['2020-08-06', '2020-08-07', '2020-08-10', '2020-08-11', '2020-08-12',]
    # align_reprot_data_to_trading_calendar('600519',dates=dates)
    # align_reprot_data_to_trading_calendar('600519')
    bb_df = align_reprot_data_to_trading_calendar('600519',start_date = '2018-08-06',end_date='2022-03-29')
    print(type(bb_df.index[0]))
    print(type(bb_df['report_date'].iloc[1]))