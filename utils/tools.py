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


def tool_trade_date_hist(strat_date, end_date):
    """
    返回指定日期间的交易日历
    Args: strat_date, end_date
    """
    tool_trade_date_hist_sina_df = ak.tool_trade_date_hist_sina()
    # 截取tool_trade_date_hist_sina_df 1991-04-03到2023-03-30的数据
    tool_trade_date_hist_sina_df = tool_trade_date_hist_sina_df[
        tool_trade_date_hist_sina_df['trade_date'].apply(
            lambda x: pd.Timestamp(x)) >= pd.Timestamp(strat_date)]
    tool_trade_date_hist_sina_df = tool_trade_date_hist_sina_df[
        tool_trade_date_hist_sina_df['trade_date'].apply(
            lambda x: pd.Timestamp(x)) <= pd.Timestamp(end_date)]

    tool_trade_date_hist_sina_df.rename(columns={'trade_date': 'date'})

    return tool_trade_date_hist_sina_df


def tool_data_cleaning_by_trade_date_hist(df, start_date, end_date):
    """
    数据清洗
    """
    trade_dates = tool_trade_date_hist(start_date, end_date)

    df['date'] = pd.to_datetime(df['date'])
    trade_dates['trade_date'] = pd.to_datetime(trade_dates['trade_date'])

    new_df = trade_dates.merge(df,
                               how='left',
                               left_on='trade_date',
                               right_on='date')

    #close：如为Nan，有前值使用前值填充，无前值使用后值填充
    #date：如为Nan，使用suspension填充（停盘）
    #open,high,low：如为Nan，使用close填充
    #其它值：如为Nan，使用0填充

    new_df['close'] = new_df['close'].fillna(method='ffill')
    new_df['close'] = new_df['close'].fillna(method='bfill')
    new_df['date'] = new_df['date'].fillna('suspension')
    new_df['open'] = new_df['open'].fillna(new_df['close'])
    new_df['high'] = new_df['high'].fillna(new_df['close'])
    new_df['low'] = new_df['low'].fillna(new_df['close'])
    new_df = new_df.fillna(0)

    return new_df
