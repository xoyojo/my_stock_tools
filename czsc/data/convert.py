# -*- coding: utf-8 -*-
'''
@File    :   convert.py
@Time    :   2023/03/24 15:19:08
@Author  :   xoyojo 
@Version :   1.0
@Site    :   https://
@Desc    :   None
'''

import pandas as pd
import numpy as np


def convert_bao_to_ts(df: pd.DataFrame) -> pd.DataFrame:
    """将包含多个周期的数据转换为时间序列数据

    Args:
        df (pd.DataFrame): 包含多个周期的数据

    Returns:
        pd.DataFrame: 时间序列数据
    """
    df.rename(columns={
    'date': 'trade_date',
    'code': 'ts_code',
    'preclose':'pre_close',
    'volume':'vol',
    'pctChg':'pct_chg',
    }, inplace=True)
    df['ts_code'] = df['ts_code'].apply(lambda x: x[3:] + '.' + x[:2].upper())
    #object 转换为 float64
    df['open'] = df['open'].astype(np.float64)
    df['close'] = df['close'].astype(np.float64)
    df['high'] = df['high'].astype(np.float64)
    df['low'] = df['low'].astype(np.float64)
    df['vol'] = df['vol'].astype(np.float64)
    df['amount'] = df['amount'].astype(np.float64)
    df['pre_close'] = df['pre_close'].astype(np.float64)
    # df['change'] = df['change'].astype(np.float64)
    df['pct_chg'] = df['pct_chg'].astype(np.float64)

    return df

