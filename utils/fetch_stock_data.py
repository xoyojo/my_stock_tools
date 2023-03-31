# -*- encoding: utf-8 -*-
'''
@File    :   fetch_stock_data.py
@Time    :   2023/03/27 22:58:01
@Author  :   xoyojo 
@Version :   1.0
@Contact :   sduw.lj@gmail.com
@License :   
@Desc    :   None
'''

import datetime
import concurrent.futures
import pickle

# from akshare import stock_yjbb_em, stock_zh_a_hist, stock_zh_a_spot_em,index_stock_info
import akshare as ak
from pandas import DataFrame, concat

from log import logger


def fetch_one_stock_hist_from_ak(stock):
    """
    返回某只股票、指定周期和指定日期间的历史行情日频率数据
    Args: stock
        type:tunple ( (stock_code,stock_name), period, start_date, end_date, adjust)
    Return: stock_data
        type:daraframe
    """

    # stock_zh_a_hist：返回单只股票的历史行情日频率数据， https://www.akshare.xyz/data/stock/stock.html#id20
    try:
        if 'zs' in stock[0][0]:
            data = ak.index_zh_a_hist(symbol=stock[0][0][3:],
                                      period=stock[1],
                                      start_date=stock[2],
                                      end_date=stock[3])
        else:
            data = ak.stock_zh_a_hist(symbol=stock[0][0],
                                      period=stock[1],
                                      start_date=stock[2],
                                      end_date=stock[3],
                                      adjust=stock[4])
    except Exception as exc:
        print(f'request failed: unable to get {stock[0]} data: {exc}')
        return None

    data.columns = [
        'date', 'open', 'close', 'high', 'low', 'vol', 'amount', 'swing',
        'pct_chg', 'change', 'turn_over'
    ]
    # data = data.set_index('date')

    return data


def fetch_multi_stock_hist_from_ak(
        stocks,
        period="daily",
        start_date="20200201",
        end_date=datetime.datetime.now().strftime('%Y%m%d'),
        adjust=""):
    """
    返回沪深京 A 股指定股票、指定周期和指定日期间的历史行情日频率数据
    Args: stocks 
        type:list [('代码', '名称')]
    Return: stocks_data
        type:dict keys:('代码', '名称'); values: daraframe
    """
    stocks_data = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        future_to_stock = {
            executor.submit(fetch_one_stock_hist_from_ak,
                            (stock, period, start_date, end_date, adjust)):
            stock
            for stock in stocks
        }  # 通过submit提交执行的函数到线程池中
        # as_completed:当子线程中的任务执行完后，直接用 result() 获取返回结果
        for future in concurrent.futures.as_completed(future_to_stock):
            stock = future_to_stock[future]  # stock: tunple ('代码', '名称')
            try:
                data = future.result()
                if data is not None:
                    data = data.astype({'vol': 'double'})
                    stocks_data[stock] = data
            except TypeError as exc:
                logger.info(f'{stock}) is None.  exception: {exc}')
            except Exception as exc:
                logger.info(
                    f'{stock}) in future generated an exception: {exc}')
    return stocks_data


def get_data_from_all(stock_code, all_data):
    for key in all_data:
        if stock_code in key:
            value = all_data[key]
            return value


def fetch_one_quarter_bb_from_ak(date):
    """
    返回沪深京所有股票某季度的业绩报表数据
    Args: date
        type:str choice of {"XXXX0331", "XXXX0630", "XXXX0930", "XXXX1231"}; 从 20100331 开始
    Return: stock_data
        type:daraframe https://www.akshare.xyz/data/stock/stock.html#id136
    """

    try:
        data = ak.stock_yjbb_em(date=date)
    except TypeError as exc:
        logger.info(
            'fetch_one_quarter_bb_from_ak at %s generated an exception: %s',
            date, exc)
        return None
    except Exception as exc:
        logger.info(
            'fetch_one_quarter_bb_from_ak at %s generated an exception: %s',
            date, exc)
        return None
    return data


quarters = ["0331", "0630", "0930", "1231"]
date_list = [str(i) + q for i in range(2010, 2023) for q in quarters]


def fetch_all_quarter_bb_from_ak(date_list=date_list):
    """
    返回沪深京所有股票季度业绩报表数据
    Return: all_bb
        type:dataframe  https://www.akshare.xyz/data/stock/stock.html#id136
    """
    all_bb = DataFrame()

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        future_to_one_quarters = {
            executor.submit(fetch_one_quarter_bb_from_ak, date): date
            for date in date_list
        }

        for future in concurrent.futures.as_completed(future_to_one_quarters):
            date = future_to_one_quarters[future]
            try:
                data = future.result()
                if data is not None:
                    all_bb = concat([all_bb, data], axis=0)
            except Exception as exc:
                logger.info(
                    'fetch_all_quarter_bb_from_ak at %s generated an exception: %s',
                    date, exc)

    return all_bb


if __name__ == "__main__":

    # fetch_one_quarter_bb_from_ak("19910331")
    # fetch_one_quarter_bb_from_ak("19990331")
    # fetch_all_quarter_bb_from_ak(
    #     date_list=["20190331", "20200331", "19910331"])

    # stock_argument = (('600519', '贵州茅台'), "daily", "20200201", "20200228", "")
    # fetch_one_stock_hist_from_ak(stock_argument)
    # stocks_list = [('600519', '贵州茅台'), ('600020', '中原高速')]
    # res = res_stocks_data = fetch_multi_stock_hist_from_ak(
    #     stocks_list, start_date="20200201", end_date="20200228")
    # print(res)

    logger.info('start')

    quarters = ["0331", "0630", "0930", "1231"]
    quarter_data = [
        str(year) + quarter for quarter in quarters
        for year in range(2015, 2024)
    ]

    all_bb_df = fetch_all_quarter_bb_from_ak(date_list=quarter_data)

    with open('temp_data/all_yjbb.pkl', 'wb') as f:
        pickle.dump(all_bb_df, f)

    start_date = "20210101"
    # 获取沪深 A 股列表 http://quote.eastmoney.com/center/gridlist.html#hs_a_board
    all_stocks_df = ak.stock_zh_a_spot_em()
    all_indexs_df = ak.index_stock_info()
    # List [('代码', '名称')]
    all_stocks_list = [tuple(x) for x in all_stocks_df[['代码', '名称']].values]
    all_indexs_list = [
        ('zs.' + x[0], x[1])
        for x in all_indexs_df[['index_code', 'display_name']].values
    ]

    all_stocks_index_list = all_stocks_list + all_indexs_list
    df2 = fetch_multi_stock_hist_from_ak(stocks=all_stocks_index_list,
                                         start_date=start_date)
    with open('temp_data/stock_data_20210701_20230328.pkl', 'wb') as f:
        pickle.dump(df2, f)
