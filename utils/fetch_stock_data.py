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
import logging
import concurrent.futures

from akshare import stock_yjbb_em, stock_zh_a_hist
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
        data = stock_zh_a_hist(symbol=stock[0][0],
                               period=stock[1],
                               start_date=stock[2],
                               end_date=stock[3],
                               adjust=stock[4])
    except Exception as exc:
        print(f'request failed: unable to get stock data: {exc}')
        return None

    if data is None or data.empty:
        logging.debug(f'股票:{stock},没有数据，略过...')
        return

    data.columns = [
        'date', 'open', 'close', 'high', 'low', 'vol', 'amount', 'swing',
        'pct_chg', 'change', 'turn_over'
    ]
    data = data.set_index('date')

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
            except Exception as exc:
                print(f'{stock[1]}({stock[1]}) generated an exception: {exc}')
    return stocks_data


def fetch_one_quarter_bb_from_ak(date):
    """
    返回沪深京所有股票某季度的业绩报表数据
    Args: date
        type:str choice of {"XXXX0331", "XXXX0630", "XXXX0930", "XXXX1231"}; 从 20100331 开始
    Return: stock_data
        type:daraframe https://www.akshare.xyz/data/stock/stock.html#id136
    """

    try:
        data = stock_yjbb_em(date=date)
    except TypeError as exc:
        logger.info(
            f'fetch_one_quarter_bb_from_ak at {date} generated an exception: {exc}'
        )
        return None
    except Exception as exc:
        logger.info(
            f'fetch_one_quarter_bb_from_ak at {date} generated an exception: {exc}'
        )
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
                    f'fetch_all_quarter_bb_from_ak at {date} generated an exception: {exc}'
                )

    return all_bb


if __name__ == "__main__":

    # fetch_one_quarter_bb_from_ak("19910331")
    # fetch_one_quarter_bb_from_ak("19990331")
    # fetch_all_quarter_bb_from_ak(
    #     date_list=["20190331", "20200331", "19910331"])

    # stock_argument = (('600519', '贵州茅台'), "daily", "20200201", "20200228", "")
    # fetch_one_stock_hist_from_ak(stock_argument)
    # stocks_list = [('600519', '贵州茅台'), ('600172', '黄河旋风')]
    # res = res_stocks_data = fetch_multi_stock_hist_from_ak(
    #     stocks_list, start_date="20200201", end_date="20200228")

    # all_data = ak.stock_zh_a_spot_em()  # 获取沪深 A 股列表 http://quote.eastmoney.com/center/gridlist.html#hs_a_board
    # subset = all_data[['代码', '名称']]
    # stocks_list = [tuple(x) for x in subset.values]  # List [('代码', '名称')]

    import pickle
    from akshare import stock_zh_a_spot_em

    # df1 = fetch_all_quarter_bb_from_ak()
    # with open('temp_data/all_yjbb.pkl', 'wb') as f:
    #     pickle.dump(df1, f)

    # 获取沪深 A 股列表 http://quote.eastmoney.com/center/gridlist.html#hs_a_board
    all_stocks = stock_zh_a_spot_em()
    stocks = [tuple(x)
              for x in all_stocks[['代码', '名称']].values]  # List [('代码', '名称')]
    df2 = fetch_multi_stock_hist_from_ak(stocks=stocks, start_date='20210701')
    with open('temp_data/stock_data_20210701_20230328.pkl', 'wb') as f:
        pickle.dump(df2, f)
