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

# here put the import lib

import datetime
import logging
import concurrent.futures

import talib as tl
import akshare as ak
from requests.exceptions import RequestException


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
        data = ak.stock_zh_a_hist(symbol=stock[0][0],
                                  period=stock[1],
                                  start_date=stock[2],
                                  end_date=stock[3],
                                  adjust=stock[4])
    except RequestException as exc:
        print(f'request failed: unable to get stock data: {exc}')
        return False

    if data is None or data.empty:
        logging.debug(f'股票:{stock},没有数据，略过...')
        return
   
    #change data columns name
    data.columns = ['date','open', 'high', 'low', 'close', 'vol', 'amount','swing', 'pct_chg','change', 'turn_over']
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


if __name__ == "__main__":
    # all_data = ak.stock_zh_a_spot_em() # 获取沪深 A 股列表 http://quote.eastmoney.com/center/gridlist.html#hs_a_board
    # subset = all_data[['代码', '名称']]
    # stocks = [tuple(x) for x in subset.values] # List [('代码', '名称')]
    stocks_list = [('600519', '贵州茅台'), ('600172', '黄河旋风')]
    res_stocks_data = fetch_multi_stock_hist_from_ak(stocks_list,
                                                     start_date="20200201",
                                                     end_date="20200228")
    print(res_stocks_data)
