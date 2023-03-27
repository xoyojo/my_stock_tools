# -*- encoding: UTF-8 -*-

import akshare as ak
import logging
import talib as tl

import concurrent.futures
#
import datetime
import pickle
import os.path



def fetch(code_name,start_date="20200201"):
    stock = code_name[0]
    # stock_zh_a_hist：返回单只股票的历史行情日频率数据， https://www.akshare.xyz/data/stock/stock.html#id20
    data = ak.stock_zh_a_hist(symbol=stock, period="daily", start_date=start_date, adjust="qfq")

    if data is None or data.empty:
        logging.debug("股票："+stock+" 没有数据，略过...")
        return

    data['p_change'] = tl.ROC(data['收盘'], 1)

    return data


def run(stocks,start_date="20200201"):
    """
    返回沪深京 A 股全部股票指定周期和指定日期间的历史行情日频率数据
    Args: stocks 
        type:list [('代码', '名称')]
    Return: stocks_data
        type:dict keys:('代码', '名称'); values: daraframe
    """

    stocks_data = {}

    # current_time = datetime.datetime.now().strftime("%Y-%m-%d")
    # stocks_data_dump_filename = 'stocks_data_{}.pkl'.format(current_time)
    # stocks_data_dump_file = 'stocks_data/' + stocks_data_dump_filename
    # if os.path.exists(stocks_data_dump_file):
    #     with open(stocks_data_dump_file, "rb") as f:
    #         stocks_data = pickle.load(f)
    # else:
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        future_to_stock = {executor.submit(fetch, stock): stock for stock in stocks}
        for future in concurrent.futures.as_completed(future_to_stock):
            stock = future_to_stock[future]
            try:
                data = future.result()
                if data is not None:
                    data = data.astype({'成交量': 'double'})
                    stocks_data[stock] = data
            except Exception as exc:
                print('%s(%r) generated an exception: %s' % (stock[1], stock[0], exc))
        
        # with open(stocks_data_dump_file, "wb") as f:
        #     pickle.dump(stocks_data,f)
    
    
    return stocks_data
