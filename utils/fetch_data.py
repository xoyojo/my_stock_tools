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
import os

import akshare as ak
import pandas as pd

from log import logger


def fetch_hist(
        symbol='zs.000001',
        period="daily",
        start_date=(datetime.datetime.now() -
                    datetime.timedelta(days=365 * 3)).strftime('%Y%m%d'),
        end_date=datetime.datetime.now().strftime('%Y%m%d'),
        adjust="qfq"):
    """
    返回某只股票、指定周期和指定日期间的历史行情日频率数据
    Args:   symbol_list
                type:list e.g. '600519' 'zs.000001' ..., where zs represents the index
            adjust: qfq, hfq, ''
    Return: data
            type: dataframe
                index: date
                columns: 'date', 'open', 'close', 'high', 'low', 'vol', 'amount', 'swing','pct_chg', 'change', 'turn_over'

    """

    # 如果start_date的格式为'%Y-%m-%d',转换为'%Y%m%d'
    if len(start_date) == 10:
        start_date = start_date.replace('-', '')
    if len(end_date) == 10:
        end_date = end_date.replace('-', '')

    # stock_zh_a_hist：返回单只股票的历史行情日频率数据， https://www.akshare.xyz/data/stock/stock.html#id20
    try:
        if 'zs' in symbol:
            data = ak.index_zh_a_hist(symbol=symbol[3:],
                                      period=period,
                                      start_date=start_date,
                                      end_date=end_date)
        else:
            data = ak.stock_zh_a_hist(symbol=symbol,
                                      period=period,
                                      start_date=start_date,
                                      end_date=end_date,
                                      adjust=adjust)
    except Exception as exc:
        print(f'request failed: unable to get {symbol} data: {exc}')
        return None

    data.columns = [
        'date', 'open', 'close', 'high', 'low', 'vol', 'amount', 'swing',
        'pct_chg', 'change', 'turn_over'
    ]
    data['date'] = pd.to_datetime(data['date'])
    data.set_index('date', inplace=True)

    return data


def fetch_hist_multi(
        symbol_list,
        period="daily",
        start_date=(datetime.datetime.now() -
                    datetime.timedelta(days=365 * 3)).strftime('%Y%m%d'),
        end_date=datetime.datetime.now().strftime('%Y%m%d'),
        adjust="qfq",
        to_loacl=True):
    """
    返回沪深京 A 股指定股票、指定周期和指定日期间的历史行情日频率数据
    Args:   symbol_list
                type:list e.g. ['600519', 'zs.000001', ...] where zs represents the index
            adjust: qfq, hfq, ''
            to_loacl: 是否保存到本地 路径为data_dump/stock-index-yyyymmdd-yyyymmdd.pkl
    Return: datas
            type:dict 
                keys: e.g. '600519' ,'zs.000001'; 
                values: daraframe
    """
    datas = {}
    if symbol_list == []:
        symbol_list = fetch_symbol(symbol='all')

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        future_to_stock = {
            executor.submit(fetch_hist,
                            symbol=symbol,
                            start_date=start_date,
                            end_date=end_date,
                            period=period,
                            adjust=adjust): symbol
            for symbol in symbol_list
        }  # 通过submit提交执行的函数到线程池中
        # as_completed:当子线程中的任务执行完后，直接用 result() 获取返回结果
        for future in concurrent.futures.as_completed(future_to_stock):
            symbol = future_to_stock[future]  # '代码'
            try:
                res = future.result()
                if res is not None:
                    res = res.astype({'vol': 'double'})
                    datas[symbol] = res
            except TypeError as exc:
                print(f'({symbol}) return None.  exception: {exc}')
            except Exception as exc:
                print(f'({symbol}) in future generated an exception: {exc}')

    if to_loacl:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        file_name = os.path.abspath(
            os.path.join(current_dir, os.pardir, 'data_dump',
                         'stock-index-' + start_date + '-' + end_date + '.pkl'))
        with open(file_name, 'wb') as f:
            pickle.dump(res, f)

    return datas


def fetch_symbol(symbol='all'):
    """
        返回(1)沪深京所有股票和指数的代码列表(2)股票或指数的名称
        Args:   symbol
                    type:str e.g. 'all' 'zs.000001' '600519' ...
                    symbol = all, return all stocks and index symbol list
                    symbol = zs.000001,etc, return stock or index name
        Return: res
                    type:list or str
    """
    all_stocks_df = ak.stock_zh_a_spot_em()
    all_indexs_df = ak.index_stock_info()
    if symbol == 'all':
        all_stocks_list = [x for x in all_stocks_df['代码'].values]
        all_indexs_list = [
            'zs.' + x for x in all_indexs_df['index_code'].values
        ]
        res = all_stocks_list + all_indexs_list
    else:
        if 'zs' in symbol:
            res = all_indexs_df[all_indexs_df['index_code'] ==
                                symbol[3:]]['display_name'].values[0]
        else:
            res = all_stocks_df[all_stocks_df['代码'] == symbol]['名称'].values[0]
    return res


def get_data_from_all(stock_code, all_data):
    for key in all_data:
        if stock_code in key:
            value = all_data[key]
            return value


def fetch_report(date):
    """
    返回沪深京所有股票某季度的业绩报表数据
    Args: date
        type:str choice of {"XXXX0331", "XXXX0630", "XXXX0930", "XXXX1231"}
    Return: report
        type:daraframe https://www.akshare.xyz/data/stock/stock.html#id136
    """

    try:
        report = ak.stock_yjbb_em(date=date)
    except TypeError as exc:
        logger.info('fetch_report at %s generated an exception: %s', date, exc)
        return None
    except Exception as exc:
        logger.info('fetch_report at %s generated an exception: %s', date, exc)
        return None

    date_choice = ["0331", "0630", "0930", "1231"]
    quarter = date_choice.index(date[-4:]) + 1
    report['year'] = date[:4]
    report['quarter'] = quarter
    report['date_choice'] = date

    return report


def fetch_report_multi(start_year, end_year, to_local=True):
    """
    返回沪深京所有股票季度业绩报表数据
    Args: start_year end_year 
            type:str e.g. '2019' '2020'
          to_local: 是否保存到本地 路径为data_dump/report-yyyymmdd-yyyymmdd.pkl
    Return: reports
        type:dataframe 
    """
    reports = pd.DataFrame()
    quarters = ["0331", "0630", "0930", "1231"]
    date_list = [
        str(i) + q for i in range(int(start_year), int(end_year)+1)
        for q in quarters
    ]
    print(date_list)

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        future_to_one_quarters = {
            executor.submit(fetch_report, date): date
            for date in date_list
        }

        for future in concurrent.futures.as_completed(future_to_one_quarters):
            date = future_to_one_quarters[future]
            try:
                data = future.result()
                if data is not None:
                    reports = pd.concat([reports, data], axis=0)
            except Exception as exc:
                logger.info(
                    'fetch_all_quarter_bb_from_ak at %s generated an exception: %s',
                    date, exc)
    if to_local:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        file_name = os.path.abspath(
            os.path.join(
                current_dir, os.pardir, 'data_dump',
                'reports-' + str(start_year) + '-' + str(end_year) + '.pkl'))

        with open(file_name, 'wb') as f:
            pickle.dump(reports, f)

    return reports


def get_hist(filename):
    """读取本地历史数据"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_name = os.path.abspath(
        os.path.join(current_dir, os.pardir, 'data_dump', filename+'.pkl'))
    with open(file_name, 'rb') as f:
        data = pickle.load(f)
    return data


if __name__ == "__main__":

    print('--------test fetch_hist--------')
    stock_data = fetch_hist('600519')
    index_data = fetch_hist('zs.000001')
    assert stock_data is not None
    assert index_data is not None

    print('--------test fetch_hist_multi--------')
    symbol_list = ['600519', '600020', 'zs.000001']
    res = fetch_hist_multi(symbol_list)
    for key, value in res.items():
        assert key in symbol_list
        assert value is not None

    print('--------test fetch_symbol--------')
    res1 = fetch_symbol(symbol='zs.000001')
    res2 = fetch_symbol(symbol='600519')
    res3 = fetch_symbol(symbol='all')
    assert res1 == '上证指数'
    assert res2 == '贵州茅台'
    assert len(res3) > 0

    print('--------test fetch_report--------')
    assert fetch_report("19890331") is None
    assert fetch_report("19990331") is not None
    # print(fetch_report("19990331"))
    # print(fetch_report("20000630"))
    # print(fetch_report("20050930"))
    # print(fetch_report("20191231"))

    print('--------test fetch_report_multi--------')
    # assert fetch_report_multi(start_year='2020', end_year='2023') is not None
    # all = fetch_report_multi(start_year='2015', end_year='2015')
    # print(all)

    print('--------test fetch_hist_all_to_loc--------')
    # fetch_hist_multi_to_loc()
