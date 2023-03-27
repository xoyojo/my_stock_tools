import akshare as ak
import logging
import talib as tl
import concurrent.futures
import datetime


def fetch(stock):
    """
    返回某只股票、指定周期和指定日期间的历史行情日频率数据
    Args: stock
        type:tunple ( (stock_code,stock_name), period, start_date, end_date, adjust)
    Return: stock_data
        type:daraframe
    """


    # stock_zh_a_hist：返回单只股票的历史行情日频率数据， https://www.akshare.xyz/data/stock/stock.html#id20
    try:
        data = ak.stock_zh_a_hist(symbol=stock[0][0], period=stock[1], start_date=stock[2], end_date=stock[3],adjust=stock[4])
    except  Exception as exc:
        print('%s(%r) fetch data generated an exception: %s' % (stock[0][1], stock[0][0], exc))

    if data is None or data.empty:
        logging.debug("股票："+stock+" 没有数据，略过...")
        return

    data['p_change'] = tl.ROC(data['收盘'], 1)

    return data

def run(stocks,period="daily",start_date="20200201",end_date=datetime.datetime.now().strftime('%Y%m%d'),adjust="qfq"):
    """
    返回沪深京 A 股指定股票、指定周期和指定日期间的历史行情日频率数据
    Args: stocks 
        type:list [('代码', '名称')]
    Return: stocks_data
        type:dict keys:('代码', '名称'); values: daraframe
    """

    stocks_data = {}


    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        future_to_stock = {executor.submit(fetch, (stock,period,start_date,end_date,adjust)): stock for stock in stocks} # 通过submit提交执行的函数到线程池中
        # as_completed:当子线程中的任务执行完后，直接用 result() 获取返回结果
        for future in concurrent.futures.as_completed(future_to_stock):
            stock = future_to_stock[future] # stock: tunple ('代码', '名称')
            try:
                data = future.result()
                if data is not None:
                    data = data.astype({'成交量': 'double'})
                    stocks_data[stock] = data
            except Exception as exc:
                print('%s(%r) generated an exception: %s' % (stock[1], stock[0], exc))

    
    
    return stocks_data

stocks =  [('600519', '名称1'),('600172', '名称2')]
res = run(stocks,start_date="20200201",end_date="20200228")
# print(res)