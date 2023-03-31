# -*- coding: utf-8 -*-
'''
@File    :   chart_componets.py
@Time    :   2023/03/31 17:05:48
@Author  :   xoyojo 
@Version :   1.0
@Site    :   https://
@Desc    :   None
'''

from pyecharts import options as opts
from pyecharts.charts import Kline, Line, Grid, Bar, Scatter, Page
from log import logger


def kline(df, date_name='date', axis_index=0):
    # 将数据转换为列表格式
    data = df[['open', 'close', 'low', 'high']].values.tolist()
    dates = df[date_name].tolist()  # date type:str, like '2021-07-01'

    # 绘制 K 线图
    kline = (
        Kline(init_opts=opts.InitOpts(
            bg_color="#000000")).add_xaxis(dates).add_yaxis(
                series_name="K线图",
                y_axis=data,
                xaxis_index=axis_index,
                yaxis_index=axis_index,
                itemstyle_opts=opts.ItemStyleOpts(
                    color="#ec0000",
                    color0="#00da3c",
                    border_color="#8A0000",
                    border_color0="#008F28",
                ),
            ).set_global_opts(
                title_opts=opts.TitleOpts(title="双品种组合"),
                xaxis_opts=opts.AxisOpts(
                    # type_='category',
                    is_scale=True,
                    splitline_opts=opts.SplitLineOpts(is_show=False)),
                yaxis_opts=opts.AxisOpts(
                    is_scale=True,
                    splitline_opts=opts.SplitLineOpts(is_show=False)),
                legend_opts=opts.LegendOpts(
                    is_show=True,
                    textstyle_opts=opts.TextStyleOpts(color='#DCDCDC'))).
        set_series_opts(
            # label_opts=opts.LabelOpts(is_show=False),  # 去掉line上的标签(数值)
            label_opts=opts.LabelOpts(is_show=False)))

    # 绘制均线图
    # 计算均线数据
    ma5 = df['close'].rolling(window=5).mean().tolist()
    ma10 = df['close'].rolling(window=10).mean().tolist()
    ma20 = df['close'].rolling(window=20).mean().tolist()

    line = (
        Line().set_series_opts(
            label_opts=opts.LabelOpts(is_show=False),  # 去掉line上的标签(数值)
        ).add_xaxis(dates).add_yaxis("MA5", ma5,
                                     symbol='none')  # symbol='none'去掉line上的圆点
        .add_yaxis("MA10", ma10, symbol='none').add_yaxis("MA20",
                                                          ma20,
                                                          symbol='none'))

    # 将 K 线图和均线图组合在一起
    k_ma = kline.overlap(line)
    return k_ma