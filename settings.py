# -*- coding: utf-8 -*-
'''
@File    :   settings.py
@Time    :   2023/04/03 16:17:13
@Author  :   xoyojo 
@Version :   1.0
@Site    :   https://
@Desc    :   None
'''
import os

config = {
    'root_dir': os.path.dirname(os.path.abspath(__file__)),
    'data_dir': os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             'data')
}

import sys

sys.path.append('..')

# def config():
#     return config

if __name__ == '__main__':
    print(config['root_dir'])
    print(config['data_dir'])