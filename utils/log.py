import logging

logger = logging.getLogger('xoyo_stock_tools')
logger.setLevel(logging.DEBUG)

hdr = logging.StreamHandler()
logger.addHandler(hdr)

formatter = logging.Formatter(
    '[%(asctime)s] %(name)s:%(levelname)s: %(message)s')
hdr.setFormatter(formatter)