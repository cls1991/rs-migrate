# coding: utf8

import json
import time


def time_now_str(format_str="%Y-%m-%d"):
    """
    获取当天时间
    :param format_str:
    :return:
    """
    return time.strftime(format_str, time.localtime())


def datetime_to_str(dt, format_str="%Y-%m-%d"):
    """
    datetime转换字符串
    :param format_str:
    :param dt:
    :return:
    """
    return dt.strftime(format_str)
