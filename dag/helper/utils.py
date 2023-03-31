# References: https://github.com/BogdanCojocar/medium-articles/blob/master/pandas_validation/validation.py

import pandas_schema
import numpy as np
from pandas_schema import Column
from pandas_schema.validation import CustomElementValidation
from dateutil.parser import parse
from decimal import Decimal, InvalidOperation
from datetime import datetime


def check_decimal(dec):
    try:
        Decimal(dec)
    except InvalidOperation:
        return False
    return True


def check_int(num):
    try:
        int(num)
    except ValueError:
        return False
    return True


def check_date(date_):
    try:
        if str(date_) != "nan":
            datetime.strptime(date_, "%Y-%m-%d")
    except ValueError:
        return False
    return True


def check_timeformat(time_):
    try:
        if str(time_) != "nan":
            datetime.strptime(time_, "%H:%M:%S")
    except ValueError:
        return False
    return True
