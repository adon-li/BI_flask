#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/05/02
# @Author  : kingsley kwong
# @Site    :
# @File    : shop\api.py.bak.bai
# @Software: BI 不漏 flask
# @Function:

from flask import Blueprint
from bi_flask.utils import *

datawall = Blueprint('datawall', __name__, url_prefix='/datawall')
module = '.'.join([__name__, __name__.split('.')[-1]])
init_import(os.path.dirname(os.path.abspath(__file__)), __name__)
