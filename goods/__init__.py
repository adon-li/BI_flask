#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author  : kingsley kwong
# @Site    :
# @File    : goods\api.py.bak

from flask import Blueprint
from bi_flask.utils import *


good = Blueprint('goods', __name__, url_prefix='/goods')
module = '.'.join([__name__, 'good'])
init_import(os.path.dirname(os.path.abspath(__file__)), __name__)
