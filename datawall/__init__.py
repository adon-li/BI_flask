#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author  : kingsley kwong


from flask import Blueprint
from bi_flask.utils import *

datawall = Blueprint('datawall', __name__, url_prefix='/datawall')
module = '.'.join([__name__, __name__.split('.')[-1]])
init_import(os.path.dirname(os.path.abspath(__file__)), __name__)
