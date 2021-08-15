from flask import Blueprint
import logging
from bi_flask.utils import init_import
import os

logger = logging.getLogger('bi')
stock = Blueprint('stock', __name__, url_prefix='/stock')

module = '.'.join([__name__, __name__.split('.')[-1]])
init_import(os.path.dirname(os.path.abspath(__file__)), __name__)


