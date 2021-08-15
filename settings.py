#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Author: Kingsley Kwong
# @File : settings.py

import os
import platform
import logging.config
import socket

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_FILE_DIR = '/data/'
SYSTEM_CODE = platform.system()
LABEL_ALLOWED_EXTENSIONS = ['xls', 'xlsx']
IP_ADDR = socket.gethostbyname(socket.gethostname())
TESTING = False  # 开启测试模式

PARAMETERS = {
    'local': {
        'upload_folder': 'C:\Inman_product_label_uploads',
        'log_dir': '/'.join([BASE_DIR, '/log']),
        'check_token_url': 'http://192.168.7.232:3000/mock/47/checkToken',
        'get_menu_url': 'http://192.168.7.232:3000/mock/47/employee/loginInfo/get',
        'download_file_path':  '//'.join([BASE_DIR, 'download']),
        'used_file_path': '//'.join([BASE_DIR, 'used']),
        'err_file_path': '//'.join([BASE_DIR, 'err']),
        'gen_file_path': '//'.join([BASE_DIR, 'export_file']),
        'redis_cache': False,
        'airflow_api': 'http://bi.hmcloud.com.cn/airflow/api/',
    },
    'develop': {
        'upload_folder': 'C:\Inman_product_label_uploads',
        'log_dir': '/'.join([BASE_DIR, '/log']),
        'check_token_url': 'http://192.168.28.8:8084/hmc-upms/bi/checkToken',
        'get_menu_url': 'http://192.168.28.8:8084/hmc-upms/bi/getMenu',
        'download_file_path':  '//'.join([BASE_DIR, 'download']),
        'used_file_path': '//'.join([BASE_DIR, 'used']),
        'err_file_path': '//'.join([BASE_DIR, 'err']),
        'gen_file_path': 'E:\huge_xlsx_writer',
        'redis_cache': False,
        'airflow_api': 'http://bi.hmcloud.com.cn/airflow/api/',
    },
    'release': {
        'upload_folder': '/'.join([BASE_FILE_DIR, '/Inman_product_label_uploads']),
        'log_dir': '/'.join([BASE_FILE_DIR, '/bi', '/log']),
        'check_token_url': 'http://hmc.hmcloud.com.cn/hmc-upms/bi/checkToken',
        'get_menu_url': 'http://hmc.hmcloud.com.cn/hmc-upms/bi/getMenu',
        'download_file_path': '//'.join([BASE_FILE_DIR, '/bi', 'download']),
        'used_file_path':  '//'.join([BASE_FILE_DIR, '/bi', 'used']),
        'err_file_path': '//'.join([BASE_FILE_DIR, '/bi', 'err']),
        'gen_file_path': '/'.join([BASE_FILE_DIR, '/inman_stock_sell']),
        'redis_cache': True,
        'airflow_api': 'http://bi.hmcloud.com.cn/airflow/api/',
    },
    'test': {
        'upload_folder': '/'.join([BASE_FILE_DIR, '/Inman_product_label_uploads']),
        'log_dir': '/'.join([BASE_FILE_DIR, '/bi', '/log']),
        'check_token_url': 'http://uat-backend.hmcloud.com.cn/hmc-upms/bi/checkToken',
        'get_menu_url': 'http://uat-backend.hmcloud.com.cn/hmc-upms/bi/getMenu',
        'download_file_path': '//'.join([BASE_FILE_DIR, '/bi', 'download']),
        'used_file_path':  '//'.join([BASE_FILE_DIR, '/bi', 'used']),
        'err_file_path': '//'.join([BASE_FILE_DIR, '/bi', 'err']),
        'gen_file_path': '/'.join([BASE_FILE_DIR, '/inman_stock_sell']),
        'redis_cache': False,
        'airflow_api': 'http://sand-airflow.hmcloud.com.cn/airflow/api/',
    },
}

if TESTING:
    from bi_flask.test_db_setting import DATA_SOURCE, SSH_SOURCE, SSH_O2O_SOURCE, REDIS_SOURCE
    params = PARAMETERS['test']
else:
    from bi_flask.db_settings import DATA_SOURCE, SSH_SOURCE, SSH_O2O_SOURCE, REDIS_SOURCE
    if SYSTEM_CODE == 'Windows':
        if IP_ADDR == '192.168.7.146' or IP_ADDR == '192.168.7.154' or IP_ADDR == '192.168.7.22':
            params = PARAMETERS['local']
        else:
            params = PARAMETERS['develop']
    else:
        params = PARAMETERS['release']

UPLOAD_FOLDER = params['upload_folder']
LOG_DIR = params['log_dir']
CHECK_TOKEN_URL = params['check_token_url']
GET_MENU_URL = params['get_menu_url']
DOWNLOAD_FILE_PATH = params['download_file_path']
USED_FILE_PATH = params['used_file_path']
ERR_FILE_PATH = params['err_file_path']
GEN_FILE_PATH = params['gen_file_path']
REDIS_CACHE = params['redis_cache']
AIRFLOW_API = params['airflow_api']

LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(threadName)s:%(thread)d] [%(name)s:%(lineno)d] [%(module)s:%(funcName)s] [%(levelname)s]- %(message)s'}
        # 日志格式
    },
    'filters': {
    },
    'handlers': {
        # 'default': {
        #     'level':'DEBUG',
        #     'class':'logging.handlers.RotatingFileHandler',
        #     'filename': '//'.join([LOG_DIR, 'default.log']),     #日志输出文件
        #     'maxBytes': 1024*1024*5,                  #文件大小
        #     'backupCount': 5,                         #备份份数
        #     'formatter':'standard',                   #使用哪种formatters日志格式
        # },
        'error': {
            'level': 'ERROR',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': '//'.join([LOG_DIR, 'error.log']),
            'maxBytes': 1024 * 1024 * 5,
            'backupCount': 5,
            'formatter': 'standard',
        },
        'info': {
            'level': 'INFO',
            'class': 'logging.handlers.RotatingFileHandler',  # 按文件大小
            'filename': '//'.join([LOG_DIR, 'info.log']),
            'maxBytes': 1024 * 1024 * 5,
            'backupCount': 5,
            'formatter': 'standard',
        },
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'standard'
        }
    },
    'loggers': {
        'bi': {'handlers': ['console', 'error', 'info'],
               'level': 'INFO',
               'propagate': True}
    }
}
