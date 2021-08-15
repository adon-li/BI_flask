#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/05/09
# @Author  : kingsley kwong
# @File    : get_engine.py


from sqlalchemy import create_engine
import sys
from sshtunnel import SSHTunnelForwarder
import logging

logger = logging.getLogger('bi')


class BaseException(Exception):
    def __init__(self, err=''):
        err_msg = "%s at Line %d:%s in %s" % (self.__class__.__name__, sys.exc_info()[2].tb_lineno,
                                              err,
                                              sys.exc_info()[2].tb_frame.f_code.co_filename)
        Exception.__init__(self, err_msg)


class DataBaseConnectError(BaseException): pass


class DataBaseExecuteError(Exception): pass


def get_engine(data_source):
    try:
        if data_source['engine'] == 'vertica':
            return create_engine('vertica+vertica_python://{user}:{passwd}@{host}:{port}/{db}'
                                 .format(user=data_source['user'],
                                         passwd=data_source['passwd'],
                                         host=data_source['host'],
                                         port=data_source['port'],
                                         db=data_source['db']), pool_recycle=20, pool_pre_ping=True,
                                 max_overflow=10, pool_size=10)

        elif data_source['engine'] == 'mysql':
            dsn = 'mysql+pymysql://{user}:{passwd}@{host}:{port}/{db}?charset=utf8'.format(user=data_source['user'],
                                                                                           passwd=data_source['passwd'],
                                                                                           host=data_source['host'],
                                                                                           port=str(data_source['port']),
                                                                                           db=data_source['db'])
            engine = create_engine(dsn, pool_recycle=60, pool_pre_ping=True, max_overflow=30, pool_size=30)
            return engine
    except Exception as e:
        raise DataBaseConnectError('executing function "%s.engine" caught %s' % (__name__, e))


def get_ssh_engine(ssh_source, data_source):
    try:
        if data_source['engine'] == 'mysql':
            server = SSHTunnelForwarder((ssh_source['ssh_host'], ssh_source['ssh_port']),
                                        ssh_username=ssh_source['ssh_user'],
                                        ssh_password=ssh_source['ssh_password'],
                                        remote_bind_address=(data_source['host'], data_source['port']))
            server.start()
            local_port = str(server.local_bind_port)
            engine = create_engine('mysql+pymysql://{user}:{passwd}@{host}:{port}/{db}?charset=utf8'
                                   .format(user=data_source['user'],
                                           passwd=data_source['passwd'],
                                           host='127.0.0.1',
                                           port=local_port,
                                           db=data_source['db']), pool_recycle=60, pool_pre_ping=True, max_overflow=30)
            return engine
        else:
            raise DataBaseConnectError('could not connect the data_source with ssh!')
    except Exception as e:
        raise DataBaseConnectError('executing function "%s.engine" caught %s' % (__name__, e))
