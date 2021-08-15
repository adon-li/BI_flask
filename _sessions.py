#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/06/01
# @Author  : kingsley kwong
# @Site    : 数据源注册模块
# @File    : _session.py
# @Software: BI 不漏 flask
# @Function:


from bi_flask.get_engine import get_engine, get_ssh_engine
from bi_flask.settings import DATA_SOURCE, SYSTEM_CODE, SSH_SOURCE, REDIS_SOURCE, REDIS_CACHE
from sqlalchemy.orm import sessionmaker, scoped_session
from werkzeug.contrib.cache import RedisCache,SimpleCache


def sessions_scopes(_sessions):
    scopes = {}
    for key, _session in _sessions.items():
        scopes.update({key: scoped_session(_session)})
    return scopes


def get_cache():
    if REDIS_CACHE is not True:
        cache = SimpleCache()
        # redis_source = REDIS_SOURCE['local_redis']
        # cache = RedisCache(host=redis_source['host'], password=redis_source['password'], db=redis_source['db'])
    else:
        redis_source = REDIS_SOURCE['bi_redis']
        cache = RedisCache(host=redis_source['host'], password=redis_source['password'], db=redis_source['db'])
    return cache


sessions = {}

for source in DATA_SOURCE.keys():
    if SYSTEM_CODE == 'Linux':
        sessions.update({source: sessionmaker(bind=get_engine(DATA_SOURCE[source]))})
    else:
        if 'ssh' in DATA_SOURCE[source].keys():
            sessions.update({source: sessionmaker(bind=get_ssh_engine(ssh_source=SSH_SOURCE[DATA_SOURCE[source]['ssh']],
                                             data_source=DATA_SOURCE[source]))})
        else:
            sessions.update({source: sessionmaker(bind=get_engine(DATA_SOURCE[source]))})
