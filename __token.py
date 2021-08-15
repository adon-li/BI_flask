#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/06/01
# @Author  : kingsley kwong
# @Site    :
# @File    : __token.py
# @Software: BI 不漏 flask
# @Function:

from flask import request, jsonify, make_response
import requests
import logging
from bi_flask.user.model import BiDataAuthority
from sqlalchemy.exc import InvalidRequestError
from bi_flask._sessions import *
from bi_flask.settings import CHECK_TOKEN_URL
import traceback

Scope = sessions_scopes(sessions)
cache = get_cache()


def is_not_empty(s):
    return s and len(s.strip()) > 0


row2dict = lambda r: {c.name: str(getattr(r, c.name)) for c in r.__table__.columns}
logger = logging.getLogger('bi')

def __token_wrapper(func):
    def inner(*args, **kwargs):
        try:
            token = request.headers.get('Authorization')
            data = {'Authorization': token}
            r = requests.get(CHECK_TOKEN_URL, headers=data)
            context = r.json()
            if r.json()['success'] is True:
                user_id = context['data']['userId']
                cache_key = '_'.join([str(user_id), 'shop_ids'])
                if cache.has(cache_key):
                    shop_ids = cache.get(cache_key)
                    context['data']['online_shop_id_list_ori'] = shop_ids[0]
                    context['data']['offline_shop_id_list_ori'] = shop_ids[1]
                    context['data']['bi_province_id_list'] = shop_ids[2]
                    context['data']['bi_business_clazz_id_list'] = shop_ids[3]
                    context['data']['bi_big_zone_id_list'] = shop_ids[4]
                    context['data']['online_shop_id_list'] = shop_ids[5]
                    context['data']['offline_shop_id_list'] = shop_ids[6]
                    context['data']['offline_hmc_shop_id_list'] = shop_ids[7]
                else:
                    ret = Scope['bi_saas'].query(BiDataAuthority.shop_ids.label('online_shop_id_list')
                                          , BiDataAuthority.offline_shop_ids.label('offline_shop_id_list_ori'),
                                          BiDataAuthority.bi_province_ids.label('bi_province_id_list')
                                          , BiDataAuthority.bi_business_clazz_ids.label('bi_business_clazz_id_list'),
                                          BiDataAuthority.bi_big_zone_ids.label('bi_big_zone_id_list')).filter(
                        BiDataAuthority.account_role_id == user_id)
                    data = [dict(zip(val.keys(), val)) for val in ret]

                    if len(data) == 0:
                        context['data']['online_shop_id_list'] = []
                        context['data']['online_shop_id_list_ori'] = []
                        context['data']['offline_shop_id_list'] = []
                        context['data']['offline_hmc_shop_id_list'] = []
                        context['data']['offline_shop_id_list_ori'] = []
                        context['data']['bi_province_id_list'] = []
                        context['data']['bi_business_clazz_id_list'] = []
                        context['data']['bi_big_zone_id_list'] = []
                    else:

                        data = data[0]
                        cache_list = []
                        for key in data.keys():
                            if data[key] is not None:
                                context['data'][key] = list(filter(is_not_empty, data[key].split('|')))
                            else:
                                context['data'][key] = []
                            cache_list.append(context['data'][key])
                        if len(context['data']['bi_business_clazz_id_list']) > 0:
                            business_clazz_ids = ', '.join(context['data']['bi_business_clazz_id_list'])
                            s = Scope['bi_saas'].execute(
                                f''' select shop_id from bi_shops_new sh 
                                left join bi_business_info info ON sh.business_clazz_id=info.business_id 
                                WHERE sh.business_clazz_id IN ({business_clazz_ids}) and info.isshow=1 ''')
                            s = s.fetchall()
                            on_ids = list([str(val[0]) for val in s])
                            context['data']['online_shop_id_list'] = on_ids
                        else:
                            context['data']['online_shop_id_list'] = []
                        cache_list.append(context['data']['online_shop_id_list'])
                        if len(context['data']['bi_province_id_list']) > 0:
                            province_ids = ', '.join(context['data']['bi_province_id_list'])
                            s = Scope['bi_saas'].execute(
                                f''' select shop_id, e3_shop_id from bi_shops_new WHERE bi_province_id IN ({province_ids}) ''')
                            s = s.fetchall()
                            off_ids = list([str(val[0]) for val in s])
                            hmc_off_ids = list([str(val[1]) for val in s])
                            context['data']['offline_shop_id_list'] = off_ids
                            context['data']['offline_hmc_shop_id_list'] = hmc_off_ids
                        else:
                            context['data']['offline_shop_id_list'] = []
                            context['data']['offline_hmc_shop_id_list'] = []
                        cache_list.append(context['data']['offline_shop_id_list'])
                        cache_list.append(context['data']['offline_hmc_shop_id_list'])
                        cache.set(cache_key, cache_list, timeout=5 * 60)
                return func(context, *args, **kwargs)
            else:
                logger.error(context)
                if context['statusCode'] == -1:
                    context['statusCode'] = 500
                return jsonify(context)
        except InvalidRequestError:
            logger.error('数据库事务错误')
            context['message'] = "数据库事务错误，请联系管理员！"
            context['statusCode'] = -1
            context['success'] = False
            context['data'] = []
            Scope['bi_saas'].rollback()
            Scope['vertica'].rollback()
            Scope['hmc_product_center'].rollback()
            Scope['hmc_scm_product'].rollback()
            Scope['hmc_trade_center'].rollback()
            Scope['hmc_member_center'].rollback()
            Scope['hmc_oms_business'].rollback()
            return jsonify(context)
        except Exception as e:
            logger.error(traceback.format_exc(limit=2))
            context['message'] = "获取数据时出错，请联系管理员！"
            context['statusCode'] = -1
            context['success'] = False
            context['data'] = []
            return jsonify(context)
        finally:

            Scope['bi_saas'].remove()
            Scope['vertica'].remove()
            Scope['hmc_product_center'].remove()
            Scope['hmc_scm_product'].remove()
            Scope['hmc_trade_center'].remove()
            Scope['hmc_member_center'].remove()
            Scope['hmc_oms_business'].remove()

    return inner


def __token_download(func):
    def inner(*args, **kwargs):
        token = request.headers.get('Authorization') if request.headers.get('Authorization') is not None else request.args.get('Authorization')
        logger.info(token)
        data = {'Authorization': token}

        r = requests.get(CHECK_TOKEN_URL, headers=data)
        if r.json()['success'] is True:
            logger.info(r.text)
            context = r.json()
            # context['token'] = token
            # context['data'] = r.json()['data']
            return func(context, *args, **kwargs)
        else:
            logger.error(r.text)
            res = make_response('Error')
            res.set_cookie('auth', '')
            return jsonify(r.json())

    return inner


def __log_out(func):
    def inner(*args, **kwargs):
        try:
            token = request.headers.get('Authorization')
            logger.info(token)
            data = {'Authorization': token}
            r = requests.get(CHECK_TOKEN_URL, headers=data)
            context = r.json()
            if r.json()['success'] is True:
                logger.info(r.text)
                user_id = context['data']['userId']
                key = '_'.join([str(user_id), 'shop_ids'])
                if cache.has(key):
                    cache.delete(key)
                return func(context, *args, **kwargs)
            else:
                logger.error(context)
                if context['statusCode'] == -1:
                    context['statusCode'] = 500
                return jsonify(context)
        except Exception as e:
            logger.error(traceback.format_exc(limit=2))
            context['message'] = "获取数据时出错，请联系管理员！"
            context['statusCode'] = -1
            context['success'] = False
            context['data'] = []
            return jsonify(context)
        finally:
            Scope['bi_saas'].remove()
            Scope['vertica'].remove()
            Scope['hmc_product_center'].remove()
            Scope['hmc_scm_product'].remove()
            Scope['hmc_trade_center'].remove()
            Scope['hmc_member_center'].remove()
            Scope['hmc_oms_business'].remove()

    return inner
