#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author  : kingsley kwong
# @Site    :
# @File    : goods\api.py.bak

from flask import jsonify, Blueprint, request, redirect
import requests, json
from bi_flask.__token import __token_wrapper, __token_download
from sqlalchemy.orm import aliased
from sqlalchemy import desc, asc, and_, or_, cast
from sqlalchemy.sql import func
from .model import *
import logging
from bi_flask._sessions import sessions, sessions_scopes
from bi_flask.utils import *
from bi_flask import excel
from werkzeug.utils import secure_filename
from bi_flask.settings import UPLOAD_FOLDER
import dateutil
from . import good
import os
import importlib

logger = logging.getLogger('bi')
Scope = sessions_scopes(sessions)

# good = Blueprint('goods', __name__, url_prefix='/goods')
# __mod_name__ = 'bi_flask.goods'


# def init_import(path, model=None):
#     for mod in os.listdir(path):
#         if os.path.isfile(os.path.join(path, mod)):
#             if not mod.startswith('_') and not mod == 'api.py' and not mod == 'model.py':
#                 if model:
#                     importlib.import_module('.'.join([__mod_name__,model, mod.split('.')[0]]))
#                 else:
#                     importlib.import_module('.'.join([__mod_name__, mod.split('.')[0]]))
#         else:
#             if not mod.startswith('_') and not mod == 'api.py' and not mod == 'model.py':
#                 init_import(os.path.join(path, mod), mod)
#
#
# init_import(os.path.dirname(os.path.abspath(__file__)), None)


@good.route('/shop_list', methods=['GET'], endpoint='shop_list')
@__token_wrapper
def shop_list(context):
    '''
    获取店铺列表
    :param context:
    :return:
    '''
    try:
        business_unit_id = context['data']['businessUnitId']
        shop_id = context['data']['online_shop_id_list'] + context['data']['offline_shop_id_list']
        if len(shop_id) == 0:
            context['data'] = []
            return jsonify(context)
        data = ({'bi_shop_id': 1500014, 'shopName': '茵曼旗舰店', 'shopId': 57301243},
                {'bi_shop_id': 1750007, 'shopName': '茵曼童装旗舰店', 'shopId': 142198236},
                {'bi_shop_id': 1500007, 'shopName': '生活在左旗舰店', 'shopId': 108231462},
                {'bi_shop_id': 2000003, 'shopName': '达丽坊服饰旗舰店', 'shopId': 66984172},
                {'bi_shop_id': 1500013, 'shopName': 'pass旗舰店', 'shopId': 112736202},
                {'bi_shop_id': 2000010, 'shopName': 'samyama旗舰店', 'shopId': 115394119},
                {'bi_shop_id': 2000022, 'shopName': '水生之城', 'shopId': 60553320},
                {'bi_shop_id': 2000015, 'shopName': '初语旗舰店', 'shopId': 68905897},
                {'bi_shop_id': 1500006, 'shopName': '秋壳旗舰店', 'shopId': 62560090},
                {'bi_shop_id': 1500012, 'shopName': '初语童装旗舰店', 'shopId': 126813856},
                {'bi_shop_id': 2000004, 'shopName': '茵曼家具旗舰店', 'shopId': 136852150},
                {'bi_shop_id': 1500005, 'shopName': '茵曼箱包旗舰店', 'shopId': 108443046},
                {'bi_shop_id': 2000006, 'shopName': '华谊家具天猫旗舰店', 'shopId': 106444345},
                {'bi_shop_id': 1750001, 'shopName': '茵曼女鞋旗舰店', 'shopId': 107160445},
                {'bi_shop_id': 340250001, 'shopName': '天猫初语服饰outlets店', 'shopId': 311958254})

        shop_datas = []
        for shop_data in data:
            if str(shop_data['bi_shop_id']) in shop_id:
                shop_datas.append(shop_data)

        context['data'] = shop_datas
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['vertica'].remove()
        Scope['bi_saas'].remove()


@good.route('/get_dates', methods=['GET'], endpoint='get_dates')
@__token_wrapper
def get_dates(context):
    '''
    获取商品起止日期
    :param context:
    :return:
    '''
    try:
        # shop_id = request.args.get('shopId')
        # if shop_id is None:
        #     raise TypeError('parameter shop_id is NoneType')
        # max_date = Scope['vertica'].query(distinct(func.max(TbSycmShopAuctionDetail.st_date))) \
        #     .filter(TbSycmShopAuctionDetail.shop_id == shop_id).first()
        # min_date = Scope['vertica'].query(distinct(func.min(TbSycmShopAuctionDetail.st_date))) \
        #     .filter(TbSycmShopAuctionDetail.shop_id == shop_id).first()
        ret = Scope['vertica'].execute(
            """SELECT MAX(st_date) as endDate, MIN(st_date) as endDate FROM hmcdata.tb_sycm_shop_auction_detail""").fetchall()
        context['data'] = {"startDate": ret[0][1].strftime('%Y-%m-%d'), "endDate": ret[0][0].strftime('%Y-%m-%d')}

        # context['data'] = {"startDate": min_date[0].strftime('%Y-%m-%d'), "endDate": max_date[0].strftime('%Y-%m-%d')}
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_total', methods=['GET'], endpoint='get_total')
@__token_wrapper
def get_total(context):
    '''
    获取商品整体明细
    :param context:
    :return:
    '''
    try:
        shop_id = request.args.get('shopId')
        data_range = request.args.get('dateRange')
        code = request.args.get('goodsCode')
        if shop_id is None:
            raise TypeError('parameter shopId is NoneType')
        if data_range is None:
            raise TypeError('parameter dateRange is NoneType')
        if code is None:
            raise TypeError('parameter code is NoneType')
        ret = Scope['vertica'].query(TbSycmShopAuctionDetail.auction_id.label('款号'),
                                     TbSycmShopAuctionDetail.auction_title.label('商品名称'),
                                     TbSycmShopAuctionDetail.auction_url.label('商品链接'),
                                     func.sum(TbSycmShopAuctionDetail.uv).label('访客数'),
                                     func.sum(TbSycmShopAuctionDetail.alipay_trade_amt).label('支付金额'),
                                     func.avg(TbSycmShopAuctionDetail.avg_amt_per_order).label('平均客单价'),
                                     func.sum(TbSycmShopAuctionDetail.collect_goods_buyer_cnt).label('收藏人数'),
                                     func.sum(TbSycmShopAuctionDetail.alipay_auction_cnt).label('支付商品件数'),
                                     func.sum(TbSycmShopAuctionDetail.add_cart_auction_cnt).label('加购件数'),
                                     func.avg(TbSycmShopAuctionDetail.pay_order_cvr).label('平均支付转化率'),
                                     func.avg(TbSycmShopAuctionDetail.avg_uv_value).label('访客平均价值')) \
            .filter(TbSycmShopAuctionDetail.shop_id == shop_id) \
            .filter(and_(TbSycmShopAuctionDetail.st_date <= data_range.split('|')[1],
                         TbSycmShopAuctionDetail.st_date >= data_range.split('|')[0])) \
            .group_by(TbSycmShopAuctionDetail.auction_id, TbSycmShopAuctionDetail.auction_title,
                      TbSycmShopAuctionDetail.auction_url)
        ret = ret.filter(or_(TbSycmShopAuctionDetail.auction_id.like('%%%s%%' % code),
                             TbSycmShopAuctionDetail.auction_title.like('%%%s%%' % code)))
        cnt = ret.count()
        context['data'] = {'total': cnt}
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/goods_detail', methods=['GET'], endpoint='goods_detail')
@__token_wrapper
def goods_detail(context):
    '''
    获取商品整体明细
    :param context:
    :return:
    '''
    try:
        shop_id = request.args.get('shopId')
        data_range = request.args.get('dateRange')
        sort_field = request.args.get('sortField')
        order = request.args.get('order')
        page_size = request.args.get('pageSize')
        page = request.args.get('page')
        code = request.args.get('goodsCode')
        category = request.args.get('category')
        product_year = request.args.get('product_year')
        product_season = request.args.get('product_season')

        # if category == 'all' or category is None:
        #     category = ''
        # print(category=='')
        if shop_id is None:
            raise TypeError('parameter shopId is NoneType')
        if data_range is None:
            raise TypeError('parameter dataRange is NoneType')
        if sort_field is None:
            raise TypeError('parameter sortField is NoneType')
        if order is None:
            raise TypeError('parameter order is NoneType')
        if page_size is None:
            raise TypeError('parameter pageSize is NoneType')
        if page is None:
            raise TypeError('parameter page is NoneType')
        if code is None:
            raise TypeError('parameter code is NoneType')
        ret = Scope['vertica'].query(TbSycmShopAuctionDetail.auction_id.label('淘宝ID'),
                                     TbSycmShopAuctionDetail.outer_goods_id.label('款号'),
                                     func.max(TbSycmShopAuctionDetail.auction_title).label('商品名称'),
                                     TbSycmShopAuctionDetail.auction_url.label('商品链接'),
                                     func.sum(TbSycmShopAuctionDetail.uv).label('访客数'),
                                     func.sum(TbSycmShopAuctionDetail.alipay_trade_amt).label('支付金额'),
                                     func.avg(TbSycmShopAuctionDetail.avg_amt_per_order).label('平均客单价'),
                                     func.sum(TbSycmShopAuctionDetail.collect_goods_buyer_cnt).label('收藏人数'),
                                     func.sum(TbSycmShopAuctionDetail.alipay_auction_cnt).label('支付商品件数'),
                                     func.sum(TbSycmShopAuctionDetail.add_cart_auction_cnt).label('加购件数'),
                                     func.avg(TbSycmShopAuctionDetail.pay_order_cvr).label('平均支付转化率'),
                                     func.avg(TbSycmShopAuctionDetail.avg_uv_value).label('访客平均价值'),
                                     func.sum(TbSycmShopAuctionDetail.selling_after_sales_succ_refund_amt).label(
                                         'selling_after_sales_succ_refund_amt'),
                                     func.sum(TbSycmShopAuctionDetail.selling_after_sales_succ_refund_cnt).label(
                                         'selling_after_sales_succ_refund_cnt'),
                                     (func.sum(TbSycmShopAuctionDetail.selling_after_sales_succ_refund_amt)
                                      / func.sum(TbSycmShopAuctionDetail.alipay_trade_amt))
                                     .label('selling_after_sales_succ_refund_amt_rate'),
                                     IomScmProductList.product_category2) \
            .outerjoin(IomScmProductList, IomScmProductList.product_sn == TbSycmShopAuctionDetail.outer_goods_id) \
            .filter(TbSycmShopAuctionDetail.shop_id == shop_id) \
            .filter(and_(TbSycmShopAuctionDetail.st_date <= data_range.split('|')[1],
                         TbSycmShopAuctionDetail.st_date >= data_range.split('|')[0])) \
            .group_by(TbSycmShopAuctionDetail.auction_id  # , TbSycmShopAuctionDetail.auction_title
                      , TbSycmShopAuctionDetail.outer_goods_id, TbSycmShopAuctionDetail.auction_url,
                      IomScmProductList.product_category2)
        ret = ret.filter(or_(TbSycmShopAuctionDetail.auction_id.like('%%%s%%' % code),
                             TbSycmShopAuctionDetail.auction_title.like('%%%s%%' % code)))
        if category != 'all' and category is not None:
            ret = ret.filter(IomScmProductList.product_category2 == category)
        if product_year != 'all' and product_year is not None:
            ret = ret.filter(IomScmProductList.product_year == product_year)
        if product_season != 'all' and product_season is not None:
            ret = ret.filter(IomScmProductList.product_season == product_season)
        cnt = ret.count()

        if order == 'descend':
            ret = Scope['vertica'].query(TbSycmShopAuctionDetail.auction_id,
                                         TbSycmShopAuctionDetail.outer_goods_id,
                                         func.max(TbSycmShopAuctionDetail.auction_title).label('auction_title'),
                                         TbSycmShopAuctionDetail.auction_url,
                                         func.sum(TbSycmShopAuctionDetail.uv).label('uv'),
                                         func.sum(TbSycmShopAuctionDetail.alipay_trade_amt).label('alipay_trade_amt'),
                                         func.avg(TbSycmShopAuctionDetail.avg_amt_per_order).label('avg_amt_per_order'),
                                         func.sum(TbSycmShopAuctionDetail.collect_goods_buyer_cnt).label(
                                             'collect_goods_buyer_cnt'),
                                         func.sum(TbSycmShopAuctionDetail.alipay_auction_cnt).label(
                                             'alipay_auction_cnt'),
                                         (func.sum(TbSycmShopAuctionDetail.alipay_trade_amt) /
                                          func.nullif(func.sum(TbSycmShopAuctionDetail.alipay_auction_cnt), 0)).label(
                                             'per_auction_price'),
                                         func.sum(TbSycmShopAuctionDetail.add_cart_auction_cnt).label(
                                             'add_cart_auction_cnt'),
                                         func.avg(TbSycmShopAuctionDetail.pay_order_cvr).label('pay_order_cvr'),
                                         func.avg(TbSycmShopAuctionDetail.avg_uv_value).label('avg_uv_value'),
                                         func.sum(TbSycmShopAuctionDetail.selling_after_sales_succ_refund_amt).label(
                                             'selling_after_sales_succ_refund_amt'),
                                         func.sum(TbSycmShopAuctionDetail.selling_after_sales_succ_refund_cnt).label(
                                             'selling_after_sales_succ_refund_cnt'),
                                         (func.sum(TbSycmShopAuctionDetail.selling_after_sales_succ_refund_amt)
                                          / func.nullif(func.sum(TbSycmShopAuctionDetail.alipay_trade_amt), 0))
                                         .label('selling_after_sales_succ_refund_amt_rate'),
                                         func.max(TbSycmShopAuctionDetail.pic_path).label('pic_path'),
                                         IomScmProductList.product_category2) \
                .outerjoin(IomScmProductList, IomScmProductList.product_sn == TbSycmShopAuctionDetail.outer_goods_id) \
                .filter(TbSycmShopAuctionDetail.shop_id == shop_id) \
                .filter(and_(TbSycmShopAuctionDetail.st_date <= data_range.split('|')[1],
                             TbSycmShopAuctionDetail.st_date >= data_range.split('|')[0])) \
                .group_by(TbSycmShopAuctionDetail.auction_id  # , TbSycmShopAuctionDetail.auction_title
                          , TbSycmShopAuctionDetail.outer_goods_id,
                          TbSycmShopAuctionDetail.auction_url, IomScmProductList.product_category2
                          # , TbSycmShopAuctionDetail.pic_path
                          )
            ret = ret.filter(or_(TbSycmShopAuctionDetail.auction_id.like('%%%s%%' % code),
                                 TbSycmShopAuctionDetail.outer_goods_id.like('%%%s%%' % code)))
            if category != 'all' and category is not None:
                ret = ret.filter(IomScmProductList.product_category2 == category)
            if product_year != 'all' and product_year is not None:
                ret = ret.filter(IomScmProductList.product_year == product_year)
            if product_season != 'all' and product_season is not None:
                ret = ret.filter(IomScmProductList.product_season == product_season)
            ret = ret.order_by(desc(sort_field))
            ret = ret.slice((int(page) - 1) * int(page_size), int(page) * int(page_size))
        else:
            ret = Scope['vertica'].query(TbSycmShopAuctionDetail.auction_id,
                                         TbSycmShopAuctionDetail.outer_goods_id,
                                         func.max(TbSycmShopAuctionDetail.auction_title).label('auction_title'),
                                         TbSycmShopAuctionDetail.auction_url,
                                         func.sum(TbSycmShopAuctionDetail.uv).label('uv'),
                                         func.sum(TbSycmShopAuctionDetail.alipay_trade_amt).label('alipay_trade_amt'),
                                         func.avg(TbSycmShopAuctionDetail.avg_amt_per_order).label('avg_amt_per_order'),
                                         func.sum(TbSycmShopAuctionDetail.collect_goods_buyer_cnt).label(
                                             'collect_goods_buyer_cnt'),
                                         func.sum(TbSycmShopAuctionDetail.alipay_auction_cnt).label(
                                             'alipay_auction_cnt'),
                                         (func.sum(TbSycmShopAuctionDetail.alipay_trade_amt) /
                                          func.nullif(func.sum(TbSycmShopAuctionDetail.alipay_auction_cnt), 0)).label(
                                             'per_auction_price'),
                                         func.sum(TbSycmShopAuctionDetail.add_cart_auction_cnt).label(
                                             'add_cart_auction_cnt'),
                                         func.avg(TbSycmShopAuctionDetail.pay_order_cvr).label('pay_order_cvr'),
                                         func.avg(TbSycmShopAuctionDetail.avg_uv_value).label('avg_uv_value'),
                                         func.sum(TbSycmShopAuctionDetail.selling_after_sales_succ_refund_amt).label(
                                             'selling_after_sales_succ_refund_amt'),
                                         func.sum(TbSycmShopAuctionDetail.selling_after_sales_succ_refund_cnt).label(
                                             'selling_after_sales_succ_refund_cnt'),
                                         (func.sum(TbSycmShopAuctionDetail.selling_after_sales_succ_refund_amt)
                                          / func.nullif(func.sum(TbSycmShopAuctionDetail.alipay_trade_amt), 0))
                                         .label('selling_after_sales_succ_refund_amt_rate'),
                                         func.max(TbSycmShopAuctionDetail.pic_path).label('pic_path'),
                                         IomScmProductList.product_category2) \
                .outerjoin(IomScmProductList, IomScmProductList.product_sn == TbSycmShopAuctionDetail.outer_goods_id) \
                .filter(TbSycmShopAuctionDetail.shop_id == shop_id) \
                .filter(and_(TbSycmShopAuctionDetail.st_date <= data_range.split('|')[1],
                             TbSycmShopAuctionDetail.st_date >= data_range.split('|')[0])) \
                .group_by(TbSycmShopAuctionDetail.auction_id,  # TbSycmShopAuctionDetail.auction_title,
                          TbSycmShopAuctionDetail.outer_goods_id,
                          TbSycmShopAuctionDetail.auction_url, IomScmProductList.product_category2
                          # , TbSycmShopAuctionDetail.pic_path
                          )
            ret = ret.filter(or_(TbSycmShopAuctionDetail.auction_id.like('%%%s%%' % code),
                                 TbSycmShopAuctionDetail.outer_goods_id.like('%%%s%%' % code)))
            if category != 'all' and category is not None:
                ret = ret.filter(IomScmProductList.product_category2 == category)
            if product_year != 'all' and product_year is not None:
                ret = ret.filter(IomScmProductList.product_year == product_year)
            if product_season != 'all' and product_season is not None:
                ret = ret.filter(IomScmProductList.product_season == product_season)
            ret = ret.order_by(asc(sort_field))
            ret = ret.slice((int(page) - 1) * int(page_size), int(page) * int(page_size))

        data = []
        exchange = lambda value: float(value) if value is not None else 0
        # print(ret)
        for val in ret:
            # record = val.to_dict()
            # record['st_date'] = record['st_date'].strftime('%Y-%m-%d')  #日期格式转换
            data.append({'auction_title': val.auction_title, 'auction_id': val.auction_id, 'uv': exchange(val.uv),
                         'outer_goods_id': val.outer_goods_id,
                         'alipay_trade_amt': exchange(val.alipay_trade_amt),
                         'avg_amt_per_order': exchange(val.avg_amt_per_order),
                         'collect_goods_buyer_cnt': val.collect_goods_buyer_cnt,
                         'alipay_auction_cnt': val.alipay_auction_cnt,
                         'add_cart_auction_cnt': val.add_cart_auction_cnt, 'pay_order_cvr': exchange(val.pay_order_cvr),
                         'avg_uv_value': exchange(val.avg_uv_value),
                         'selling_after_sales_succ_refund_amt': exchange(val.selling_after_sales_succ_refund_amt),
                         'selling_after_sales_succ_refund_cnt': exchange(val.selling_after_sales_succ_refund_cnt),
                         'auction_url': val.auction_url,
                         'pic_path': val.pic_path,
                         'per_auction_price': exchange(val.per_auction_price),
                         'selling_after_sales_succ_refund_amt_rate': exchange(
                             val.selling_after_sales_succ_refund_amt_rate)})
        context['data'] = {"list": data, "total": cnt, "page": int(page)}
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/goods_detail/export', methods=['GET'], endpoint='goods_detail_export')
@__token_download
def goods_detail_export(context):
    '''
    获取商品整体明细
    :param context:
    :return:
    '''
    try:
        shop_id = request.args.get('shopId')
        data_range = request.args.get('dateRange')
        sort_field = request.args.get('sortField')
        order = request.args.get('order')
        code = request.args.get('goodsCode')
        file_name = request.args.get('fileName')
        if shop_id is None:
            raise TypeError('parameter shopId is NoneType')
        if data_range is None:
            raise TypeError('parameter dataRange is NoneType')
        if sort_field is None:
            raise TypeError('parameter sortField is NoneType')
        if order is None:
            raise TypeError('parameter order is NoneType')
        if code is None:
            raise TypeError('parameter code is NoneType')
        if file_name is None:
            raise TypeError('parameter fileName is NoneType')
        sort_field_dict = {'auction_id': '款号',
                           'auction_title': '商品名称',
                           'auction_url': '商品链接',
                           'uv': '访客数',
                           'alipay_trade_amt': '支付金额',
                           'avg_amt_per_order': '平均客单价',
                           'collect_goods_buyer_cnt': '收藏人数',
                           'alipay_auction_cnt': '支付商品件数',
                           'add_cart_auction_cnt': '加购件数',
                           'pay_order_cvr': '平均支付转化率',
                           'avg_uv_value': '访客平均价值'}
        if order == 'descend':
            ret = Scope['vertica'].query(TbSycmShopAuctionDetail.auction_id.label('淘宝ID'),
                                         TbSycmShopAuctionDetail.outer_goods_id.label('款号'),
                                         func.max(TbSycmShopAuctionDetail.auction_title).label('商品名称'),
                                         TbSycmShopAuctionDetail.auction_url.label('商品链接'),
                                         func.sum(TbSycmShopAuctionDetail.uv).label('访客数'),
                                         func.sum(TbSycmShopAuctionDetail.alipay_trade_amt).label('支付金额'),
                                         func.avg(TbSycmShopAuctionDetail.avg_amt_per_order).label('平均客单价'),
                                         func.sum(TbSycmShopAuctionDetail.collect_goods_buyer_cnt).label('收藏人数'),
                                         func.sum(TbSycmShopAuctionDetail.alipay_auction_cnt).label('支付商品件数'),
                                         (func.sum(TbSycmShopAuctionDetail.alipay_trade_amt) /
                                          func.nullif(func.sum(TbSycmShopAuctionDetail.alipay_auction_cnt), 0)).label(
                                             '件单价'),
                                         func.sum(TbSycmShopAuctionDetail.add_cart_auction_cnt).label('加购件数'),
                                         func.avg(TbSycmShopAuctionDetail.pay_order_cvr).label('平均支付转化率'),
                                         func.avg(TbSycmShopAuctionDetail.avg_uv_value).label('访客平均价值'),
                                         func.sum(TbSycmShopAuctionDetail.selling_after_sales_succ_refund_amt).label(
                                             '成功退款金额'),
                                         func.sum(TbSycmShopAuctionDetail.selling_after_sales_succ_refund_cnt).label(
                                             '成功退款笔数'),
                                         (func.sum(TbSycmShopAuctionDetail.selling_after_sales_succ_refund_amt)
                                          / func.nullif(func.sum(TbSycmShopAuctionDetail.alipay_trade_amt), 0)).label(
                                             '退款率')
                                         ) \
                .filter(TbSycmShopAuctionDetail.shop_id == shop_id) \
                .filter(and_(TbSycmShopAuctionDetail.st_date <= data_range.split('|')[1],
                             TbSycmShopAuctionDetail.st_date >= data_range.split('|')[0])) \
                .group_by(TbSycmShopAuctionDetail.auction_id,  # TbSycmShopAuctionDetail.auction_title,
                          TbSycmShopAuctionDetail.outer_goods_id,
                          TbSycmShopAuctionDetail.auction_url)
            ret = ret.filter(or_(TbSycmShopAuctionDetail.auction_id.like('%%%s%%' % code),
                                 TbSycmShopAuctionDetail.auction_title.like('%%%s%%' % code)))
            ret = ret.order_by(desc(sort_field_dict[sort_field]))
        else:
            ret = Scope['vertica'].query(TbSycmShopAuctionDetail.auction_id.label('淘宝ID'),
                                         TbSycmShopAuctionDetail.outer_goods_id.label('款号'),
                                         func.max(TbSycmShopAuctionDetail.auction_title).label('商品名称'),
                                         TbSycmShopAuctionDetail.auction_url.label('商品链接'),
                                         func.sum(TbSycmShopAuctionDetail.uv).label('访客数'),
                                         func.sum(TbSycmShopAuctionDetail.alipay_trade_amt).label('支付金额'),
                                         func.avg(TbSycmShopAuctionDetail.avg_amt_per_order).label('平均客单价'),
                                         func.sum(TbSycmShopAuctionDetail.collect_goods_buyer_cnt).label('收藏人数'),
                                         func.sum(TbSycmShopAuctionDetail.alipay_auction_cnt).label('支付商品件数'),
                                         (func.sum(TbSycmShopAuctionDetail.alipay_trade_amt) /
                                          func.sum(TbSycmShopAuctionDetail.alipay_auction_cnt)).label('件单价'),
                                         func.sum(TbSycmShopAuctionDetail.add_cart_auction_cnt).label('加购件数'),
                                         func.avg(TbSycmShopAuctionDetail.pay_order_cvr).label('平均支付转化率'),
                                         func.avg(TbSycmShopAuctionDetail.avg_uv_value).label('访客平均价值'),
                                         func.sum(TbSycmShopAuctionDetail.selling_after_sales_succ_refund_amt).label(
                                             '成功退款金额'),
                                         func.sum(TbSycmShopAuctionDetail.selling_after_sales_succ_refund_cnt).label(
                                             '成功退款笔数'),
                                         (func.sum(TbSycmShopAuctionDetail.selling_after_sales_succ_refund_amt)
                                          / func.nullif(func.sum(TbSycmShopAuctionDetail.alipay_trade_amt), 0)).label(
                                             '退款率')
                                         ) \
                .filter(TbSycmShopAuctionDetail.shop_id == shop_id) \
                .filter(and_(TbSycmShopAuctionDetail.st_date <= data_range.split('|')[1],
                             TbSycmShopAuctionDetail.st_date >= data_range.split('|')[0])) \
                .group_by(TbSycmShopAuctionDetail.auction_id,  # TbSycmShopAuctionDetail.auction_title,
                          TbSycmShopAuctionDetail.outer_goods_id,
                          TbSycmShopAuctionDetail.auction_url)
            ret = ret.filter(or_(TbSycmShopAuctionDetail.auction_id.like('%%%s%%' % code),
                                 TbSycmShopAuctionDetail.auction_title.like('%%%s%%' % code)))
            ret = ret.order_by(asc(sort_field_dict[sort_field]))
        # print(type(ret.all()))
        columns_names = ['商品名称', '款号', '访客数',
                         '支付金额',
                         '平均客单价',
                         '收藏人数',
                         '支付商品件数', '件单价',
                         '加购件数', '平均支付转化率',
                         '访客平均价值',
                         '商品链接', '成功退款金额', '成功退款笔数', '退款率']
        return excel.make_response_from_query_sets(ret.all(), column_names=columns_names, file_type='xlsx',
                                                   file_name=file_name)

    finally:
        Scope['vertica'].remove()


@good.route('/sku_detail', methods=['GET'], endpoint='sku_detail')
@__token_wrapper
def sku_detail(context):
    '''
    获取商品sku整体明细
    :param context:
    :return:
    '''
    try:
        shop_id = request.args.get('shopId')
        data_range = request.args.get('dateRange')
        sort_field = request.args.get('sortField')
        order = request.args.get('order')
        page_size = request.args.get('pageSize')
        page = request.args.get('page')
        code = request.args.get('goodsCode')
        if shop_id is None:
            raise TypeError('parameter shopId is NoneType')
        if data_range is None:
            raise TypeError('parameter dataRange is NoneType')
        if sort_field is None:
            raise TypeError('parameter sortField is NoneType')
        if order is None:
            raise TypeError('parameter order is NoneType')
        if code is None:
            raise TypeError('parameter code is NoneType')
        if page_size is None:
            raise TypeError('parameter pageSize is NoneType')
        if page is None:
            raise TypeError('parameter page is NoneType')

        count = Scope['vertica'].query(func.max(TbSycmAuctionSKUDetail.auction_title).label('商品名称'),
                                       cast(TbSycmAuctionSKUDetail.auction_id, String).label('款号'),
                                       TbSycmAuctionSKUDetail.tb_sku.label('商品SKU'),
                                       cast(TbSycmAuctionSKUDetail.tb_sku_id, String).label('SKU ID'),
                                       func.sum(TbSycmAuctionSKUDetail.add_cart_auction_cnt).label('加购件数'),
                                       func.sum(TbSycmAuctionSKUDetail.gmv_auction_cnt).label('下单件数'),
                                       func.sum(TbSycmAuctionSKUDetail.alipay_auction_cnt).label('支付件数'),
                                       func.sum(TbSycmAuctionSKUDetail.alipay_trade_amt).label('支付金额')) \
            .filter(TbSycmAuctionSKUDetail.shop_id == shop_id) \
            .filter(and_(TbSycmAuctionSKUDetail.st_date <= data_range.split('|')[1],
                         TbSycmAuctionSKUDetail.st_date >= data_range.split('|')[0])) \
            .group_by(  # TbSycmAuctionSKUDetail.auction_title,
            TbSycmAuctionSKUDetail.auction_id,
            TbSycmAuctionSKUDetail.tb_sku, TbSycmAuctionSKUDetail.tb_sku_id)
        count = count.filter(
            or_(cast(TbSycmAuctionSKUDetail.auction_id, String).label('auction_id').like('%%%s%%' % code),
                TbSycmAuctionSKUDetail.auction_title.like('%%%s%%' % code)))

        cnt = count.count()

        if order == 'descend':
            e3_query = Scope['vertica'].query(E3TaobaoItems.sku_id.label('sku_id'),
                                              func.last_value(E3TaobaoItems.pic_path)
                                              .over(partition_by=E3TaobaoItems.sku_id).label('pic_path'),
                                              func.max(E3TaobaoItems.lastchanged).label('lastchanged')) \
                .filter(func.right(E3TaobaoItems.pic_path, 2) != 'Xa') \
                .filter(func.right(E3TaobaoItems.pic_path, 4) != '.gif') \
                .filter(func.right(E3TaobaoItems.pic_path, 18) != 'Xa_!!130974249.jpg') \
                .group_by(E3TaobaoItems.sku_id, E3TaobaoItems.pic_path).subquery()
            alis = aliased(e3_query)

            ret = Scope['vertica'].query(func.max(TbSycmAuctionSKUDetail.auction_title).label('auction_title'),
                                         cast(TbSycmAuctionSKUDetail.auction_id, String).label('auction_id'),
                                         TbSycmAuctionSKUDetail.tb_sku,
                                         TbSycmAuctionSKUDetail.tb_sku_id,
                                         func.sum(TbSycmAuctionSKUDetail.add_cart_auction_cnt).label(
                                             'add_cart_auction_cnt'),
                                         func.sum(TbSycmAuctionSKUDetail.gmv_auction_cnt).label('gmv_auction_cnt'),
                                         func.sum(TbSycmAuctionSKUDetail.alipay_auction_cnt).label(
                                             'alipay_auction_cnt'),
                                         func.sum(TbSycmAuctionSKUDetail.alipay_trade_amt).label('alipay_trade_amt'),
                                         (func.sum(TbSycmAuctionSKUDetail.alipay_trade_amt) /
                                          func.nullif(func.sum(TbSycmAuctionSKUDetail.alipay_auction_cnt), 0)).label(
                                             'per_auction_price'),
                                         alis.c.pic_path) \
                .outerjoin(alis, alis.c.sku_id == TbSycmAuctionSKUDetail.tb_sku_id) \
                .filter(TbSycmAuctionSKUDetail.shop_id == shop_id) \
                .filter(and_(TbSycmAuctionSKUDetail.st_date <= data_range.split('|')[1],
                             TbSycmAuctionSKUDetail.st_date >= data_range.split('|')[0])) \
                .group_by(TbSycmAuctionSKUDetail.auction_title, TbSycmAuctionSKUDetail.auction_id,
                          TbSycmAuctionSKUDetail.tb_sku, TbSycmAuctionSKUDetail.tb_sku_id, alis.c.pic_path)
            ret = ret.filter(
                or_(cast(TbSycmAuctionSKUDetail.auction_id, String).label('auction_id').like('%%%s%%' % code),
                    TbSycmAuctionSKUDetail.auction_title.like('%%%s%%' % code)))
            ret = ret.order_by(desc(sort_field))
            ret = ret.slice((int(page) - 1) * int(page_size), int(page) * int(page_size))
        else:
            e3_query = Scope['vertica'].query(E3TaobaoItems.sku_id.label('sku_id'),
                                              func.last_value(E3TaobaoItems.pic_path)
                                              .over(partition_by=E3TaobaoItems.sku_id).label('pic_path'),
                                              func.max(E3TaobaoItems.lastchanged).label('lastchanged')) \
                .filter(func.right(E3TaobaoItems.pic_path, 2) != 'Xa') \
                .filter(func.right(E3TaobaoItems.pic_path, 4) != '.gif') \
                .filter(func.right(E3TaobaoItems.pic_path, 18) != 'Xa_!!130974249.jpg') \
                .group_by(E3TaobaoItems.sku_id, E3TaobaoItems.pic_path).subquery()
            alis = aliased(e3_query)

            ret = Scope['vertica'].query(func.max(TbSycmAuctionSKUDetail.auction_title).label('auction_title'),
                                         cast(TbSycmAuctionSKUDetail.auction_id, String).label('auction_id'),
                                         TbSycmAuctionSKUDetail.tb_sku,
                                         TbSycmAuctionSKUDetail.tb_sku_id,
                                         func.sum(TbSycmAuctionSKUDetail.add_cart_auction_cnt).label(
                                             'add_cart_auction_cnt'),
                                         func.sum(TbSycmAuctionSKUDetail.gmv_auction_cnt).label('gmv_auction_cnt'),
                                         func.sum(TbSycmAuctionSKUDetail.alipay_auction_cnt).label(
                                             'alipay_auction_cnt'),
                                         func.sum(TbSycmAuctionSKUDetail.alipay_trade_amt).label('alipay_trade_amt'),
                                         (func.sum(TbSycmAuctionSKUDetail.alipay_trade_amt) /
                                          func.nullif(func.sum(TbSycmAuctionSKUDetail.alipay_auction_cnt), 0)).label(
                                             'per_auction_price'),
                                         alis.c.pic_path) \
                .outerjoin(alis, alis.c.sku_id == TbSycmAuctionSKUDetail.tb_sku_id) \
                .filter(TbSycmAuctionSKUDetail.shop_id == shop_id) \
                .filter(and_(TbSycmAuctionSKUDetail.st_date <= data_range.split('|')[1],
                             TbSycmAuctionSKUDetail.st_date >= data_range.split('|')[0])) \
                .group_by(TbSycmAuctionSKUDetail.auction_title, TbSycmAuctionSKUDetail.auction_id,
                          TbSycmAuctionSKUDetail.tb_sku, TbSycmAuctionSKUDetail.tb_sku_id, alis.c.pic_path)
            ret = ret.filter(
                or_(cast(TbSycmAuctionSKUDetail.auction_id, String).label('auction_id').like('%%%s%%' % code),
                    TbSycmAuctionSKUDetail.auction_title.like('%%%s%%' % code)))
            ret = ret.order_by(asc(sort_field))
            ret = ret.slice((int(page) - 1) * int(page_size), int(page) * int(page_size))
        data = []
        exchange = lambda value: float(value) if value is not None else 0
        for val in ret:
            # record = val.to_dict()
            # record['st_date'] = record['st_date'].strftime('%Y-%m-%d')  #日期格式转换
            data.append({'auction_id': val.auction_id, 'tb_sku_id': val.tb_sku_id, 'tb_sku': val.tb_sku,
                         'auction_title': val.auction_title,
                         'add_cart_auction_cnt': val.add_cart_auction_cnt, 'gmv_auction_cnt': val.gmv_auction_cnt,
                         'alipay_auction_cnt': val.alipay_auction_cnt,
                         'alipay_trade_amt': exchange(val.alipay_trade_amt),
                         'per_auction_price': exchange(val.per_auction_price),
                         'pic_path': val.pic_path})
        context['data'] = {"list": data, "total": cnt, "page": int(page)}
        return jsonify(context)

    finally:
        Scope['vertica'].remove()


# @good.route('/get_sku_total', methods=['GET'], endpoint='get_sku_total')
# @__token_wrapper
# def get_sku_total(context):
#     '''
#     获取商品整体明细
#     :param context:
#     :return:
#     '''
#     try:
#         shop_id = request.args.get('shopId')
#         data_range = request.args.get('dateRange')
#         code = request.args.get('goodsCode')
#
#         ret = Scope['vertica'].query(TbSycmAuctionSKUDetail.auction_title.label('商品名称'),
#                             cast(TbSycmAuctionSKUDetail.auction_id, String).label('款号'),
#                             TbSycmAuctionSKUDetail.tb_sku.label('商品SKU'),
#                             cast(TbSycmAuctionSKUDetail.tb_sku_id, String).label('SKU ID'),
#                             func.sum(TbSycmAuctionSKUDetail.add_cart_auction_cnt).label('加购件数'),
#                             func.sum(TbSycmAuctionSKUDetail.gmv_auction_cnt).label('下单件数'),
#                             func.sum(TbSycmAuctionSKUDetail.alipay_auction_cnt).label('支付件数'),
#                             func.sum(TbSycmAuctionSKUDetail.alipay_trade_amt).label('支付金额')) \
#             .filter(TbSycmAuctionSKUDetail.shop_id == shop_id) \
#             .filter(and_(TbSycmAuctionSKUDetail.st_date <= data_range.split('|')[1],
#                          TbSycmAuctionSKUDetail.st_date >= data_range.split('|')[0])) \
#             .group_by(TbSycmAuctionSKUDetail.auction_title, TbSycmAuctionSKUDetail.auction_id,
#                       TbSycmAuctionSKUDetail.tb_sku, TbSycmAuctionSKUDetail.tb_sku_id)
#         ret = ret.filter(
#             or_(cast(TbSycmAuctionSKUDetail.auction_id, String).label('auction_id').like('%%%s%%' % code),
#                 TbSycmAuctionSKUDetail.auction_title.like('%%%s%%' % code)))
#
#         cnt = ret.count()
#         context['data'] = {'total': cnt}
#         return jsonify(context)
#     except Exception as e:
#         logger.error(e)
#         context['message'] = "获取数据时出错，请联系管理员！"
#         context['statusCode'] = -1
#         context['success'] = False
#         context['data'] = []
#         return jsonify(context)
#     finally:
#         Scope['vertica'].remove()


@good.route('/sku_detail/export', methods=['GET'], endpoint='sku_detail_export')
@__token_download
def sku_detail_export(context):
    '''
    获取商品sku整体明细
    :param context:
    :return:
    '''
    try:
        shop_id = request.args.get('shopId')
        data_range = request.args.get('dateRange')
        sort_field = request.args.get('sortField')
        order = request.args.get('order')
        code = request.args.get('goodsCode')
        file_name = request.args.get('fileName')
        if shop_id is None:
            raise TypeError('parameter shopId is NoneType')
        if data_range is None:
            raise TypeError('parameter dataRange is NoneType')
        if sort_field is None:
            raise TypeError('parameter sortField is NoneType')
        if order is None:
            raise TypeError('parameter order is NoneType')
        if code is None:
            raise TypeError('parameter code is NoneType')
        if file_name is None:
            raise TypeError('parameter fileName is NoneType')
        sort_field_dict = {'auction_id': '款号', 'tb_sku_id': 'SKU ID', 'tb_sku': '商品SKU', 'auction_title': '商品名称',
                           'add_cart_auction_cnt': '加购件数', 'gmv_auction_cnt': '下单件数', 'alipay_auction_cnt': '支付件数',
                           'alipay_trade_amt': '支付金额'}
        if order == 'descend':
            ret = Scope['vertica'].query(TbSycmAuctionSKUDetail.auction_title.label('商品名称'),
                                         cast(TbSycmAuctionSKUDetail.auction_id, String).label('款号'),
                                         TbSycmAuctionSKUDetail.tb_sku.label('商品SKU'),
                                         cast(TbSycmAuctionSKUDetail.tb_sku_id, String).label('SKU ID'),
                                         func.sum(TbSycmAuctionSKUDetail.add_cart_auction_cnt).label('加购件数'),
                                         func.sum(TbSycmAuctionSKUDetail.gmv_auction_cnt).label('下单件数'),
                                         func.sum(TbSycmAuctionSKUDetail.alipay_auction_cnt).label('支付件数'),
                                         func.sum(TbSycmAuctionSKUDetail.alipay_trade_amt).label('支付金额'),
                                         (func.sum(TbSycmAuctionSKUDetail.alipay_trade_amt) /
                                          func.nullif(func.sum(TbSycmAuctionSKUDetail.alipay_auction_cnt), 0)).label(
                                             '件单价'),
                                         ) \
                .filter(TbSycmAuctionSKUDetail.shop_id == shop_id) \
                .filter(and_(TbSycmAuctionSKUDetail.st_date <= data_range.split('|')[1],
                             TbSycmAuctionSKUDetail.st_date >= data_range.split('|')[0])) \
                .group_by(TbSycmAuctionSKUDetail.auction_title, TbSycmAuctionSKUDetail.auction_id,
                          TbSycmAuctionSKUDetail.tb_sku, TbSycmAuctionSKUDetail.tb_sku_id)
            ret = ret.filter(
                or_(cast(TbSycmAuctionSKUDetail.auction_id, String).label('auction_id').like('%%%s%%' % code),
                    TbSycmAuctionSKUDetail.auction_title.like('%%%s%%' % code)))
            ret = ret.order_by(desc(sort_field_dict[sort_field]))
        else:
            ret = Scope['vertica'].query(TbSycmAuctionSKUDetail.auction_title.label('商品名称'),
                                         cast(TbSycmAuctionSKUDetail.auction_id, String).label('款号'),
                                         TbSycmAuctionSKUDetail.tb_sku.label('商品SKU'),
                                         cast(TbSycmAuctionSKUDetail.tb_sku_id, String).label('SKU ID'),
                                         func.sum(TbSycmAuctionSKUDetail.add_cart_auction_cnt).label('加购件数'),
                                         func.sum(TbSycmAuctionSKUDetail.gmv_auction_cnt).label('下单件数'),
                                         func.sum(TbSycmAuctionSKUDetail.alipay_auction_cnt).label('支付件数'),
                                         func.sum(TbSycmAuctionSKUDetail.alipay_trade_amt).label('支付金额'),
                                         (func.sum(TbSycmAuctionSKUDetail.alipay_trade_amt) /
                                          func.nullif(func.sum(TbSycmAuctionSKUDetail.alipay_auction_cnt), 0)).label(
                                             '件单价')
                                         ) \
                .filter(TbSycmAuctionSKUDetail.shop_id == shop_id) \
                .filter(and_(TbSycmAuctionSKUDetail.st_date <= data_range.split('|')[1],
                             TbSycmAuctionSKUDetail.st_date >= data_range.split('|')[0])) \
                .group_by(TbSycmAuctionSKUDetail.auction_title, TbSycmAuctionSKUDetail.auction_id,
                          TbSycmAuctionSKUDetail.tb_sku, TbSycmAuctionSKUDetail.tb_sku_id)
            ret = ret.filter(
                or_(cast(TbSycmAuctionSKUDetail.auction_id, String).label('auction_id').like('%%%s%%' % code),
                    TbSycmAuctionSKUDetail.auction_title.like('%%%s%%' % code)))
            ret = ret.order_by(asc(sort_field_dict[sort_field]))

        columns_names = ['款号', 'SKU ID', '商品SKU', '商品名称', '加购件数',
                         '下单件数', '支付件数', '支付金额', '件单价']
        return excel.make_response_from_query_sets(ret.all(), column_names=columns_names, file_type='xlsx',
                                                   file_name=file_name)

    finally:
        Scope['vertica'].remove()


# ######################单品分析##################################
@good.route('/goodsAnalysis', methods=['GET'], endpoint='goodsAnalysis')
@__token_wrapper
def goodsAnalysis(context):
    try:
        product_sn = request.args.get('product_sn')
        data = {
            'goods_bn': product_sn,
            'token': "A8764D6A38967D293F924FC7DAA1FFC"
        }
        headers = {'Content-Type': 'application/json; charset=UTF-8'}
        r = requests.post('http://goodsanalysis.hmcloud.com.cn/hmcgoodsAnalysis.aspx/Analysis', data=json.dumps(data),
                          headers=headers)
        result = json.loads(r.json()["d"])
        if result['statusCode'] == 200:
            context["statusCode"] = 1
            context["success"] = True
            context["message"] = "操作成功"
            context["data"] = result["data"]
        else:
            context["statusCode"] = 1
            context["success"] = True
            context["message"] = "操作成功"
            context["data"] = []
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)


@good.route('/getSeasonPercent', methods=['GET'], endpoint='getSeasonPercent')
@__token_wrapper
def getSeasonPercent(context):
    try:
        product_sn = request.args.get('product_sn')
        seasonid = request.args.get('seasonid')
        chaneltype = request.args.get('chaneltype')
        data = {
            'goods_bn': product_sn,
            "seasonid": seasonid,
            "chaneltype": chaneltype
        }
        headers = {'Content-Type': 'application/json; charset=UTF-8'}
        r = requests.post('http://goodsanalysis.hmcloud.com.cn/hmcgoodsAnalysis.aspx/GetSeasonWeekpercent2',
                          data=json.dumps(data), headers=headers)
        result = r.json()["d"]
        data = {}
        data["week"] = result[0]
        data["channel"] = result[1]
        data["sales"] = result[2]
        data["title"] = result[3]
        # print(json.dumps(data))
        context["statusCode"] = 1
        context["success"] = True
        context["message"] = "操作成功"
        context["data"] = data

        # print(context)
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)


# 质量退返
@good.route('/get_quality_return', methods=['GET'], endpoint='get_quality_return')
@__token_wrapper
def get_produce_info(context):
    try:
        brand_name = request.args.get('brand_name')
        doc_year = int(request.args.get('doc_year'))
        doc_season = request.args.get('doc_season')
        # print(doc_season)
        if doc_season == '':
            context['data'] = []
            return jsonify(context)
        # sql = f"""
        # SELECT * FROM hmcdata.bi_quality_report WHERE doc_year={doc_year} AND doc_season in ({doc_season}) AND CategoryShopType='{brand_name}' AND channel<>'共款'
        # ORDER BY channel,return_type
        #     """
        sql = f"""SELECT SUM(product_count) as product_count,SUM(wb_quantity) as wb_quantity,SUM(DefectiveQuantity) as DefectiveQuantity,SUM(tran_total) as tran_total,
        SUM(sale_nums) as sale_nums,SUM(quantity_return) as quantity_return,SUM("return") as 'return',doc_year,check_type,return_type,channel FROM hmcdata.bi_quality_report 
        WHERE doc_year={doc_year} AND doc_season in ({doc_season}) AND CategoryShopType='{brand_name}' 
        GROUP BY doc_year,channel,check_type,return_type
        ORDER BY channel,return_type"""
        # print(sql)
        ret = Scope['vertica'].execute(sql)
        # data = {'now': {}, 'last': {}}

        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_5(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_5(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                elif val[i] is None:
                    data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)
            # print(data_dict)
            # if data_dict['time'] == 'now':
            #     data['now'] = data_dict
            # else:
            #     data['last'] = data_dict
        context['data'] = data
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


# 商品总数
@good.route('/get_goods_rank_total', methods=['GET'], endpoint='get_goods_rank_total')
@__token_wrapper
def get_goods_rank(context):
    try:
        business_id = request.args.get('business_id')
        dateStart = request.args.get('dateStart')
        dateEnd = request.args.get('dateEnd')
        dc_shop_id = request.args.get('dc_shop_id')
        CategoryClass = request.args.get('CategoryClass')
        if CategoryClass == 'all' or CategoryClass is None:
            cat_sql = ""
        else:
            cat_sql = f""" AND g.product_category2='{CategoryClass}'"""

        if dc_shop_id == 'all' or dc_shop_id is None:
            sql_str = ""
        else:
            sql_str = f"""AND dc_shop_id={dc_shop_id}"""

        sql = f"""
        SELECT count(*) as total from(
        SELECT 1 FROM bi_shop_saleinfo_goods g
        LEFT JOIN bi_shops_new s ON g.dc_shop_id=s.shop_id
        LEFT JOIN iom_scm_product_list p ON g.product_sn=p.ProductSN
        WHERE totalday>='{dateStart}' AND totalday<='{dateEnd}' AND s.business_clazz_id={business_id} 
        {sql_str}  {cat_sql}
        GROUP BY g.product_sn,g.product_category2
        )a
        """
        # print(sql)
        ret = Scope['bi_saas'].execute(sql)
        # data = {'now': {}, 'last': {}}

        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                elif val[i] is None:
                    data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)

        context['data'] = data[0]
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()


# 商品排名
@good.route('/get_goods_rank', methods=['GET'], endpoint='get_goods_rank')
@__token_wrapper
def get_goods_rank(context):
    try:
        business_id = request.args.get('business_id')
        dateStart = request.args.get('dateStart')
        dateEnd = request.args.get('dateEnd')
        dc_shop_id = request.args.get('dc_shop_id')
        page = request.args.get('page')
        page_size = request.args.get('pageSize')
        CategoryClass = request.args.get('CategoryClass')

        sort_field = request.args.get('sortField')
        order = request.args.get('order')

        sort_dict = {'price_sale': 'SUM(price_sale)', 'order_products': 'SUM(order_products)',
                     'order_count': 'SUM(order_count)'}
        if order is None:
            order_by = "ORDER BY SUM(price_sale) DESC"
        else:
            order_by = f''' ORDER BY {sort_dict[sort_field]} {'DESC' if order == 'descend' else 'ASC'} '''
        if dc_shop_id == 'all' or dc_shop_id is None:
            sql_str = ""
        else:
            sql_str = f"""AND dc_shop_id={dc_shop_id}"""
        if CategoryClass == 'all' or CategoryClass is None:
            cat_sql = ""
        else:
            cat_sql = f""" AND g.product_category2='{CategoryClass}'"""

        if page_size is None:
            page_size = 20
        offset = f''' LIMIT {int(page_size)} OFFSET {(int(page) - 1) * int(page_size)} '''

        sql = f"""SELECT i.img_src,g.product_sn,SUM(price_sale) as price_sale,
        (SUM(price_sale)-SUM(sale_cost))/SUM(price_sale) as GM,SUM(order_products) as order_products,SUM(order_count) as order_count,
        SUM(price_sale)/SUM(order_products) as unit_price,product_year,product_season,g.product_category2,max(la.product_label) as product_label FROM bi_shop_saleinfo_goods g
        LEFT JOIN bi_shops_new s ON g.dc_shop_id=s.shop_id
        LEFT JOIN bi_product_img i ON g.product_sn=i.product_sn AND i.relate_type=1 
		LEFT JOIN product_sales_label la on la.product_sn=g.product_sn
        WHERE totalday>='{dateStart}' AND totalday<='{dateEnd}' AND s.business_clazz_id={business_id}
        {sql_str} {cat_sql}
        GROUP BY g.product_sn,g.product_category2
        {order_by}
        {offset}
        """
        # print(sql)
        ret = Scope['bi_saas'].execute(sql)
        # data = {'now': {}, 'last': {}}

        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                # elif val[i] is None:
                #     data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)

        context['data'] = data
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()


@good.route('/export_goods_rank', methods=['GET'], endpoint='export_goods_rank')
@__token_download
def export_goods_rank(context):
    """
    商品排名导出
    @param context:
    @return:
    """
    try:
        business_id = request.args.get('business_id')
        dateStart = request.args.get('dateStart')
        dateEnd = request.args.get('dateEnd')
        dc_shop_id = request.args.get('dc_shop_id')
        CategoryClass = request.args.get('CategoryClass')
        business_name = request.args.get('business_name')
        shop_name = request.args.get('shop_name')

        sort_field = request.args.get('sortField')
        order = request.args.get('order')

        sort_dict = {'price_sale': 'SUM(price_sale)', 'order_products': 'SUM(order_products)',
                     'order_count': 'SUM(order_count)'}
        if order is None:
            order_by = "ORDER BY SUM(price_sale) DESC"
        else:
            order_by = f''' ORDER BY {sort_dict[sort_field]} {'DESC' if order == 'descend' else 'ASC'} '''
        if dc_shop_id == 'all' or dc_shop_id is None:
            sql_str = ""
            shop_name_title = "全部店铺"
        else:
            sql_str = f"""AND dc_shop_id={dc_shop_id}"""
            shop_name_title = shop_name

        if CategoryClass == 'all' or CategoryClass is None:
            cat_sql = ""
            cat_title = "全类目"
        else:
            cat_sql = f""" AND g.product_category2='{CategoryClass}'"""
            cat_title = "全类目"

        sql = f"""SELECT s.shop_name as '店铺',g.product_sn  as '款号',SUM(price_sale) as '销售额',
        (SUM(price_sale)-SUM(sale_cost))/SUM(price_sale) as '毛利率',SUM(order_products) as '销售件数',SUM(order_count) as '订单数',
        SUM(price_sale)/SUM(order_products) as '件单价',product_year as '年份',product_season as '季节',g.product_category2 as '类目' FROM bi_shop_saleinfo_goods g
        LEFT JOIN bi_shops_new s ON g.dc_shop_id=s.shop_id
        LEFT JOIN bi_product_img i ON g.product_sn=i.product_sn AND i.relate_type=1 
        WHERE totalday>='{dateStart}' AND totalday<='{dateEnd}' AND s.business_clazz_id={business_id}
        {sql_str} {cat_sql}
        GROUP BY g.product_sn,g.product_category2,s.shop_name
        """
        ret = Scope['bi_saas'].execute(sql)
        # data = {'now': {}, 'last': {}}
        columns_names = ret.keys()

        return excel.make_response_from_query_sets(ret.fetchall(), column_names=columns_names, file_type='xlsx',
                                                   file_name=f"""{business_name}_{shop_name_title}_商品日报_{dateStart}-{dateEnd}_{cat_title}""")

    finally:
        Scope['bi_saas'].remove()


# 商品排名
@good.route('/get_goods_rank_product_sn', methods=['GET'], endpoint='get_goods_rank_product_sn')
@__token_wrapper
def get_goods_rank_product_sn(context):
    try:
        business_id = request.args.get('business_id')
        dateStart = request.args.get('dateStart')
        dateEnd = request.args.get('dateEnd')
        dc_shop_id = request.args.get('dc_shop_id')
        product_sn = request.args.get('product_sn')

        if dc_shop_id == 'all' or dc_shop_id is None:
            sql_str = ""
        else:
            sql_str = f"""AND dc_shop_id={dc_shop_id}"""

        sql = f"""SELECT i.img_src,g.product_sn,SUM(price_sale) as price_sale,
        (SUM(price_sale)-SUM(sale_cost))/SUM(price_sale) as GM,SUM(order_products) as order_products,SUM(order_count) as order_count,
        SUM(price_sale)/SUM(order_products) as unit_price,product_year,product_season,g.product_category2 FROM bi_shop_saleinfo_goods g
        LEFT JOIN bi_shops_new s ON g.dc_shop_id=s.shop_id
        LEFT JOIN bi_product_img i ON g.product_sn=i.product_sn AND i.relate_type=1 
        WHERE totalday>='{dateStart}' AND totalday<='{dateEnd}' AND s.business_clazz_id={business_id} and g.product_sn='{product_sn}'
        {sql_str} 
        GROUP BY g.product_sn,g.product_category2
   
        """
        # print(sql)
        ret = Scope['bi_saas'].execute(sql)
        # data = {'now': {}, 'last': {}}

        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                elif val[i] is None:
                    data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)

        context['data'] = data
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()


@good.route('/get_goods_size', methods=['GET'], endpoint='get_goods_size')
@__token_wrapper
def get_goods_size(context):
    '''
    获取商品尺码
    :param context:
    :return:
    '''
    try:
        product_sn = request.args.get('product_sn')
        product_sn_list = product_sn.split(',')
        p = ""
        for i in product_sn_list:
            p += ("'" + i + "'" + ',')
        grade = {'XXS': 'A', 'XS': 'B', 'S': 'C', 'M': 'D', 'L': 'E', 'XL': 'F', 'XXL': 'G', 'XXXL': 'H', 'XXXXL': 'I',
                 '均码': 'J', '25': 'A', '26': 'B', '27': 'C', '28': 'D', '29': 'E', '30': 'F', '31': 'G', '32': 'H',
                 '33': 'I', '34': 'B', '35': 'C', '36': 'D', '37': 'E', '38': 'F', '39': 'G', '40': 'H', '110': 'A',
                 '120': 'B', '130': 'C', '140': 'D', '150': 'E', '160': 'F', '165': 'G'}
        sql = f'''SELECT DISTINCT  CASE b.size_name 
        WHEN '25' THEN 'XXS' WHEN '26' THEN 'XS' WHEN '27' THEN 'S' WHEN '28' THEN 'M' WHEN '29' THEN 'L'
        WHEN '30' THEN 'XL' WHEN '31' THEN 'XXL' WHEN '32' THEN 'XXXL' WHEN '33' THEN 'XXXXL' WHEN '34' THEN 'XS'
        WHEN '35' THEN 'S' WHEN '36' THEN 'M' WHEN '37' THEN 'L' WHEN '38' THEN 'XL' WHEN '39' THEN 'XXL' 
        WHEN '40' THEN 'XXXL' WHEN '110' THEN 'XXS' WHEN '120' THEN 'XS' WHEN '130' THEN 'S' WHEN '140' THEN 'M'
        WHEN '150' THEN 'L' WHEN '160' THEN 'XL' WHEN '165' THEN 'XXL' ELSE b.size_name END as size_name 
        FROM hmcdata.iom_scm_product_list a INNER JOIN hmcdata.iom_scm_sku_scu b ON a.product_sn=b.product_sn
            WHERE a.product_sn IN ({p[:-1]}) 

        '''
        # print(sql)
        ret = Scope['vertica'].execute(sql)
        # data = {'now': {}, 'last': {}}

        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if val[i] not in grade.keys():
                    context['message'] = "目前不支持服装以外的品类"
                    context['statusCode'] = -1
                    context['success'] = False
                    context['data'] = []
                    return jsonify(context)
                # if isinstance(val[i], decimal.Decimal):
                #     data_dict[column] = format_4(val[i])
                # elif isinstance(val[i], datetime.datetime):
                #     data_dict[column] = datetime_format(val[i])
                # elif isinstance(val[i], float):
                #     data_dict[column] = format_4(val[i])
                # elif isinstance(val[i], datetime.date):
                #     data_dict[column] = datetime_format(val[i])
                # elif val[i] is None:
                #     data_dict[column] = 0
                # else:
                #     data_dict[column] = val[i]
                data.append(val[i])
        data = list(sorted(data, key=grade.__getitem__))
        context['data'] = data
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()
        Scope['vertica'].remove()


@good.route('/get_goods_size_supply', methods=['GET'], endpoint='get_goods_size_supply')
@__token_wrapper
def get_goods_size_supply(context):
    '''
    商品尺码需下单件数
    :param context:
    :return:
    '''
    try:
        product_sn = request.args.get('product_sn')
        business_unit_id = context['data']['businessUnitId']
        supply_total = request.args.get('supplyTotal')
        product_sn_list = product_sn.split(',')
        p = ""
        for i in product_sn_list:
            p += ("'" + i + "'" + ',')
        if product_sn == '':
            raise Exception(' Parameter product_sn or supply_total is null, please check your parameter! ')

        # sql = f'''
        # SELECT
        # a.color,
        # a.product_sn,
        # stock_xxs_qty stock_xxs_rate,
        # stock_xs_qty stock_xs_rate,
        # stock_s_qty stock_s_rate,
        # stock_m_qty stock_m_rate,
        # stock_l_qty stock_l_rate,
        # stock_xl_qty stock_xl_rate,
        # stock_xxl_qty stock_xxl_rate,
        # stock_xxxl_qty stock_xxxl_rate,
        # stock_xxxxl_qty stock_xxxxl_rate,
        # stock_share_size_qty stock_share_size_rate,
        # ROUND((IFNULL(stock_xxs_qty, 0)+ IFNULL(stock_xs_qty, 0)+IFNULL(stock_s_qty, 0)+IFNULL(stock_m_qty, 0)+IFNULL(stock_l_qty, 0)+IFNULL(stock_xl_qty, 0)+IFNULL(stock_xxl_qty, 0)+IFNULL(stock_xxxl_qty, 0)+IFNULL(stock_xxxxl_qty, 0)+IFNULL(stock_share_size_qty, 0)) / b.stock_all_qty*100,2) as stock_sum_color_rate,
        #
        # ROUND( sale_xxs_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0))*100,2) sale_xxs_rate,
        # ROUND( sale_xs_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0))*100,2) sale_xs_rate,
        # ROUND( sale_s_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0))*100,2) sale_s_rate,
        # ROUND( sale_m_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0))*100,2) sale_m_rate,
        # ROUND( sale_l_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0))*100,2) sale_l_rate,
        # ROUND( sale_xl_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0))*100,2) sale_xl_rate,
        # ROUND( sale_xxl_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0))*100,2) sale_xxl_rate,
        # ROUND( sale_xxxl_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0))*100,2) sale_xxxl_rate,
        # ROUND( sale_xxxxl_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0))*100,2) sale_xxxxl_rate,
        # ROUND( sale_share_size_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0))*100,2) sale_share_size_rate,
        # ROUND((IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0)) / b.sale_all_qty*100,2) as sale_sum_color_rate,
        #
        # ROUND( order_xxs_qty / (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0))*100,2) order_xxs_rate,
        # ROUND( order_xs_qty / (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0))*100,2) order_xs_rate,
        # ROUND( order_s_qty / (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0))*100,2) order_s_rate,
        # ROUND( order_m_qty / (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0))*100,2) order_m_rate,
        # ROUND( order_l_qty / (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0))*100,2) order_l_rate,
        # ROUND( order_xl_qty / (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0))*100,2) order_xl_rate,
        # ROUND( order_xxl_qty / (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0))*100,2) order_xxl_rate,
        # ROUND( order_xxxl_qty / (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0))*100,2) order_xxxl_rate,
        # ROUND( order_xxxxl_qty / (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0))*100,2) order_xxxxl_rate,
        # ROUND( order_share_size_qty / (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0))*100,2) order_share_size_rate,
        # ROUND((IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0)) / b.order_all_qty*100,2) as order_sum_color_rate,
        #
        #
        # b.stock_xxs_total as sum_stock_xxs_rate,
        # b.stock_xs_total as sum_stock_xs_rate,
        # b.stock_s_total as sum_stock_s_rate,
        # b.stock_m_total as sum_stock_m_rate,
        # b.stock_l_total as sum_stock_l_rate,
        # b.stock_xl_total as sum_stock_xl_rate,
        # b.stock_xxl_total as sum_stock_xxl_rate,
        # b.stock_xxxl_total as sum_stock_xxxl_rate,
        # b.stock_xxxxl_total as sum_stock_xxxxl_rate,
        # b.stock_share_size_total as sum_stock_share_size_rate,
        #
        #
        # ROUND( b.sale_xxs_total / case b.sale_all_qty when 0 then null else b.sale_all_qty end * 100, 2) as sum_sale_xxs_rate,
        # ROUND( b.sale_xs_total / case b.sale_all_qty when 0 then null else b.sale_all_qty end * 100, 2) as sum_sale_xs_rate,
        # ROUND( b.sale_s_total / case b.sale_all_qty when 0 then null else b.sale_all_qty end * 100, 2) as sum_sale_s_rate,
        # ROUND( b.sale_m_total / case b.sale_all_qty when 0 then null else b.sale_all_qty end * 100, 2) as sum_sale_m_rate,
        # ROUND( b.sale_l_total / case b.sale_all_qty when 0 then null else b.sale_all_qty end * 100, 2) as sum_sale_l_rate,
        # ROUND( b.sale_xl_total / case b.sale_all_qty when 0 then null else b.sale_all_qty end * 100, 2) as sum_sale_xl_rate,
        # ROUND( b.sale_xxl_total / case b.sale_all_qty when 0 then null else b.sale_all_qty end * 100, 2) as sum_sale_xxl_rate,
        # ROUND( b.sale_xxxl_total / case b.sale_all_qty when 0 then null else b.sale_all_qty end * 100, 2) as sum_sale_xxxl_rate,
        # ROUND( b.sale_xxxxl_total / case b.sale_all_qty when 0 then null else b.sale_all_qty end * 100, 2) as sum_sale_xxxxl_rate,
        # ROUND( b.sale_share_size_total / case b.sale_all_qty when 0 then null else b.sale_all_qty end * 100, 2) as sum_sale_share_size_rate,
        #
        #
        # ROUND( b.order_xxs_total / case b.order_all_qty when 0 then null else b.order_all_qty end * 100, 2) as sum_order_xxs_rate,
        # ROUND( b.order_xs_total / case b.order_all_qty when 0 then null else b.order_all_qty end * 100, 2) as sum_order_xs_rate,
        # ROUND( b.order_s_total / case b.order_all_qty when 0 then null else b.order_all_qty end * 100, 2) as sum_order_s_rate,
        # ROUND( b.order_m_total / case b.order_all_qty when 0 then null else b.order_all_qty end * 100, 2) as sum_order_m_rate,
        # ROUND( b.order_l_total / case b.order_all_qty when 0 then null else b.order_all_qty end * 100, 2) as sum_order_l_rate,
        # ROUND( b.order_xl_total / case b.order_all_qty when 0 then null else b.order_all_qty end * 100, 2) as sum_order_xl_rate,
        # ROUND( b.order_xxl_total / case b.order_all_qty when 0 then null else b.order_all_qty end * 100, 2) as sum_order_xxl_rate,
        # ROUND( b.order_xxxl_total / case b.order_all_qty when 0 then null else b.order_all_qty end * 100, 2) as sum_order_xxxl_rate,
        # ROUND( b.order_xxxxl_total / case b.order_all_qty when 0 then null else b.order_all_qty end * 100, 2) as sum_order_xxxxl_rate,
        # ROUND( b.order_share_size_total / case b.order_all_qty when 0 then null else b.order_all_qty end * 100, 2) as sum_order_share_size_rate,
        #
        #
        # ROUND( ({supply_total} + b.stock_all_qty) * (sale_xxs_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0))) * (sale_xxs_qty / b.sale_xxs_total) - stock_xxs_qty,0) AS supply_xxs_qty,
        # ROUND( ({supply_total} + b.stock_all_qty) * (sale_xs_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0))) * (sale_xs_qty / b.sale_xs_total) - stock_xs_qty,0) AS supply_xs_qty,
        # ROUND( ({supply_total} + b.stock_all_qty) * (sale_s_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0)))  * (sale_s_qty / b.sale_s_total) - stock_s_qty, 0) AS supply_s_qty,
        # ROUND( ({supply_total} + b.stock_all_qty) * (sale_m_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0))) * (sale_m_qty / b.sale_m_total) - stock_m_qty, 0) AS supply_m_qty,
        # ROUND( ({supply_total} + b.stock_all_qty) * (sale_l_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0))) * (sale_l_qty / b.sale_l_total) - stock_l_qty,0) AS supply_l_qty,
        # ROUND( ({supply_total} + b.stock_all_qty) * (sale_xl_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0))) * (sale_xl_qty / b.sale_xl_total) - stock_xl_qty,0) AS supply_xl_qty,
        # ROUND( ({supply_total} + b.stock_all_qty) * (sale_xxl_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0))) * (sale_xxl_qty / b.sale_xxl_total) - stock_xxl_qty,0) AS supply_xxl_qty,
        # ROUND( ({supply_total} + b.stock_all_qty) * (sale_xxxl_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0))) * (sale_xxxl_qty / b.sale_xxxl_total) - stock_xxxl_qty,0) AS supply_xxxl_qty,
        # ROUND( ({supply_total} + b.stock_all_qty) * (sale_xxxxl_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0))) * (sale_xxxxl_qty / b.sale_xxxxl_total) - stock_xxxxl_qty,0) AS supply_xxxxl_qty,
        # ROUND( ({supply_total} + b.stock_all_qty) * (sale_share_size_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0))) * (sale_share_size_qty / b.sale_share_size_total) - stock_share_size_qty,0) AS supply_share_size_qty,
        # #
        # ROUND( (IFNULL( ROUND( ({supply_total} + b.stock_all_qty) * (sale_xxs_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0))) * (sale_xxs_qty / b.sale_xxs_total) - stock_xxs_qty,0),0) +
        # IFNULL( ROUND( ({supply_total} + b.stock_all_qty) * (sale_xs_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0))) * (sale_xs_qty / b.sale_xs_total) - stock_xs_qty,0), 0) +
        # IFNULL( ROUND( ({supply_total} + b.stock_all_qty) * (sale_s_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0)))  * (sale_s_qty / b.sale_s_total) - stock_s_qty, 0), 0) +
        # IFNULL( ROUND( ({supply_total} + b.stock_all_qty) * (sale_m_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0))) * (sale_m_qty / b.sale_m_total) - stock_m_qty, 0), 0) +
        # IFNULL( ROUND( ({supply_total} + b.stock_all_qty) * (sale_l_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0))) * (sale_l_qty / b.sale_l_total) - stock_l_qty,0), 0) +
        # IFNULL( ROUND( ({supply_total} + b.stock_all_qty) * (sale_xl_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0))) * (sale_xl_qty / b.sale_xl_total) - stock_xl_qty,0), 0) +
        # IFNULL( ROUND( ({supply_total} + b.stock_all_qty) * (sale_xxl_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0))) * (sale_xxl_qty / b.sale_xxl_total) - stock_xxl_qty,0), 0) +
        # IFNULL( ROUND( ({supply_total} + b.stock_all_qty) * (sale_xxxl_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0))) * (sale_xxxl_qty / b.sale_xxxl_total) - stock_xxxl_qty,0), 0) +
        # IFNULL( ROUND( ({supply_total} + b.stock_all_qty) * (sale_xxxxl_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0))) * (sale_xxxxl_qty / b.sale_xxxxl_total) - stock_xxxxl_qty,0), 0) +
        # IFNULL( ROUND( ({supply_total} + b.stock_all_qty) * (sale_share_size_qty / (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0))) * (sale_share_size_qty / b.sale_share_size_total) - stock_share_size_qty,0),0)) / {supply_total} * 100, 2)  AS supply_sum_color_rate
        #
        # FROM hmcdata.bi_size_supply a
        # LEFT JOIN (
        # SELECT product_sn
        # , sum(stock_xxs_qty) as stock_xxs_total
        # , sum(stock_xs_qty) as stock_xs_total
        # , sum(stock_s_qty) as stock_s_total
        # , sum(stock_m_qty) as stock_m_total
        # , sum(stock_l_qty) as stock_l_total
        # , sum(stock_xl_qty) as stock_xl_total
        # , sum(stock_xxl_qty) as stock_xxl_total
        # , sum(stock_xxxl_qty) as stock_xxxl_total
        # , sum(stock_xxxxl_qty) as stock_xxxxl_total
        # , sum(stock_share_size_qty) as stock_share_size_total
        #
        #
        # ,IFNULL(sum(stock_xxs_qty), 0)+ IFNULL(sum(stock_xs_qty), 0)+IFNULL(sum(stock_s_qty), 0)+IFNULL(sum(stock_m_qty), 0)+IFNULL(sum(stock_l_qty), 0)+IFNULL(sum(stock_xl_qty), 0)+IFNULL(sum(stock_xxl_qty), 0)+IFNULL(sum(stock_xxxl_qty), 0)+IFNULL(sum(stock_xxxxl_qty), 0)+IFNULL(sum(stock_share_size_qty), 0) as stock_all_qty
        #
        # ,SUM(sale_xxs_qty) as sale_xxs_total
        # ,SUM(sale_xs_qty) as sale_xs_total
        # ,SUM(sale_s_qty) as sale_s_total
        # ,SUM(sale_m_qty) as sale_m_total
        # ,SUM(sale_l_qty) as sale_l_total
        # ,SUM(sale_xl_qty) as sale_xl_total
        # ,SUM(sale_xxl_qty) as sale_xxl_total
        # ,SUM(sale_xxxl_qty) as sale_xxxl_total
        # ,SUM(sale_xxxxl_qty) as sale_xxxxl_total
        # ,SUM(sale_share_size_qty) as sale_share_size_total
        #
        # ,IFNULL(sum(sale_xxs_qty), 0)+ IFNULL(sum(sale_xs_qty), 0)+IFNULL(sum(sale_s_qty), 0)+IFNULL(sum(sale_m_qty), 0)+IFNULL(sum(sale_l_qty), 0)+IFNULL(sum(sale_xl_qty), 0)+IFNULL(sum(sale_xxl_qty), 0)+IFNULL(sum(sale_xxxl_qty), 0)+IFNULL(sum(sale_xxxxl_qty), 0)+IFNULL(sum(sale_share_size_qty), 0) as sale_all_qty
        #
        # , sum(order_xxs_qty) as order_xxs_total
        # , sum(order_xs_qty) as order_xs_total
        # , sum(order_s_qty) as order_s_total
        # , sum(order_m_qty) as order_m_total
        # , sum(order_l_qty) as order_l_total
        # , sum(order_xl_qty) as order_xl_total
        # , sum(order_xxl_qty) as order_xxl_total
        # , sum(order_xxxl_qty) as order_xxxl_total
        # , sum(order_xxxxl_qty) as order_xxxxl_total
        # , sum(order_share_size_qty) as order_share_size_total
        #
        # ,IFNULL(sum(order_xxs_qty), 0)+ IFNULL(sum(order_xs_qty), 0)+IFNULL(sum(order_s_qty), 0)+IFNULL(sum(order_m_qty), 0)+IFNULL(sum(order_l_qty), 0)+IFNULL(sum(order_xl_qty), 0)+IFNULL(sum(order_xxl_qty), 0)+IFNULL(sum(order_xxxl_qty), 0)+IFNULL(sum(order_xxxxl_qty), 0)+IFNULL(sum(order_share_size_qty), 0) as order_all_qty
        #
        #
        # FROM hmcdata.bi_size_supply WHERE product_sn = '{product_sn}' GROUP BY product_sn) b
        # ON a.product_sn=b.product_sn
        # WHERE a.product_sn = '{product_sn}'
        #
        # '''
        sql = f"""SELECT
        b.stock_all_qty,
        a.color,
        a.product_sn,
        stock_xxs_qty stock_xxs_rate,
        stock_xs_qty stock_xs_rate,
        stock_s_qty stock_s_rate,
        stock_m_qty stock_m_rate,
        stock_l_qty stock_l_rate,
        stock_xl_qty stock_xl_rate,
        stock_xxl_qty stock_xxl_rate,
        stock_xxxl_qty stock_xxxl_rate,
        stock_xxxxl_qty stock_xxxxl_rate,
        stock_share_size_qty stock_share_size_rate,
    IFNULL(stock_xxs_qty, 0)+ IFNULL(stock_xs_qty, 0)+IFNULL(stock_s_qty, 0)+IFNULL(stock_m_qty, 0)+IFNULL(stock_l_qty, 0)+IFNULL(stock_xl_qty, 0)+IFNULL(stock_xxl_qty, 0)+IFNULL(stock_xxxl_qty, 0)+IFNULL(stock_xxxxl_qty, 0)+IFNULL(stock_share_size_qty, 0)  as stock_sum_color_rate,

    ROUND( sale_xxs_qty / CASE (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0)) WHEN 0 THEN NULL ELSE (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0)) END *100,2) sale_xxs_rate,
    ROUND( sale_xs_qty / CASE (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0)) WHEN 0 THEN NULL ELSE (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0)) END*100,2) sale_xs_rate,
    ROUND( sale_s_qty / CASE (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0)) WHEN 0 THEN NULL ELSE (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0)) END*100,2) sale_s_rate,
    ROUND( sale_m_qty / CASE (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0)) WHEN 0 THEN NULL ELSE (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0)) END*100,2) sale_m_rate,
    ROUND( sale_l_qty / CASE (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0)) WHEN 0 THEN NULL ELSE (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0)) END*100,2) sale_l_rate,
    ROUND( sale_xl_qty / CASE (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0)) WHEN 0 THEN NULL ELSE (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0)) END*100,2) sale_xl_rate,
    ROUND( sale_xxl_qty / CASE (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0)) WHEN 0 THEN NULL ELSE (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0)) END*100,2) sale_xxl_rate,
    ROUND( sale_xxxl_qty / CASE (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0)) WHEN 0 THEN NULL ELSE (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0)) END*100,2) sale_xxxl_rate,
    ROUND( sale_xxxxl_qty / CASE (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0)) WHEN 0 THEN NULL ELSE (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0)) END*100,2) sale_xxxxl_rate,
    ROUND( sale_share_size_qty / CASE (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0)) WHEN 0 THEN NULL ELSE (IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0)) END*100,2) sale_share_size_rate,
    ROUND((IFNULL(sale_xxs_qty, 0)+ IFNULL(sale_xs_qty, 0)+IFNULL(sale_s_qty, 0)+IFNULL(sale_m_qty, 0)+IFNULL(sale_l_qty, 0)+IFNULL(sale_xl_qty, 0)+IFNULL(sale_xxl_qty, 0)+IFNULL(sale_xxxl_qty, 0)+IFNULL(sale_xxxxl_qty, 0)+IFNULL(sale_share_size_qty, 0)) / CASE b.sale_all_qty WHEN 0 THEN NULL ELSE b.sale_all_qty END*100,2) as sale_sum_color_rate,

    ROUND( order_xxs_qty / CASE (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0)) WHEN 0 THEN NULL ELSE (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0)) END *100,2) order_xxs_rate,
    ROUND( order_xs_qty / CASE (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0)) WHEN 0 THEN NULL ELSE (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0)) END*100,2) order_xs_rate,
    ROUND( order_s_qty / CASE (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0)) WHEN 0 THEN NULL ELSE (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0)) END*100,2) order_s_rate,
    ROUND( order_m_qty / CASE (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0)) WHEN 0 THEN NULL ELSE (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0)) END*100,2) order_m_rate,
    ROUND( order_l_qty / CASE (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0)) WHEN 0 THEN NULL ELSE (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0)) END*100,2) order_l_rate,
    ROUND( order_xl_qty / CASE (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0)) WHEN 0 THEN NULL ELSE (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0)) END*100,2) order_xl_rate,
    ROUND( order_xxl_qty / CASE (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0)) WHEN 0 THEN NULL ELSE (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0)) END*100,2) order_xxl_rate,
    ROUND( order_xxxl_qty / CASE (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0)) WHEN 0 THEN NULL ELSE (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0)) END*100,2) order_xxxl_rate,
    ROUND( order_xxxxl_qty / CASE (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0)) WHEN 0 THEN NULL ELSE (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0)) END*100,2) order_xxxxl_rate,
    ROUND( order_share_size_qty / CASE (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0)) WHEN 0 THEN NULL ELSE (IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0)) END*100,2) order_share_size_rate,
    ROUND((IFNULL(order_xxs_qty, 0)+ IFNULL(order_xs_qty, 0)+IFNULL(order_s_qty, 0)+IFNULL(order_m_qty, 0)+IFNULL(order_l_qty, 0)+IFNULL(order_xl_qty, 0)+IFNULL(order_xxl_qty, 0)+IFNULL(order_xxxl_qty, 0)+IFNULL(order_xxxxl_qty, 0)+IFNULL(order_share_size_qty, 0)) / CASE b.order_all_qty WHEN 0 THEN NULL ELSE b.order_all_qty END *100,2) as order_sum_color_rate
     
        FROM hmcdata.bi_size_supply_new a
        LEFT JOIN (
        SELECT product_sn
        , sum(stock_xxs_qty) as stock_xxs_total
        , sum(stock_xs_qty) as stock_xs_total
        , sum(stock_s_qty) as stock_s_total
        , sum(stock_m_qty) as stock_m_total
        , sum(stock_l_qty) as stock_l_total
        , sum(stock_xl_qty) as stock_xl_total
        , sum(stock_xxl_qty) as stock_xxl_total
        , sum(stock_xxxl_qty) as stock_xxxl_total
        , sum(stock_xxxxl_qty) as stock_xxxxl_total
        , sum(stock_share_size_qty) as stock_share_size_total


        ,IFNULL(sum(stock_xxs_qty), 0)+ IFNULL(sum(stock_xs_qty), 0)+IFNULL(sum(stock_s_qty), 0)+IFNULL(sum(stock_m_qty), 0)+IFNULL(sum(stock_l_qty), 0)+IFNULL(sum(stock_xl_qty), 0)+IFNULL(sum(stock_xxl_qty), 0)+IFNULL(sum(stock_xxxl_qty), 0)+IFNULL(sum(stock_xxxxl_qty), 0)+IFNULL(sum(stock_share_size_qty), 0) as stock_all_qty

        ,SUM(sale_xxs_qty) as sale_xxs_total
        ,SUM(sale_xs_qty) as sale_xs_total
        ,SUM(sale_s_qty) as sale_s_total
        ,SUM(sale_m_qty) as sale_m_total
        ,SUM(sale_l_qty) as sale_l_total
        ,SUM(sale_xl_qty) as sale_xl_total
        ,SUM(sale_xxl_qty) as sale_xxl_total
        ,SUM(sale_xxxl_qty) as sale_xxxl_total
        ,SUM(sale_xxxxl_qty) as sale_xxxxl_total
        ,SUM(sale_share_size_qty) as sale_share_size_total

        ,IFNULL(sum(sale_xxs_qty), 0)+ IFNULL(sum(sale_xs_qty), 0)+IFNULL(sum(sale_s_qty), 0)+IFNULL(sum(sale_m_qty), 0)+IFNULL(sum(sale_l_qty), 0)+IFNULL(sum(sale_xl_qty), 0)+IFNULL(sum(sale_xxl_qty), 0)+IFNULL(sum(sale_xxxl_qty), 0)+IFNULL(sum(sale_xxxxl_qty), 0)+IFNULL(sum(sale_share_size_qty), 0) as sale_all_qty

        , sum(order_xxs_qty) as order_xxs_total
        , sum(order_xs_qty) as order_xs_total
        , sum(order_s_qty) as order_s_total
        , sum(order_m_qty) as order_m_total
        , sum(order_l_qty) as order_l_total
        , sum(order_xl_qty) as order_xl_total
        , sum(order_xxl_qty) as order_xxl_total
        , sum(order_xxxl_qty) as order_xxxl_total
        , sum(order_xxxxl_qty) as order_xxxxl_total
        , sum(order_share_size_qty) as order_share_size_total

        ,IFNULL(sum(order_xxs_qty), 0)+ IFNULL(sum(order_xs_qty), 0)+IFNULL(sum(order_s_qty), 0)+IFNULL(sum(order_m_qty), 0)+IFNULL(sum(order_l_qty), 0)+IFNULL(sum(order_xl_qty), 0)+IFNULL(sum(order_xxl_qty), 0)+IFNULL(sum(order_xxxl_qty), 0)+IFNULL(sum(order_xxxxl_qty), 0)+IFNULL(sum(order_share_size_qty), 0) as order_all_qty


        FROM hmcdata.bi_size_supply_new WHERE product_sn in( {p[:-1]}) GROUP BY product_sn) b
        ON a.product_sn=b.product_sn
        WHERE a.product_sn in( {p[:-1]})
        order by a.product_sn
        """

        # print(sql)
        ret = Scope['vertica'].execute(sql)
        # data = {'now': {}, 'last': {}}

        columns = ret.keys()
        data = []

        # print(list({prefix: len(group)} for prefix, group in itertools.groupby(sorted(map(lambda x: x.split('_')[0], columns)))) )

        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                elif val[i] is None:
                    pass
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)
        # compress_list = list(map(lambda x: True if x.split('_')[0] == 'sum' else False,data[0].keys()))
        # re_compress_list = list(map(lambda x: False if x.split('_')[0] == 'sum' else True,data[0].keys()))
        # sum_data = list(map(lambda x: list(itertools.compress(x.items(), compress_list)), data))
        # s_data = list(map(lambda x: list(itertools.compress(x.items(), re_compress_list)), data))
        # sum_data = map(lambda x: dict(x), sum_data)
        # s_data = map(lambda x: dict(x), s_data)

        # sum_data = list(sum_data)[0]
        # data = list(s_data)

        context['data'] = data
        # context['sum_data'] = sum_data
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_product_period', methods=['GET'], endpoint='get_product_period')
@__token_wrapper
def get_product_period(context):
    '''
    获取生产周期
    :param context:
    :return:
    '''
    try:
        channel = request.args.get('channel')
        category_class = request.args.get('category_class')
        develop_property = request.args.get('develop_property')
        doc_year = request.args.get('doc_year')
        category = request.args.get('category')

        if develop_property == 'all' or develop_property is None:
            dev_sql = ""
        else:
            dev_sql = f"""AND c.DevProperty = '{develop_property}'"""

        if category == 'all' or category is None:
            category_sql = ''
        else:
            category_sql = f"""AND b.product_category2='{category}'"""

        grade = {'春季': 'A', '夏季': 'B', '秋季': 'C', '冬季': 'D', '全季': 'E'}
        # {'fst_period': '首单周期', 'fab_period': '快返周期', 'crs_period': '跨季返单周期', 'prs_period': '当季返单周期',
        #  'fst_rate': '首单占比', 'fab_rate': '快返占比', 'crs_rate': '跨季占比', 'prs_rate': '当季占比'}
        sql = f'''SELECT ifnull(a.doc_season,ifnull(b.doc_season,ifnull(c.doc_season,d.doc_season))) as doc_season, a.period AS fst_period , b.period AS fab_period, IFNULL(c.period,0) AS crs_period, d.period AS prs_period
        , case (IFNULL(a.quantity, 0)+ IFNULL(b.quantity, 0)+IFNULL(c.quantity, 0)+IFNULL(d.quantity, 0)) when 0 then 0 else
        ROUND( IFNULL(a.quantity, 0) / (IFNULL(a.quantity, 0)+ IFNULL(b.quantity, 0)+IFNULL(c.quantity, 0)+IFNULL(d.quantity, 0)) * 100, 2) end AS fst_rate
        , case (IFNULL(a.quantity, 0)+ IFNULL(b.quantity, 0)+IFNULL(c.quantity, 0)+IFNULL(d.quantity, 0)) when 0 then 0 else
        ROUND( IFNULL(b.quantity, 0) / (IFNULL(a.quantity, 0)+ IFNULL(b.quantity, 0)+IFNULL(c.quantity, 0)+IFNULL(d.quantity, 0)) * 100, 2) end AS fab_rate
        , case (IFNULL(a.quantity, 0)+ IFNULL(b.quantity, 0)+IFNULL(c.quantity, 0)+IFNULL(d.quantity, 0)) when 0 then 0 else
        ROUND( IFNULL(c.quantity, 0) / (IFNULL(a.quantity, 0)+ IFNULL(b.quantity, 0)+IFNULL(c.quantity, 0)+IFNULL(d.quantity, 0)) * 100, 2) end AS crs_rate
        , case (IFNULL(a.quantity, 0)+ IFNULL(b.quantity, 0)+IFNULL(c.quantity, 0)+IFNULL(d.quantity, 0)) when 0 then 0 else
        ROUND( IFNULL(d.quantity, 0) / (IFNULL(a.quantity, 0)+ IFNULL(b.quantity, 0)+IFNULL(c.quantity, 0)+IFNULL(d.quantity, 0)) * 100, 2) end AS prs_rate
        FROM (
        SELECT IFNULL(a.Channel, '线上') Channel, a.doc_season, a.OrderType,
        AVG(IFNULL(date(a.Arrival90PercentDate), date(a.PurchaseOrderFinishDate)) - date(a.PlanPurchaseOrderApprovalDate)) period , SUM(TranTotal) quantity 
        FROM hmcdata.iom_scm_produce_schedule_info a LEFT JOIN hmcdata.iom_scm_product_list b ON a.product_sn=b.product_sn
        LEFT JOIN hmcdata.iom_scm_inman_product_label c ON a.product_sn=c.ProductSn
        WHERE b.CategoryClass = '{category_class}' AND IFNULL(a.Channel, '线上') = '{channel}' {category_sql}
        AND a.OrderType = '首单'  AND a.Status<>2 AND a.doc_year={doc_year}  {dev_sql}
        GROUP BY a.OrderType, a.doc_season, IFNULL(a.Channel, '线上')
        ) a 
        full JOIN (
        SELECT IFNULL(a.Channel, '线上') Channel, a.doc_season, a.OrderType,
        AVG(IFNULL(date(a.Arrival90PercentDate), date(a.PurchaseOrderFinishDate)) - date(a.PlanPurchaseOrderApprovalDate)) period, SUM(TranTotal) quantity
        FROM hmcdata.iom_scm_produce_schedule_info a LEFT JOIN hmcdata.iom_scm_product_list b ON a.product_sn=b.product_sn
        LEFT JOIN hmcdata.iom_scm_inman_product_label c ON a.product_sn=c.ProductSn
        WHERE b.CategoryClass = '{category_class}' AND IFNULL(a.Channel, '线上') = '{channel}' {category_sql}
        AND a.OrderType = '快速返单'  AND a.Status<>2  AND a.doc_year={doc_year} {dev_sql}
        GROUP BY a.OrderType, a.doc_season, IFNULL(a.Channel, '线上')
        ) b ON a.doc_season=b.doc_season
        full JOIN (
        SELECT IFNULL(a.Channel, '线上') Channel, a.doc_season, a.OrderType,
        AVG(IFNULL(date(a.Arrival90PercentDate), date(a.PurchaseOrderFinishDate)) - date(a.PlanPurchaseOrderApprovalDate)) period, SUM(TranTotal) quantity
        FROM hmcdata.iom_scm_produce_schedule_info a LEFT JOIN hmcdata.iom_scm_product_list b ON a.product_sn=b.product_sn
        LEFT JOIN hmcdata.iom_scm_inman_product_label c ON a.product_sn=c.ProductSn
        WHERE b.CategoryClass = '{category_class}' AND IFNULL(a.Channel, '线上') = '{channel}' {category_sql}
        AND a.OrderType = '跨季返单'  AND a.Status<>2  AND a.doc_year={doc_year} {dev_sql}
        GROUP BY a.OrderType, a.doc_season, IFNULL(a.Channel, '线上')
        ) c ON a.doc_season=c.doc_season
        full JOIN (
        SELECT IFNULL(a.Channel, '线上') Channel, a.doc_season, a.OrderType,
        AVG(IFNULL(date(a.Arrival90PercentDate), date(a.PurchaseOrderFinishDate)) - date(a.PlanPurchaseOrderApprovalDate)) period, SUM(TranTotal) quantity
        FROM hmcdata.iom_scm_produce_schedule_info a LEFT JOIN hmcdata.iom_scm_product_list b ON a.product_sn=b.product_sn
        LEFT JOIN hmcdata.iom_scm_inman_product_label c ON a.product_sn=c.ProductSn
        WHERE b.CategoryClass = '{category_class}' AND IFNULL(a.Channel, '线上') = '{channel}' {category_sql}
        AND a.OrderType = '当季返单'  AND a.Status<>2  AND a.doc_year={doc_year} {dev_sql}
        GROUP BY a.OrderType, a.doc_season, IFNULL(a.Channel, '线上')
        ) d ON a.doc_season=d.doc_season
        order by a.doc_season
        '''
        # print(sql)
        ret = Scope['vertica'].execute(sql)

        columns = ret.keys()
        data = []

        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                elif val[i] is None:
                    data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)
        data = list(sorted(data, key=lambda x: grade[x['doc_season']]))
        context['data'] = data
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_DevProperty', methods=['GET'], endpoint='get_DevProperty')
@__token_wrapper
def get_DevProperty(context):
    '''
    获取开发属性
    :param context:
    :return:
    '''
    try:

        sql = f'''
        SELECT DISTINCT DevProperty FROM hmcdata.iom_scm_inman_product_label
        '''
        ret = Scope['vertica'].execute(sql)
        data = []
        for DevProperty in ret:
            data.append(DevProperty[0])

        context['data'] = data
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_product_category', methods=['GET'], endpoint='get_product_category')
@__token_wrapper
def get_product_category(context):
    '''
    获取生产类目
    :param context:
    :return:
    '''
    try:
        channel = request.args.get('channel')
        category_class = request.args.get('categoryClass')
        develop_property = request.args.get('developProperty')
        season = request.args.get('season')
        # {'category': '类目', 'qty': '今年下单量', 'last_qty': '去年下单量', 'amt': '今年下单额', 'last_amt': '去年下单额',
        # 'cost': '今年成本', 'last_cost': '去年成本', 'same_cmp': '同比'}
        sql = f'''
        SELECT IFNULL( a.product_category2, c.product_category2) category , IFNULL( a.quantity, 0) qty, IFNULL( c.quantity, 0) last_qty
        , ROUND( IFNULL(a.quantity,0) * IFNULL(a.cost,0), 2) amt, ROUND( IFNULL(c.quantity,0) * IFNULL(c.cost,0), 2) last_amt
        , ROUND( IFNULL(a.cost, 0), 2) cost, ROUND( IFNULL(c.cost, 0), 2) last_cost
        , IFNULL( ROUND( (IFNULL( a.quantity, 0) - c.quantity) / c.quantity * 100, 2), 0) same_cmp  FROM
        (
        SELECT c.product_category2,SUM(a.NormalActualQuantity-IFNULL(b.actual_quantity,0)) as quantity, AVG(d.costamount) as cost FROM
        (
        SELECT ProduceOrderDocNum,product_sn,SUM(NormalActualQuantity) as NormalActualQuantity FROM hmcdata.iom_scm_Product_transfer_list
        WHERE ProduceOrderDocNum IN(
        SELECT ProduceDocNum FROM hmcdata.iom_scm_produce_schedule_info a
        LEFT JOIN hmcdata.iom_scm_inman_product_label b ON a.product_sn=b.ProductSn
        WHERE doc_year={datetime.datetime.now().year} AND doc_season = '{season}' AND IFNULL(Channel, '线上') = '{channel}'
        AND b.DevProperty = '{develop_property}' AND a.Status<>2  
        GROUP BY ProduceDocNum
        )
        GROUP BY ProduceOrderDocNum,product_sn
        )a
        LEFT JOIN
        (
        SELECT produce_order_doc_num,product_sn,SUM(actual_quantity) as actual_quantity FROM hmcdata.iom_scm_product_genuine_return_factory
        WHERE produce_order_doc_num IN(
        SELECT ProduceDocNum FROM hmcdata.iom_scm_produce_schedule_info a
        LEFT JOIN hmcdata.iom_scm_inman_product_label b ON a.product_sn=b.ProductSn
        WHERE doc_year={datetime.datetime.now().year} AND doc_season = '{season}' AND IFNULL(Channel, '线上') = '{channel}' 
        AND b.DevProperty = '{develop_property}' AND a.Status<>2  
        GROUP BY ProduceDocNum
        )
        GROUP BY produce_order_doc_num,product_sn
        )b ON a.ProduceOrderDocNum=b.produce_order_doc_num AND a.product_sn=b.product_sn 
        LEFT JOIN hmcdata.iom_scm_product_list c ON a.product_sn=c.product_sn
        LEFT JOIN hmcdata.bi_goods_cost d ON a.product_sn=d.goods_bn
        WHERE c.CategoryClass='{category_class}' 
        GROUP BY c.product_category2
        ) a
        FULL JOIN 
        (
        SELECT c.product_category2,SUM(a.NormalActualQuantity-IFNULL(b.actual_quantity,0)) as quantity, AVG(d.costamount) as cost FROM
        (
        SELECT ProduceOrderDocNum,product_sn,SUM(NormalActualQuantity) as NormalActualQuantity FROM hmcdata.iom_scm_Product_transfer_list
        WHERE ProduceOrderDocNum IN(
        SELECT ProduceDocNum FROM hmcdata.iom_scm_produce_schedule_info a
        LEFT JOIN hmcdata.iom_scm_inman_product_label b ON a.product_sn=b.ProductSn
        WHERE doc_year={(datetime.datetime.now() - dateutil.relativedelta.relativedelta(years=1)).year} AND doc_season = '{season}' AND IFNULL(Channel, '线上') = '{channel}'
        AND b.DevProperty = '{develop_property}' AND a.Status<>2  
        GROUP BY ProduceDocNum
        )
        GROUP BY ProduceOrderDocNum,product_sn
        )a
        LEFT JOIN
        (
        SELECT produce_order_doc_num,product_sn,SUM(actual_quantity) as actual_quantity FROM hmcdata.iom_scm_product_genuine_return_factory
        WHERE produce_order_doc_num IN(
        SELECT ProduceDocNum FROM hmcdata.iom_scm_produce_schedule_info a
        LEFT JOIN hmcdata.iom_scm_inman_product_label b ON a.product_sn=b.ProductSn
        WHERE doc_year={(datetime.datetime.now() - dateutil.relativedelta.relativedelta(years=1)).year} AND doc_season = '{season}' AND IFNULL(Channel, '线上') = '{channel}'
        AND b.DevProperty = '{develop_property}' AND a.Status<>2  
        GROUP BY ProduceDocNum
        )
        GROUP BY produce_order_doc_num,product_sn
        )b ON a.ProduceOrderDocNum=b.produce_order_doc_num AND a.product_sn=b.product_sn 
        LEFT JOIN hmcdata.iom_scm_product_list c ON a.product_sn=c.product_sn
        LEFT JOIN hmcdata.bi_goods_cost d ON a.product_sn=d.goods_bn
        WHERE c.CategoryClass='{category_class}'
        GROUP BY c.product_category2
        ) c ON a.product_category2=c.product_category2

        '''
        ret = Scope['vertica'].execute(sql)

        columns = ret.keys()
        data = []

        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                elif val[i] is None:
                    pass
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)

        # data = list(sorted(data, key=grade.__getitem__))
        context['data'] = data
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/yushou_shop_list', methods=['GET'], endpoint='yushou_shop_list')
@__token_wrapper
def yushou_shop_list(context):
    '''
    获取预售店铺列表
    :param context:
    :return:
    '''
    try:
        shop_id = context['data']['online_shop_id_list']
        if len(shop_id) == 0:
            context['data'] = []
            return jsonify(context)
        sql = f'''SELECT shop_name,dc_shop_id FROM bi_shop_saleinfo_goods_yushou 
        GROUP BY shop_name,dc_shop_id
        ORDER BY SUM(order_products) DESC;'''
        # print(sql)
        ret = Scope['bi_saas'].execute(sql)
        shop_datas = []
        for shop_name, dc_shop_id in ret:
            if str(dc_shop_id) in shop_id:
                shop_datas.append({"shop_name": shop_name, "dc_shop_id": dc_shop_id})

        context['data'] = shop_datas
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()


@good.route('/yushou_detail_total', methods=['GET'], endpoint='yushou_detail_total')
@__token_wrapper
def yushou_detail_total(context):
    '''
    获取预售店铺列表
    :param context:
    :return:
    '''
    dateStart = request.args.get('dateStart')
    dateEnd = request.args.get('dateEnd')
    dc_shop_id = request.args.get('dc_shop_id')

    try:

        sql = f'''SELECT count(*) as total,SUM(order_products) as order_products,SUM(front_money) as total_front_money,
        SUM(price_sale) as total_price_sale FROM(
        SELECT SUM(order_products) as order_products,SUM(front_money) as front_money,
        SUM(price_sale) as price_sale FROM bi_shop_saleinfo_goods_yushou yushou
        WHERE dc_shop_id={dc_shop_id} AND totalday>='{dateStart}' AND totalday<='{dateEnd}'
        GROUP BY yushou.product_sn
        )a;'''
        # print(sql)
        ret = Scope['bi_saas'].execute(sql)
        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                elif val[i] is None:
                    data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)

        context['data'] = data[0]
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()


@good.route('/yushou_detail', methods=['GET'], endpoint='yushou_detail')
@__token_wrapper
def yushou_detail(context):
    '''
    获取预售店铺列表
    :param context:
    :return:
    '''
    dateStart = request.args.get('dateStart')
    dateEnd = request.args.get('dateEnd')
    dc_shop_id = request.args.get('dc_shop_id')
    page_size = request.args.get('page_size')
    page = request.args.get('page')
    sort_field = request.args.get('sortField')
    order = request.args.get('order')
    sort_dict = {'quantity': 's.quantity', 'order_products': 'SUM(order_products)'}
    if order is None:
        order_by = "ORDER BY SUM(order_products) DESC"
    else:
        order_by = f''' ORDER BY {sort_dict[sort_field]} {'DESC' if order == 'descend' else 'ASC'} '''
    if page is None:
        page = 1
    if page_size is None:
        page_size = 20
    offset = f''' LIMIT {int(page_size)} OFFSET {(int(page) - 1) * int(page_size)} '''
    try:

        sql = f'''SELECT yushou.product_sn,MAX(title) as title,SUM(order_products) as order_products,SUM(front_money) as front_money,
        SUM(price_sale) as price_sale,IFNULL(img.img_src,MAX(yushou.img_src)) as img_src,product_category2,IFNULL(s.quantity,0) as quantity,MAX(num_iid) as num_iid,
        MAX(n.on_way_nums) as on_way_nums FROM bi_shop_saleinfo_goods_yushou yushou
        LEFT JOIN bi_product_img img ON yushou.product_sn=img.product_sn
        LEFT JOIN product_stock_now s ON yushou.product_sn=s.goods_sn
        LEFT JOIN product_onway_nums n ON yushou.product_sn=n.product_sn
        WHERE dc_shop_id={dc_shop_id} AND totalday>='{dateStart}' AND totalday<='{dateEnd}'
        GROUP BY yushou.product_sn,product_category2,s.quantity
        {order_by}
        {offset}
        ;'''
        # print(sql)
        ret = Scope['bi_saas'].execute(sql)
        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                elif val[i] is None:
                    data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)

        context['data'] = data
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()


@good.route('/get_yushou_tmall_url', methods=['GET'], endpoint='get_yushou_tmall_url')
@__token_download
def get_yushou_tmall_url(context):
    '''
    获取具体评论款号
    :param context:
    :return:
    '''
    try:
        product_sn = request.args.get('product_sn')

        sql = f'''SELECT auction_url FROM hmcdata.tb_sycm_shop_auction_detail WHERE auction_id='{product_sn}' AND auction_status<>'已下架'
ORDER BY st_date desc,selling_after_sales_succ_refund_cnt DESC,pv DESC LIMIT 1'''
        # print(sql)
        ret = Scope['vertica'].execute(sql)
        url = (ret.fetchone()[0])
        return redirect(url)

    finally:
        Scope['vertica'].remove()


@good.route('/shop_product_sale', methods=['GET'], endpoint='shop_product_sale')
@__token_wrapper
def shop_product_sale(context):
    '''
    获取款号销售曲线
    :param context:
    :return:
    '''
    dateStart = request.args.get('dateStart')
    dateEnd = request.args.get('dateEnd')
    dc_shop_id = request.args.get('dc_shop_id')
    product_sn = request.args.get('product_sn')

    try:

        sql = f'''SELECT SUM(order_products) as order_products,totalday FROM bi_shop_saleinfo_goods_yushou WHERE product_sn='{product_sn}' 
        AND totalday>='{dateStart}' AND totalday<='{dateEnd}'
        AND dc_shop_id={dc_shop_id}
        GROUP BY totalday
        ORDER BY totalday ASC;'''
        # print(sql)
        ret = Scope['bi_saas'].execute(sql)
        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                elif val[i] is None:
                    data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)

        context['data'] = data
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()


@good.route('/shop_product_uv', methods=['GET'], endpoint='shop_product_uv')
@__token_wrapper
def shop_product_uv(context):
    '''
    获取款号销售曲线
    :param context:
    :return:
    '''
    dateStart = request.args.get('dateStart')
    dateEnd = request.args.get('dateEnd')
    dc_shop_id = request.args.get('dc_shop_id')
    product_sn = request.args.get('product_sn')
    shop = {'1500014': '茵曼旗舰店', '1750007': '茵曼童装旗舰店', '1500007': '生活在左旗舰店', '2000003': '达丽坊服饰旗舰店', '1500013': 'pass旗舰店',
            '2000010': 'samyama旗舰店', '2000022': '水生之城', '2000015': '初语旗舰店', '1500006': '秋壳旗舰店',
            '1500012': '初语童装旗舰店', '2000004': '茵曼家具旗舰店', '1500005': '茵曼箱包旗舰店', '2000006': '华谊家具天猫旗舰店',
            '1750001': '茵曼女鞋旗舰店'}
    shop_name = shop[dc_shop_id]
    try:

        sql = f'''SELECT SUM(uv) as uv,SUM(collect_goods_buyer_cnt) as collect_goods_buyer_cnt,SUM(add_cart_auction_cnt) as add_cart_auction_cnt,st_date FROM hmcdata.tb_sycm_shop_auction_detail 
        WHERE shop_name='{shop_name}' AND st_date>='{dateStart}' AND st_date<='{dateEnd}'
        AND outer_goods_id='{product_sn}'
        GROUP BY st_date
        ORDER BY st_date ASC;'''
        # print(sql)
        ret = Scope['vertica'].execute(sql)
        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                elif val[i] is None:
                    data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)

        context['data'] = data
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/front_cat_spread', methods=['GET'], endpoint='front_cat_spread')
@__token_wrapper
def front_cat_spread(context):
    '''
    获取类目分布
    :param context:
    :return:
    '''
    dateStart = request.args.get('dateStart')
    dateEnd = request.args.get('dateEnd')
    dc_shop_id = request.args.get('dc_shop_id')

    try:

        sql = f'''SELECT SUM(order_products) as order_products,product_category2 FROM bi_shop_saleinfo_goods_yushou yushou
        WHERE dc_shop_id={dc_shop_id} AND totalday>='{dateStart}' AND totalday<='{dateEnd}'
        GROUP BY product_category2
        ORDER BY SUM(order_products) DESC;'''
        # print(sql)
        ret = Scope['bi_saas'].execute(sql)
        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                elif val[i] is None:
                    data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)

        context['data'] = data
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()


@good.route('/front_province_spread', methods=['GET'], endpoint='front_province_spread')
@__token_wrapper
def front_province_spread(context):
    '''
    获取省份分布
    :param context:
    :return:
    '''
    dateStart = request.args.get('dateStart')
    dateEnd = request.args.get('dateEnd')
    dc_shop_id = request.args.get('dc_shop_id')

    try:
        sql = f'''SELECT SUM(t.num) as value,t.hm_province as name FROM  hmcdata.e3_taobao_trade t 
        LEFT JOIN hmcdata.dc_shop sh ON t.shop_id=sh.outer_shop_id AND sh.src_business_id=t.src_business_id AND sh.platform_id=1
        WHERE t.created>='{dateStart} 00:00:00' AND  t.created<='{dateEnd} 23:59:59' and t.step_trade_status='FRONT_PAID_FINAL_NOPAID' AND t.status<>'TRADE_CLOSED' AND t.is_tran_success<>1
        AND sh.id={dc_shop_id} AND t.hm_province is not NULL
        GROUP BY t.hm_province
        order by SUM(t.num) desc
        ;'''
        # print(sql)
        ret = Scope['vertica'].execute(sql)
        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                elif val[i] is None:
                    data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)

        context['data'] = data
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/product_collocation', methods=['GET'], endpoint='product_collocation')
@__token_wrapper
def product_collocation(context):
    '''
    获取线下搭配
    :param context:
    :return:
    '''
    product_sn = request.args.get('product_sn')
    product_season = request.args.get('product_season')
    dateStart = request.args.get('dateStart')
    dateEnd = request.args.get('dateEnd')
    season_dict = {'春季': 'd.spring_zone', '夏季': 'd.summer_zone', '秋季': 'd.autumn_zone', '冬季': 'd.winter_zone'}
    # print(dateStart)
    total_sql = f"""SELECT COUNT(*) FROM hmcdata.product_collocation where product_sn='{product_sn}'"""
    ret_total = Scope['vertica'].execute(total_sql)
    count = ret_total.fetchone()[0]
    print(count)
    if count == 0:
        context['data'] = []
        return jsonify(context)
    if dateStart is None or dateStart == '':
        dateSql = ""
    else:
        dateSql = f"""AND c.pay_time>='{dateStart} 00:00:00' AND c.pay_time<='{dateEnd} 23:59:59'"""

    try:
        sql = f'''  SELECT a.product_sn,a.quantity,a.zone_name,a.img_src FROM(
            SELECT c.product_sn,SUM(quantity) as quantity,MAX(i.img_src) as img_src,{season_dict[product_season]} as zone_name FROM hmcdata.product_collocation c
            LEFT JOIN hmcdata.bi_product_img i ON c.product_sn=i.product_sn
            LEFT JOIN hmcdata.bi_shop_district_area d ON c.shop_code=d.shop_code
            WHERE CONCAT(user_id,CONCAT('_',DATE(pay_time))) IN(
            SELECT  CONCAT(user_id,CONCAT('_',DATE(pay_time))) FROM hmcdata.product_collocation WHERE product_sn='{product_sn}'
            ) AND c.product_sn<>'{product_sn}'  AND {season_dict[product_season]}='1区' {dateSql}
            GROUP BY c.product_sn,zone_name,{season_dict[product_season]}
            ORDER BY SUM(quantity) DESC
            LIMIT 10
            )a
            UNION ALL
            SELECT a.product_sn,a.quantity,a.zone_name,a.img_src FROM(
            SELECT c.product_sn,SUM(quantity) as quantity,MAX(i.img_src) as img_src,{season_dict[product_season]} as zone_name FROM hmcdata.product_collocation c
            LEFT JOIN hmcdata.bi_product_img i ON c.product_sn=i.product_sn
            LEFT JOIN hmcdata.bi_shop_district_area d ON c.shop_code=d.shop_code
            WHERE CONCAT(user_id,CONCAT('_',DATE(pay_time))) IN(
            SELECT  CONCAT(user_id,CONCAT('_',DATE(pay_time))) FROM hmcdata.product_collocation WHERE product_sn='{product_sn}'
            ) AND c.product_sn<>'{product_sn}'  AND {season_dict[product_season]}='2区' {dateSql}
            GROUP BY c.product_sn,{season_dict[product_season]}
            ORDER BY SUM(quantity) DESC
            LIMIT 10
            )a
            UNION ALL
            SELECT a.product_sn,a.quantity,a.zone_name,a.img_src FROM(
            SELECT c.product_sn,SUM(quantity) as quantity,MAX(i.img_src) as img_src,{season_dict[product_season]} as zone_name FROM hmcdata.product_collocation c
            LEFT JOIN hmcdata.bi_product_img i ON c.product_sn=i.product_sn
            LEFT JOIN hmcdata.bi_shop_district_area d ON c.shop_code=d.shop_code
            WHERE CONCAT(user_id,CONCAT('_',DATE(pay_time))) IN(
            SELECT  CONCAT(user_id,CONCAT('_',DATE(pay_time))) FROM hmcdata.product_collocation WHERE product_sn='{product_sn}'
            ) AND c.product_sn<>'{product_sn}'  AND {season_dict[product_season]}='3区' {dateSql}
            GROUP BY c.product_sn,{season_dict[product_season]}
            ORDER BY SUM(quantity) DESC
            LIMIT 10
            )a
            UNION ALL
            SELECT a.product_sn,a.quantity,a.zone_name,a.img_src FROM(
            SELECT c.product_sn,SUM(quantity) as quantity,MAX(i.img_src) as img_src,{season_dict[product_season]} as zone_name FROM hmcdata.product_collocation c
            LEFT JOIN hmcdata.bi_product_img i ON c.product_sn=i.product_sn
            LEFT JOIN hmcdata.bi_shop_district_area d ON c.shop_code=d.shop_code
            WHERE CONCAT(user_id,CONCAT('_',DATE(pay_time))) IN(
            SELECT  CONCAT(user_id,CONCAT('_',DATE(pay_time))) FROM hmcdata.product_collocation WHERE product_sn='{product_sn}'
            ) AND c.product_sn<>'{product_sn}'  AND {season_dict[product_season]}='4区' {dateSql}
            GROUP BY c.product_sn,{season_dict[product_season]}
            ORDER BY SUM(quantity) DESC
            LIMIT 10
            )a
            UNION ALL
            SELECT a.product_sn,a.quantity,a.zone_name,a.img_src FROM(
            SELECT c.product_sn,SUM(quantity) as quantity,MAX(i.img_src) as img_src,{season_dict[product_season]} as zone_name FROM hmcdata.product_collocation c
            LEFT JOIN hmcdata.bi_product_img i ON c.product_sn=i.product_sn
            LEFT JOIN hmcdata.bi_shop_district_area d ON c.shop_code=d.shop_code
            WHERE CONCAT(user_id,CONCAT('_',DATE(pay_time))) IN(
            SELECT  CONCAT(user_id,CONCAT('_',DATE(pay_time))) FROM hmcdata.product_collocation WHERE product_sn='{product_sn}'
            ) AND c.product_sn<>'{product_sn}'  AND {season_dict[product_season]}='5区' {dateSql}
            GROUP BY c.product_sn,{season_dict[product_season]}
            ORDER BY SUM(quantity) DESC
            LIMIT 10
            )a'''
        # print(sql)
        ret = Scope['vertica'].execute(sql)
        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                elif val[i] is None:
                    data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)

        context['data'] = data
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_product_season', methods=['GET'], endpoint='get_product_season')
@__token_wrapper
def get_product_season(context):
    '''
    获取款号季节和图片地址
    :param context:
    :return:
    '''
    product_sn = request.args.get('product_sn')

    try:
        sql = f'''  SELECT l.product_sn,l.product_season,i.img_src FROM hmcdata.iom_scm_product_list l 
                    LEFT JOIN hmcdata.bi_product_img i ON l.product_sn=i.product_sn
                    WHERE l.product_sn='{product_sn}' '''
        # print(sql)
        ret = Scope['vertica'].execute(sql)
        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                elif val[i] is None:
                    data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)

        context['data'] = data[0]
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_shop_district', methods=['GET'], endpoint='get_shop_district')
@__token_wrapper
def get_shop_district(context):
    '''
    获取地区分布
    :param context:
    :return:
    '''

    try:
        sql = ''' SELECT zone,spring,summer,autumn,winter FROM hmcdata.bi_shop_district_area_summary ORDER BY "zone" ASC '''
        # print(sql)
        ret = Scope['vertica'].execute(sql)
        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                elif val[i] is None:
                    data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)

        context['data'] = data
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/product_collocation_online', methods=['GET'], endpoint='product_collocation_online')
@__token_wrapper
def product_collocation_online(context):
    '''
    获取线上搭配
    :param context:
    :return:
    '''
    product_sn = request.args.get('product_sn')
    dateStart = request.args.get('dateStart')
    dateEnd = request.args.get('dateEnd')
    if dateStart is None or dateStart == '':
        dateSql = ""
    else:
        dateSql = f"""AND c.pay_time>='{dateStart}' AND c.pay_time<='{dateEnd}'"""

    try:
        sql = f''' SELECT c.product_sn,SUM(quantity) as quantity,MAX(i.img_src) as img_src FROM hmcdata.product_collocation_online c
                LEFT JOIN hmcdata.bi_product_img i ON c.product_sn=i.product_sn
                WHERE CONCAT(user_id,CONCAT('_',pay_time)) IN(
                    SELECT CONCAT(user_id,CONCAT('_',pay_time)) FROM hmcdata.product_collocation_online WHERE product_sn='{product_sn}'
                ) AND c.product_sn<>'{product_sn}'  {dateSql}
                GROUP BY c.product_sn
                ORDER BY SUM(quantity) DESC
                LIMIT 10 '''
        # print(sql)
        ret = Scope['vertica'].execute(sql)
        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                elif val[i] is None:
                    data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)

        context['data'] = data
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_product_DevProperty', methods=['GET'], endpoint='get_product_devProperty')
@__token_wrapper
def get_product_devProperty(context):
    '''
    获取开发属性
    :param context:
    :return:
    '''

    try:
        sql = f'''
            SELECT DISTINCT devProperty FROM hmcdata.iom_scm_inman_product_label
            '''
        ret = Scope['vertica'].execute(sql)
        data = []
        for val in ret:
            data.append(val[0])

        context['data'] = data
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_product_reorder_recommendation_CategoryClass', methods=['GET'],
            endpoint='get_product_reorder_recommendation_CategoryClass')
@__token_wrapper
def get_product_reorder_recommendation_CategoryClass(context):
    try:
        clazz_ids = context['data']['bi_business_clazz_id_list']
        brand_list = []
        clazz_ids = ','.join(clazz_ids)
        sql = f"""select distinct categoryClass from bi_business_brand_new n where business_id in({clazz_ids}) and status=1
        AND exists(
            select 1 from product_reorder_recommendation r where n.categoryClass=r.category_class
        )
        order by sort desc;"""
        # sql = f"""SELECT CategoryClass FROM hmcdata.product_reorder_recommendation_cal_CategoryClass order by sorter ASC;"""
        ret = Scope['bi_saas'].execute(sql)
        columns = ret.keys()
        data = []
        for cat in ret:
            data.append(cat[0])
        context['data'] = data
        return jsonify(context)

    finally:
        Scope['bi_saas'].remove()


@good.route('/get_product_reorder_recommendation', methods=['GET'], endpoint='get_product_reorder_recommendation')
@__token_wrapper
def get_product_reorder_recommendation(context):
    '''
    获取返单明细
    :param context:
    :return:
    '''
    doc_year = request.args.get('doc_year')
    doc_season = request.args.get('doc_season')
    dev_prop = request.args.get('dev_prop')
    page = request.args.get('page')
    page_size = request.args.get('pageSize')
    dateStart = request.args.get('dateStart')
    dateEnd = request.args.get('dateEnd')
    product_sn = request.args.get('product_sn')
    cat = request.args.get('cat')
    category_class = request.args.get('category_class')
    goods_level = request.args.get('goods_level')
    product_level = request.args.get('product_level')
    tag = request.args.get('tag')
    data_product_tag = request.args.get('data_product_tag')
    if tag is not None:
        if tag == '1':
            tag_sql = f"""AND r.goods_level='{goods_level}' AND r.product_level='{product_level}'  """
        elif tag == '2':
            tag_sql = f"""AND r.product_tag='{data_product_tag}'  """
    else:
        tag_sql = ""
    if category_class == '茵曼服装':
        main_cat_sql = "AND is_main_cat=1"
    elif category_class is None or category_class == "":
        context['data'] = []
        return jsonify(context)
    else:
        main_cat_sql = ""

    if page_size is None:
        page_size = 10
    offset = f''' LIMIT {int(page_size)} OFFSET {(int(page) - 1) * int(page_size)} '''
    if dev_prop == 'all':
        sql_sub = ""
    else:
        sql_sub = f"""AND dev_prop='{dev_prop}'"""
    if dateStart is None or dateStart == "":
        date_sql = ""
    else:
        date_sql = f"""AND up_to_date>='{dateStart}' AND up_to_date<='{dateEnd}' """
    if cat is None or cat == 'all':
        cat_sql = ""
    else:
        cat_sql = f"""AND goods_catname_L2='{cat}' """

    try:
        if product_sn is None:
            sql = f'''SELECT SQL_CALC_FOUND_ROWS category_class, up_to_date, r.product_sn,r.color,i.img_src,old_product_sn, doc_year, doc_season,goods_catname_L2,first_order_nums,cost, fixed_price,
                gross_profit_of_fixed_price, conversion_rate, last_last_week_sales, last_last_week_sales_amount, last_last_week_discount, 
                last_last_week_gross_profit, last_week_sales, last_week_sales_amount, last_week_discount, last_week_gross_profit, total_sales_nums, total_sales_amount, 
                total_discount, total_gross_profit, sold_out_rate, in_stock, on_way_nums, total_in_stock,product_rank, color_rank, stock_product_rank, stock_color_rank,
                turnover, sales_forecast,check_sales,suggest_nums, last_two_week_sales_nums,reorder_min_nums, reorder_times, reorder_interval, reorder_total, dev_prop,
                product_level, goods_level,product_tag,check_sales,product_season_prop,last_7days_conversion_rate,total_return,DATE(r.gmt_created) as gmt_created
                FROM hmcdata.product_reorder_recommendation r
                LEFT JOIN hmcdata.bi_product_img i ON r.product_sn=i.product_sn  AND i.relate_type=1
                WHERE  doc_year={doc_year}  and r.category_class='{category_class}' {tag_sql}
                AND doc_season='{doc_season}' {sql_sub} {date_sql} {cat_sql}  ORDER BY suggest_nums DESC,total_sales_nums DESC {offset} '''
        else:
            sql = f'''SELECT category_class, up_to_date, r.product_sn,r.color,i.img_src,old_product_sn, doc_year, doc_season,goods_catname_L2,first_order_nums,cost, fixed_price,
                    gross_profit_of_fixed_price, conversion_rate, last_last_week_sales, last_last_week_sales_amount, last_last_week_discount, 
                    last_last_week_gross_profit, last_week_sales, last_week_sales_amount, last_week_discount, last_week_gross_profit, total_sales_nums, total_sales_amount, 
                    total_discount, total_gross_profit, sold_out_rate, in_stock, on_way_nums, total_in_stock,product_rank, color_rank, stock_product_rank, stock_color_rank,
                    turnover, sales_forecast,check_sales,suggest_nums, last_two_week_sales_nums,reorder_min_nums, reorder_times, reorder_interval, reorder_total, dev_prop,
                    product_level, goods_level,product_tag,check_sales,product_season_prop,last_7days_conversion_rate,total_return,DATE(r.gmt_created) as gmt_created
                    FROM hmcdata.product_reorder_recommendation r
                    LEFT JOIN hmcdata.bi_product_img i ON r.product_sn=i.product_sn  AND i.relate_type=1
                    WHERE  r.product_sn='{product_sn}'  and r.category_class='{category_class}' {tag_sql} '''
        # print(sql)
        ret = Scope['bi_saas'].execute(sql)

        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                # elif val[i] is None:
                #     data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)

        context['data'] = data
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()


# 商品总数
@good.route('/get_product_reorder_recommendation_total', methods=['GET'],
            endpoint='get_product_reorder_recommendation_total')
@__token_wrapper
def get_product_reorder_recommendation_total(context):
    try:
        doc_year = request.args.get('doc_year')
        doc_season = request.args.get('doc_season')
        dev_prop = request.args.get('dev_prop')
        dateStart = request.args.get('dateStart')
        dateEnd = request.args.get('dateEnd')
        category_class = request.args.get('category_class')
        cat = request.args.get('cat')
        goods_level = request.args.get('goods_level')
        product_level = request.args.get('product_level')
        tag = request.args.get('tag')
        data_product_tag = request.args.get('data_product_tag')
        # print(tag=='1')
        if tag is not None:
            if tag == '1':
                tag_sql = f"""AND goods_level='{goods_level}' AND product_level='{product_level}'  """
            elif tag == '2':
                tag_sql = f"""AND product_tag='{data_product_tag}'  """
        else:
            tag_sql = ""
        if category_class == '茵曼服装':
            main_cat_sql = "AND is_main_cat=1"
        elif category_class is None or category_class == "":
            context['data'] = []
            return jsonify(context)
        else:
            main_cat_sql = ""
        if dev_prop == 'all':
            sql_sub = ""
        else:
            sql_sub = f"""AND dev_prop='{dev_prop}'"""
        if dateStart is None or dateStart == "":
            date_sql = ""
        else:
            date_sql = f"""AND up_to_date>='{dateStart}' AND up_to_date<='{dateEnd}' """
        if cat == 'all' or cat is None:
            cat_sub = ""
        else:
            cat_sub = f"""AND goods_catname_L2='{cat}' """

        sql = f"""
        SELECT count(*) as total from(
        SELECT 1 FROM hmcdata.product_reorder_recommendation WHERE  doc_year={doc_year} 
            AND doc_season='{doc_season}' {date_sql} {sql_sub} AND category_class='{category_class}' {cat_sub} {tag_sql}
        )a
        """
        # print(sql)
        ret = Scope['bi_saas'].execute(sql)
        # data = {'now': {}, 'last': {}}

        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                elif val[i] is None:
                    data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)

        context['data'] = data[0]
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()


@good.route('/get_product_reorder_recommendation_overview', methods=['GET'],
            endpoint='get_product_reorder_recommendation_overview')
@__token_wrapper
def get_product_reorder_recommendation_overview(context):
    """
    返单总览
    @param context:
    @return:
    """
    try:
        doc_year = request.args.get('doc_year')
        doc_season = request.args.get('doc_season')
        dev_prop = request.args.get('dev_prop')
        dateStart = request.args.get('dateStart')
        dateEnd = request.args.get('dateEnd')
        category_class = request.args.get('category_class')
        cat = request.args.get('cat')
        product_sn = request.args.get('product_sn')
        if category_class == '茵曼服装':
            main_cat_sql = "AND is_main_cat=1"
        elif category_class is None or category_class == "":
            context['data'] = []
            return jsonify(context)
        else:
            main_cat_sql = ""
        if dev_prop == 'all':
            sql_sub = ""
        else:
            sql_sub = f"""AND dev_prop='{dev_prop}'"""
        if dateStart is None or dateStart == "":
            date_sql = ""
        else:
            date_sql = f"""AND up_to_date>='{dateStart}' AND up_to_date<='{dateEnd}' """
        if cat == 'all' or cat is None:
            cat_sub = ""
        else:
            cat_sub = f"""AND goods_catname_L2='{cat}' """

        if product_sn is None:
            sql = f"""
                SELECT product_tag,product_level,goods_level,COUNT(DISTINCT product_sn) as product_count,sum(total_sales_nums) as total_sales_nums,sum(total_sales_amount) as total_sales_amount,
                sum(total_sales_amount)/sum(total_sales_nums) as price_per,(sum(total_sales_amount)-sum(cost*total_sales_nums))/sum(total_sales_amount) as gross_profit,sum(total_in_stock) as total_in_stock,
                sum(IFNULL(total_sales_nums,0)-total_return)/sum(total_in_stock+IFNULL(total_sales_nums,0)-total_return) as sold_out_rate ,sum(cost*total_sales_nums) as total_cost,sum(total_return) as total_return
                FROM  hmcdata.product_reorder_recommendation prr  WHERE  doc_year ={doc_year} and doc_season ='{doc_season}' {date_sql} {cat_sub}  {sql_sub}
                and total_sales_nums >0 and category_class ='{category_class}' and product_tag is not NULL 
                GROUP BY product_tag,product_level,goods_level
            """
        else:
            sql = f"""
                    SELECT product_tag,product_level,goods_level,COUNT(DISTINCT product_sn) as product_count,sum(total_sales_nums) as total_sales_nums,sum(total_sales_amount) as total_sales_amount,
                    sum(total_sales_amount)/sum(total_sales_nums) as price_per,(sum(total_sales_amount)-sum(cost*total_sales_nums))/sum(total_sales_amount) as gross_profit,sum(total_in_stock) as total_in_stock,
                    sum(IFNULL(total_sales_nums,0)-total_return)/sum(total_in_stock+IFNULL(total_sales_nums,0)-total_return) as sold_out_rate ,sum(cost*total_sales_nums) as total_cost,sum(total_return) as total_return
                    FROM  hmcdata.product_reorder_recommendation prr  WHERE  category_class ='{category_class}' and product_tag is not NULL  and product_sn='{product_sn}'
                    GROUP BY product_tag,product_level,goods_level
                """
        # print(sql)
        ret = Scope['bi_saas'].execute(sql)

        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                elif val[i] is None:
                    data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)

        context['data'] = data
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()


@good.route('/get_product_CatProperty', methods=['GET'], endpoint='get_product_CatProperty')
@__token_wrapper
def get_product_CatProperty(context):
    '''
    获取开发属性
    :param context:
    :return:
    '''
    category_class = request.args.get('category_class')
    try:
        sql = f'''
             SELECT DISTINCT goods_catname_L2  FROM hmcdata.product_reorder_recommendation WHERE category_class ='{category_class}'
            '''
        ret = Scope['bi_saas'].execute(sql)
        data = []
        for val in ret:
            data.append(val[0])

        context['data'] = data
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()


@good.route('/export_reorder_recommendation', methods=['GET'], endpoint='export_reorder_recommendation')
@__token_download
def export_reorder_recommendation(context):
    '''
    导出
    :param context:
    :return:
    '''
    doc_year = request.args.get('doc_year')
    doc_season = request.args.get('doc_season')
    dev_prop = request.args.get('dev_prop')
    page = request.args.get('page')
    page_size = request.args.get('pageSize')
    dateStart = request.args.get('dateStart')
    dateEnd = request.args.get('dateEnd')
    product_sn = request.args.get('product_sn')
    cat = request.args.get('cat')
    category_class = request.args.get('category_class')

    if page_size is None:
        page_size = 10
    offset = f''' LIMIT {int(page_size)} OFFSET {(int(page) - 1) * int(page_size)} '''
    if dev_prop == 'all':
        sql_sub = ""
    else:
        sql_sub = f"""AND dev_prop='{dev_prop}'"""
    if dateStart is None or dateStart == "":
        date_sql = ""
    else:
        date_sql = f"""AND up_to_date>='{dateStart}' AND up_to_date<='{dateEnd}' """
    if cat is None or cat == 'all':
        cat_sql = ""
    else:
        cat_sql = f"""AND goods_catname_L2='{cat}' """

    if category_class == '茵曼服装':
        main_cat_sql = "AND is_main_cat=1"
    elif category_class is None or category_class == "":
        context['data'] = []
        return jsonify(context)
    else:
        main_cat_sql = ""

    try:
        if product_sn is None:
            sql = f'''SELECT category_class as '品类', up_to_date as '上新日期', r.product_sn as '款号',r.color as '颜色',i.img_src as '图片',old_product_sn as '旧款号', doc_year as '年份', doc_season as '季节',goods_catname_L2 as '类目',first_order_nums as '首单量',cost as '成本', fixed_price as '一口价',
                    gross_profit_of_fixed_price as '定价毛利', conversion_rate as '近30天转化率', last_last_week_sales as '最近前一周销量', last_last_week_sales_amount as '最近前一周销售额', last_last_week_discount as '最近前一周折扣', 
                    last_last_week_gross_profit as '最近前一周毛利', last_week_sales as '最近一周销售', last_week_sales_amount as '最近一周销额', last_week_discount as '最近一周折扣', last_week_gross_profit as '最近一周毛利', total_sales_nums as '累计销量', total_sales_amount as '累计销售额', total_return as '累计退货',
                    total_discount as '累计折扣', total_gross_profit as '累计毛利', sold_out_rate as '售罄率', in_stock as '在库库存', on_way_nums as '在途库存', total_in_stock as '总库存',product_rank as '累计销售品类单款排名', color_rank as '累计销售品类单色排名', stock_product_rank as '库存品类单款排名', stock_color_rank as '库存品类单色排名',
                    turnover as '周转', sales_forecast as '季末预计销量',check_sales as '往年类目款最高销量',suggest_nums as '建议返单数', last_two_week_sales_nums as '近两周销售数',reorder_min_nums as '返单起订量', reorder_times as '返单次数', reorder_interval as '平均返单时间', reorder_total as '累计返单量', dev_prop as '开发属性',
                    product_level as '款式等级', goods_level as '商品等级',product_tag as '销售标签',product_season_prop as '销售季节',last_7days_conversion_rate as '最近7天转换率',dsr
                    FROM hmcdata.product_reorder_recommendation r
                    LEFT JOIN hmcdata.bi_product_img i ON r.product_sn=i.product_sn  AND i.relate_type=1
                    WHERE  doc_year={doc_year}  and category_class='{category_class}'
                    AND doc_season='{doc_season}' {sql_sub} {date_sql} {cat_sql}  ORDER BY suggest_nums DESC,total_sales_nums DESC '''
        else:
            sql = f'''SELECT category_class, up_to_date, r.product_sn,r.color,i.img_src,old_product_sn, doc_year, doc_season,goods_catname_L2,first_order_nums,cost, fixed_price,
                        gross_profit_of_fixed_price, conversion_rate, last_last_week_sales, last_last_week_sales_amount, last_last_week_discount, 
                        last_last_week_gross_profit, last_week_sales, last_week_sales_amount, last_week_discount, last_week_gross_profit, total_sales_nums, total_sales_amount, 
                        total_discount, total_gross_profit, sold_out_rate, in_stock, on_way_nums, total_in_stock,product_rank, color_rank, stock_product_rank, stock_color_rank,
                        turnover, sales_forecast,check_sales,suggest_nums, last_two_week_sales_nums,reorder_min_nums, reorder_times, reorder_interval, reorder_total, dev_prop,
                        product_level, goods_level,product_tag,check_sales,product_season_prop,last_7days_conversion_rate
                        FROM hmcdata.product_reorder_recommendation r
                        LEFT JOIN hmcdata.bi_product_img i ON r.product_sn=i.product_sn  AND i.relate_type=1
                        WHERE  r.product_sn='{product_sn}'  and r.category_class='{category_class}' '''
        # print(sql)

        ret = Scope['bi_saas'].execute(sql)
        columns_names = ret.keys()
        # return excel.make_response_from_query_sets(ret.fetchall(), column_names=columns_names, file_type='xlsx',
        #                                            file_name="商品返单建议")
        return export_file(columns_names, ret, f"""商品返单建议{datetime.datetime.now().strftime('%Y-%m-%d %H_%M_%S')}""")

    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()


@good.route('/get_stock_day_report', methods=['GET'], endpoint='get_stock_day_report')
@__token_wrapper
def get_stock_day_report(context):
    '''
    获取每日库存报表明细
    :param context:
    :return:
    '''
    category_class = request.args.get('CategoryClass')
    page = request.args.get('page')
    page_size = request.args.get('pageSize')
    product_sn = request.args.get('product_sn')
    if product_sn is None or product_sn == "":
        product_sql = ""
    else:
        product_sql = f"""AND product_sn='{product_sn}' """
    if page_size is None:
        page_size = 10
    offset = f''' LIMIT {int(page_size)} OFFSET {(int(page) - 1) * int(page_size)} '''

    try:
        sql = f"""
            SELECT product_sn, design_product_sn, color, CategoryClass, up_to_date, product_category2, product_year, product_season, dev_prop, old_product_sn, nature,
            sell_tag, doc_year, doc_season, iom_cost, cost, fixed_price, first_order_nums_on, reorder_nums_on, plan_total_on, first_order_nums_off, reorder_nums_off, plan_total_off, 
            produced_nums_on, production_onway_nums_on, produced_nums_off, production_onway_nums_off, production_onway_nums, o2o_return_onway, o2o_stock_in_shop, o2o_stock_toshop_onway,
            o2o_stock_in_warehouse, online_zongcang_stock, online_channel_stock, online_outof_stock, online_back_stock, 
            online_defective_stock, total_stock, online_stock, offline_stock, tran_total, gmt_created,
            online_statistics_stock, offline_statistics_stock,
            plan_total_on*ifnull(cost,iom_cost) as plan_total_on_amount,plan_total_off*ifnull(cost,iom_cost) as plan_total_off_amount,
            tran_total*ifnull(cost,iom_cost) as produced_nums_amount,production_onway_nums*ifnull(cost,iom_cost) as production_onway_nums_amount,
            o2o_stock_in_shop*ifnull(cost,iom_cost) as o2o_stock_in_shop_amount,o2o_stock_toshop_onway*ifnull(cost,iom_cost) as o2o_stock_toshop_onway_amount,
            o2o_return_onway*ifnull(cost,iom_cost) as o2o_return_onway_amount,
            o2o_stock_in_warehouse*ifnull(cost,iom_cost) as o2o_stock_in_warehouse_amount,offline_stock*ifnull(cost,iom_cost) as o2o_total_stock_amount,
            ifnull(online_zongcang_stock,0)+ifnull(online_channel_stock,0) as online_total_stock_warehouse,
            online_stock*ifnull(cost,iom_cost) as online_stock_amount,online_back_stock*ifnull(cost,iom_cost) as online_back_stock_amount,
            online_defective_stock*ifnull(cost,iom_cost) as online_defective_stock_amount,total_stock * ifnull(cost,iom_cost) as  total_stock_amount,
            online_statistics_stock*ifnull(cost,iom_cost) as online_statistics_stock_amount,
            offline_statistics_stock*ifnull(cost,iom_cost) as offline_statistics_stock_amount
            FROM hmcdata.bi_goods_stock_day_report WHERE CategoryClass='{category_class}' {product_sql} ORDER BY doc_year DESC,up_to_date DESC,product_sn DESC {offset};
        """
        print(sql)
        ret = Scope['bi_saas'].execute(sql)
        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                # elif val[i] is None:
                #     data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)

        context['data'] = data
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()


@good.route('/get_stock_day_report_total', methods=['GET'], endpoint='get_stock_day_report_total')
@__token_wrapper
def get_stock_day_report_total(context):
    '''
    获取每日库存报表明细
    :param context:
    :return:
    '''
    category_class = request.args.get('CategoryClass')
    product_sn = request.args.get('product_sn')
    if product_sn is None or product_sn == "":
        product_sql = ""
    else:
        product_sql = f"""AND product_sn='{product_sn}' """
    try:
        sql = f"""
                SELECT count(*) as total from(
            SELECT 1
            FROM hmcdata.bi_goods_stock_day_report WHERE CategoryClass='{category_class}'  {product_sql}
            )a;
        """
        ret = Scope['bi_saas'].execute(sql)
        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                # elif val[i] is None:
                #     data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)

        context['data'] = data[0]
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()


@good.route('/export_stock_day_report', methods=['GET'], endpoint='export_stock_day_report')
@__token_download
def export_stock_day_report(context):
    '''
    导出
    :param context:
    :return:
    '''
    category_class = request.args.get('CategoryClass')

    try:
        sql = f"""
                   SELECT product_sn as '款号', design_product_sn as '设计款号', color as '颜色', CategoryClass as '品类', up_to_date as '上新时间', product_category2 as '类目', product_year as '开发年份', product_season as '开发季节', dev_prop as '开发属性', old_product_sn as '旧款号', nature as '性质',
                   sell_tag as '销售标签', doc_year as '销售年份', doc_season as '销售季节', iom_cost as '成本', cost as '财务成本', fixed_price as '一口价', first_order_nums_on as '线上首单', reorder_nums_on as '线上返单', plan_total_on as '线上总下单',plan_total_on*ifnull(cost,iom_cost) as '线上总下单成本',produced_nums_on as '线上已入库数',production_onway_nums_on as '线上未入库数', first_order_nums_off as 'O2O首单', reorder_nums_off as 'O2O返单', plan_total_off as 'O2O总下单', plan_total_off*ifnull(cost,iom_cost) as 'O2O总下单成本',
                   produced_nums_off as 'O2O已入库数', production_onway_nums_off as 'O2O未入库数', tran_total as '合计入库',tran_total*ifnull(cost,iom_cost) as '合计入库成本额', production_onway_nums as '合计生产在途库存',production_onway_nums*ifnull(cost,iom_cost) as '合计生产在途库存成本额',
                   o2o_stock_in_shop as 'O2O门店挂板库存',o2o_stock_in_shop*ifnull(cost,iom_cost) as 'O2O门店挂板库存成本额',o2o_stock_toshop_onway as 'O2O到店在途库存',o2o_stock_toshop_onway*ifnull(cost,iom_cost) as 'O2O到店在途库存成本',o2o_stock_in_warehouse as 'O2O在库库存',o2o_stock_in_warehouse*ifnull(cost,iom_cost) as 'O2O在库库存成本',o2o_return_onway as 'O2O退版在途',  
                    o2o_return_onway*ifnull(cost,iom_cost) as 'O2O退版在途成本额',offline_stock as '合计O2O可用库存',offline_stock*ifnull(cost,iom_cost) as '合计O2O可用库存成本额',online_zongcang_stock as 'E3总仓可用库存', online_channel_stock as 'E3渠道仓库存',ifnull(online_zongcang_stock,0)+ifnull(online_channel_stock,0) as '合计线上在库库存', online_outof_stock as 'E3缺货(预售)',online_stock as '合计线上可用库存',online_stock*ifnull(cost,iom_cost) as '合计线上可用库存成本', online_back_stock as '退货仓', 
                  online_back_stock*ifnull(cost,iom_cost) as '退货仓成本额',
                   online_defective_stock as '次品仓',online_defective_stock*ifnull(cost,iom_cost) as '次品仓成本额', total_stock as '总库存', total_stock * ifnull(cost,iom_cost) as  '总库存成本额',
                   online_statistics_stock as '线上订货剩余库存',online_statistics_stock*ifnull(cost,iom_cost) as '线上订货剩余库存成本',
                   offline_statistics_stock as 'O2O订货剩余库存' , offline_statistics_stock*ifnull(cost,iom_cost) as 'O2O订货剩余库存成本额',
                   is_gift as '是否礼品',gmt_created as '生成报表时间'
                   FROM hmcdata.bi_goods_stock_day_report WHERE CategoryClass='{category_class}' ORDER BY doc_year DESC,up_to_date DESC,product_sn DESC;
               """
        # print(sql)

        ret = Scope['bi_saas'].execute(sql)
        columns_names = ret.keys()
        # return excel.make_response_from_query_sets(ret.fetchall(), column_names=columns_names, file_type='xlsx',
        #                                          file_name=f"每日库存报表({category_class})")

        return export_file(columns_names, ret, f"""每日库存报表{datetime.datetime.now().strftime('%Y-%m-%d %H_%M_%S')}""")
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()


@good.route('/get_product_analyze_img', methods=['GET'], endpoint='get_product_analyze_img')
@__token_wrapper
def get_product_analyze_img(context):
    '''
    获取单款分析图片
    :param context:
    :return:
    '''
    product_sn = request.args.get('product_sn')
    try:
        sql = f"""SELECT product_sn, first_sell_date, last_sell_date, product_year, product_season, category, sales_price, "source", 
            designer, sales_way, sales_prop, product_name, cost,add_cart_auction_cnt ,collect_goods_buyer_cnt ,uv ,negative_comment FROM hmcdata.product_analyse_info
            WHERE product_sn ='{product_sn}'"""
        ret = Scope['vertica'].execute(sql)
        columns = ret.keys()
        info = {}
        for val in ret:
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    info[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    info[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    info[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    info[column] = date_format(val[i])
                else:
                    info[column] = val[i]
        img_list = []
        sql = f"""
                SELECT img_src FROM hmcdata.bi_product_color_img where product_sn='{product_sn}' order by img_src;
        """
        ret = Scope['vertica'].execute(sql)
        for img in ret.fetchall():
            img_list.append(img[0])

        context['data'] = {"info": info, "img_list": img_list}
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_product_reorder_sale_trend', methods=['GET'], endpoint='get_product_reorder_sale_trend')
@__token_wrapper
def get_product_reorder_sale_trend(context):
    '''
    获取快返销售趋势
    :param context:
    :return:
    '''
    product_sn = request.args.get('product_sn')
    doc_year = request.args.get('doc_year')
    doc_season = request.args.get('doc_season')
    color = request.args.get('color')

    try:
        sql = f"""SELECT total_year,total_week,SUM(sale_nums) as sale_nums,SUM(sale_amount) as sale_amount,SUM(sale_cost) as sale_cost,SUM(sale_origin) as sale_origin,sale_type
FROM product_reorder_week_sales WHERE product_sn='{product_sn}' AND doc_year={doc_year} AND doc_season='{doc_season}' AND color ='{color}'
GROUP BY product_sn,color,total_year,total_week,sale_type
ORDER BY total_year ASC,total_week ASC"""
        ret = Scope['bi_saas'].execute(sql)
        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                # elif val[i] is None:
                #     data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)

        context['data'] = data
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()


@good.route('/get_product_analyze_sale_info', methods=['GET'], endpoint='get_product_analyze_sale_info')
@__token_wrapper
def get_product_analyze_sale_info(context):
    '''
    获取单款分析销售信息
    :param context:
    :return:
    '''
    product_sn = request.args.get('product_sn')
    try:
        sql = f"""SELECT first_sell_date,first_day_sale_nums,gross_profit,sale_nums,total_amount,IFNULL(last_sell_date,date(NOW()))-IFNULL(first_sell_date,date(NOW())) as sell_days
FROM hmcdata.product_analyse_info WHERE  product_sn ='{product_sn}'"""
        ret = Scope['vertica'].execute(sql)
        columns = ret.keys()
        sale_info = {}
        for val in ret:
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    sale_info[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    sale_info[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    sale_info[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    sale_info[column] = date_format(val[i])
                else:
                    sale_info[column] = val[i]
        sql = f"""
                SELECT IFNULL(AVG(a.goods_number),0) FROM (
SELECT total_day,sum(goods_number) as goods_number FROM hmcdata.product_analyse_day_sales WHERE product_sn='{product_sn}' GROUP BY total_day
ORDER BY  total_day ASC limit 30
) a 
        """
        ret = Scope['vertica'].execute(sql)
        nums = ret.fetchone()
        if nums != None:
            nums = round(nums[0])
        else:
            nums = 0
        avg_nums = {"avg_nums": nums}
        sale_info.update(avg_nums)

        context['data'] = sale_info
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/product_analyze_check', methods=['GET'], endpoint='product_analyze_check')
@__token_wrapper
def product_analyze_check(context):
    '''
    获取是否有该款数据
    :param context:
    :return:
    '''
    product_sn = request.args.get('product_sn')
    try:
        sql = f"""SELECT 1 FROM hmcdata.product_analyse_info WHERE  product_sn ='{product_sn}'"""
        ret = Scope['vertica'].execute(sql)
        result = ret.fetchone()
        checked = False
        if result != None:
            checked = True

        context['data'] = {'checked': checked}
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/product_analyze_uv', methods=['GET'], endpoint='product_analyze_uv')
@__token_wrapper
def product_analyze_uv(context):
    '''
    获取款号销售曲线
    :param context:
    :return:
    '''
    product_sn = request.args.get('product_sn')

    try:

        sql = f'''SELECT SUM(uv) as uv,SUM(collect_goods_buyer_cnt) as collect_goods_buyer_cnt,SUM(add_cart_auction_cnt) as add_cart_auction_cnt,st_date FROM hmcdata.tb_sycm_shop_auction_detail 
        WHERE  st_date>=date(NOW()-30)
        AND outer_goods_id='{product_sn}'
        GROUP BY st_date
        ORDER BY st_date ASC;'''
        # print(sql)
        ret = Scope['vertica'].execute(sql)
        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                elif val[i] is None:
                    data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)

        context['data'] = data
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/product_analyze_stock', methods=['GET'], endpoint='product_analyze_stock')
@__token_wrapper
def product_analyze_stock(context):
    '''
    获取款号库存数据
    :param context:
    :return:
    '''
    product_sn = request.args.get('product_sn')

    try:
        data = {"all_stock": 0, 'return_warehouse': 0, 'shop_quantity': 0}

        sql = f'''SELECT ifnull(SUM(es.sl-es.sl2),0) as nums,'all_stock' as warehouse FROM hmcdata.e3_spkcb es left join hmcdata.e3_cangku c on es.ck_id =c.cangku_id AND es.src_business_id =c.src_business_id 
		WHERE es.goods_sn ='{product_sn}' AND c.cwlb_id>=1
		UNION ALL 
		SELECT ifnull(SUM(es.sl-es.sl2),0) as nums,'return' as warehouse FROM hmcdata.e3_spkcb es left join hmcdata.e3_cangku c on es.ck_id =c.cangku_id AND es.src_business_id =c.src_business_id 
		WHERE es.goods_sn ='{product_sn}' AND c.cwlb_id in(3,9)
		UNION ALL 
        SELECT ifnull(SUM(quantity),0) as nums, 'shop_quantity' as warehouse FROM  hmcdata.hmc_inventory_stock a
        LEFT JOIN hmcdata.hmc_inventory_warehouse b 
        ON a.warehouse_id=b.inventory_warehouse_id AND a.business_unit_id=b.business_unit_id
        WHERE b."type" = 4 AND a.product_sn = '{product_sn}';'''
        # print(sql)
        ret = Scope['vertica'].execute(sql)
        for nums, warehouse in ret:
            if warehouse == "all_stock":
                data['all_stock'] = nums
            elif warehouse == "shop_quantity":
                data['shop_quantity'] = nums
            else:
                data['return_warehouse'] = nums
        data['all_stock'] = data['all_stock'] + data['shop_quantity']
        sku_sql = f""" select a.*, ifnull(d.shop_qty, 0) as shop_qty from ( SELECT sku,sk.color_name ,sk.size_name, SUM(es.sl-es.sl2) as nums FROM hmcdata.e3_spkcb es left join hmcdata.e3_cangku c on es.ck_id =c.cangku_id AND es.src_business_id =c.src_business_id
		left join hmcdata.iom_scm_sku_scu sk on es.sku =sk.sku_barcode 
		WHERE es.goods_sn ='{product_sn}' AND c.cwlb_id in(1,7,10)
		GROUP BY sku,sk.color_name ,sk.size_name
		ORDER BY color_name ,size_name) a LEFT JOIN (SELECT sku_code, SUM(quantity) as shop_qty FROM hmcdata.hmc_inventory_stock a LEFT JOIN hmcdata.hmc_inventory_warehouse b 
        ON a.warehouse_id=b.inventory_warehouse_id AND a.business_unit_id=b.business_unit_id
        WHERE b."type" = 4
        GROUP BY sku_code) d ON a.sku=d.sku_code  """

        ret = Scope['vertica'].execute(sku_sql)
        columns = ret.keys()
        sku_list = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                elif val[i] is None:
                    data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            sku_list.append(data_dict)
        data.update({"sku_list": sku_list})

        context['data'] = data
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/product_analyze_fabric', methods=['GET'], endpoint='product_analyze_fabric')
@__token_wrapper
def product_analyze_fabric(context):
    '''
    获取款号面料信息
    :param context:
    :return:
    '''
    product_sn = request.args.get('product_sn')

    try:

        sql = f"""SELECT fabric_itemcode,goods_color,StockItemType,HaveGreyClothDaysSupply,NotHaveGreyClothDaysSupply,HaveGreyClothQuantity,
            NotHaveGreyClothQuantity,unit,DaysSupply,fabric_single_dosage FROM bi_scm_goods_fabric where goods_bn='{product_sn}' """

        ret = Scope['bi_saas'].execute(sql)
        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                elif val[i] is None:
                    data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)

        context['data'] = data
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()


@good.route('/product_analyze_produce', methods=['GET'], endpoint='product_analyze_produce')
@__token_wrapper
def product_analyze_produce(context):
    '''
    获取款号制单信息
    :param context:
    :return:
    '''
    product_sn = request.args.get('product_sn')

    try:

        sql = f"""SELECT date(CreatedOn) as CreatedOn ,i.ProduceDocNum ,FactoryName ,OrderType,date(PlanPurchaseOrderApprovalDate) as PlanPurchaseOrderApprovalDate,IFNULL(DATE(Arrival90PercentDate),DATE(PurchaseOrderFinishDate)) as last_day,
                ArrivalScheduleDate,case Status when 0 then sum(NotArriveQty) else 0 end as NotArriveQty,IFNULL(sum(a.AccidentCount),0) as AccidentCount,AVG(IFNULL(IFNULL(DATE(Arrival90PercentDate),DATE(PurchaseOrderFinishDate)),DATE(NOW()))-DATE(ArrivalScheduleDate)) as DelayDay
                ,sum(PlanTotal) as PlanTotal,sum(TranTotal) as TranTotal,sum(WBQuantity) as WBQuantity, sum(DefectiveQuantityAmount) as DefectiveQuantityAmount
                FROM hmcdata.iom_scm_produce_schedule_info i 
                left join (
                SELECT COUNT(DISTINCT AccidentOrderId ) as AccidentCount,ProduceOrderNum FROM  hmcdata.iom_scm_inman_accident_order_web isiaow 
                group by ProduceOrderNum
                )a on i.ProduceDocNum =a.ProduceOrderNum
                LEFT JOIN (
                SELECT ProduceOrderNum ,ProductSN,ProductColor,SUM(WBQuantity) as WBQuantity,SUM(DefectiveQuantityAmount) as DefectiveQuantityAmount
                    FROM hmcdata.iom_scm_ProductEstimateArrive WHERE Enabled=1 AND EstimateType='大货预约'
                GROUP BY ProduceOrderNum,ProductColor,ProductSN
                )b on i.ProduceDocNum =b.ProduceOrderNum AND i.ColorName =b.ProductColor and i.product_sn =b.ProductSN
                WHERE product_sn='{product_sn}' and Status <>2
                GROUP BY CreatedOn,FactoryName ,OrderType,PlanPurchaseOrderApprovalDate,last_day,ArrivalScheduleDate,Status ,i.ProduceDocNum
                ORDER BY CreatedOn ASC """

        ret = Scope['vertica'].execute(sql)
        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                elif val[i] is None:
                    data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)

        context['data'] = data
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/product_analyze_offline', methods=['GET'], endpoint='product_analyze_offline')
@__token_wrapper
def product_analyze_offline(context):
    '''
    获取款号线下信息
    :param context:
    :return:
    '''
    product_sn = request.args.get('product_sn')

    try:
        sql = f"""SELECT color_name ,b_area,first_in_time ,first_sale_time,is_model ,in_model,stock,is_sale ,transfer_pool,is_sale ,is_model ,
                        sale_amount,return_amount,cost,out_sale,fixed_price ,
                    sys_model,in_transfer,out_transfer
                        FROM hmcdata.level1_area_product_color_stock where product_sn ='{product_sn}' """

        ret = Scope['vertica'].execute(sql)
        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                elif val[i] is None:
                    data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)

        context['data'] = data
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/product_analyze_sale_weeks', methods=['GET'], endpoint='product_analyze_sale_weeks')
@__token_wrapper
def product_analyze_sale_weeks(context):
    '''
    获取款号最近一年周销售
    :param context:
    :return:
    '''
    product_sn = request.args.get('product_sn')

    try:

        sql = f"""SELECT product_sn,total_week,total_year,sum(goods_number) as goods_number,sum(payment) as payment FROM product_analyse_week_sales WHERE product_sn='{product_sn}'
GROUP BY product_sn,total_week,total_year
ORDER BY total_year,total_week ASC """

        ret = Scope['bi_saas'].execute(sql)
        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                elif val[i] is None:
                    data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)

        context['data'] = data
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()


@good.route('/get_product_discount_monitor', methods=['GET'], endpoint='get_product_discount_monitor')
@__token_wrapper
def get_product_discount_monitor(context):
    '''
    获取茵曼商品折扣监控商品
    :param context:
    :return:
    '''
    page = request.args.get('page')
    page_size = request.args.get('pageSize')
    product_tag = request.args.get('product_tag')
    dc_shop_id = request.args.get('dc_shop_id')
    date_start = request.args.get('dateStart')
    date_end = request.args.get('dateEnd')
    if page_size is None:
        page_size = 10
    if dc_shop_id == 'all' or dc_shop_id is None:
        dc_sql = ""
    else:
        dc_sql = f""" and dc_shop_id={dc_shop_id}"""
    if product_tag == 'all' or product_tag is None:
        product_sql = ""
    else:
        product_sql = f""" and product_tag='{product_tag}'"""
    if date_start is None or date_start == "":
        date_sql = ""
    else:
        date_sql = f"and monitor_min_date >='{date_start}' AND monitor_max_date <='{date_end}'"

    offset = f''' LIMIT {int(page_size)} OFFSET {(int(page) - 1) * int(page_size)} '''
    try:

        sql = f"""SELECT  product_sn, dev_prop, product_tag, product_level, goods_level, doc_year, doc_season, shop_name, min_discount, discount,
            life_circle_min_date, date(gmt_created) as monitor_time, life_circle_max_date, monitor_min_date, monitor_max_date,product_season_prop
             FROM hmcdata.bi_inman_product_discount_monitor WHERE product_season_prop<>'老款'  {date_sql} {dc_sql} {product_sql}
            ORDER BY monitor_max_date DESC {offset} """

        ret = Scope['bi_saas'].execute(sql)
        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)

        context['data'] = data
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()


@good.route('/get_product_discount_monitor_total', methods=['GET'], endpoint='get_product_discount_monitor_total')
@__token_wrapper
def get_product_discount_monitor_total(context):
    '''
    获取茵曼商品折扣监控商品总数
    :param context:
    :return:
    '''
    product_tag = request.args.get('product_tag')
    dc_shop_id = request.args.get('dc_shop_id')
    date_start = request.args.get('dateStart')
    date_end = request.args.get('dateEnd')

    if dc_shop_id == 'all' or dc_shop_id is None:
        dc_sql = ""
    else:
        dc_sql = f""" and dc_shop_id={dc_shop_id}"""
    if product_tag == 'all' or product_tag is None:
        product_sql = ""
    else:
        product_sql = f""" and product_tag='{product_tag}'"""
    if date_start is None or date_start == "":
        date_sql = ""
    else:
        date_sql = f"and monitor_min_date >='{date_start}' AND monitor_max_date <='{date_end}'"

    try:

        sql = f"""SELECT  count(*)
             FROM hmcdata.bi_inman_product_discount_monitor WHERE product_season_prop<>'老款'  {date_sql} {dc_sql} {product_sql}
            """

        ret = Scope['bi_saas'].execute(sql)
        total = ret.fetchone()[0]

        context['data'] = total
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()


@good.route('/export_product_discount_monitor', methods=['GET'], endpoint='export_product_discount_monitor')
@__token_download
def export_product_discount_monitor(context):
    '''
    导出茵曼商品折扣监控商品
    :param context:
    :return:
    '''
    product_tag = request.args.get('product_tag')
    dc_shop_id = request.args.get('dc_shop_id')
    date_start = request.args.get('dateStart')
    date_end = request.args.get('dateEnd')

    if dc_shop_id == 'all' or dc_shop_id is None:
        dc_sql = ""
    else:
        dc_sql = f""" and dc_shop_id={dc_shop_id}"""
    if product_tag == 'all' or product_tag is None:
        product_sql = ""
    else:
        product_sql = f""" and product_tag='{product_tag}'"""
    if date_start is None or date_start == "":
        date_sql = ""
    else:
        date_sql = f"and monitor_min_date >='{date_start}' AND monitor_max_date <='{date_end}'"

    try:

        sql = f"""SELECT  product_sn as '款号', dev_prop as '销售属性',product_season_prop as '销售季节', product_tag as '商品标签',   shop_name as '店铺', min_discount as '最低折扣', discount as '折扣',
             monitor_min_date as '监控开始时间', monitor_max_date as '监控结束时间',date(gmt_created) as '生成数据时间'
             FROM hmcdata.bi_inman_product_discount_monitor WHERE 1=1  {date_sql} {dc_sql} {product_sql}
            ORDER BY monitor_max_date DESC  """

        ret = Scope['bi_saas'].execute(sql)
        columns = ret.keys()

        return export_file(columns, ret, f"""茵曼女装商品折扣监控{datetime.datetime.now().strftime('%Y-%m-%d %H_%M_%S')}""")

        # return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()


@good.route('/get_product_gross_profit_monitor', methods=['GET'], endpoint='get_product_gross_profit_monitor')
@__token_wrapper
def get_product_gross_profit_monitor(context):
    '''
    获取茵曼商品毛利监控商品
    :param context:
    :return:
    '''
    page = request.args.get('page')
    page_size = request.args.get('pageSize')
    dc_shop_id = request.args.get('dc_shop_id')
    date_string = request.args.get('dateString')
    if page_size is None:
        page_size = 10
    if dc_shop_id == 'all' or dc_shop_id is None:
        dc_sql = ""
    else:
        dc_sql = f""" and dc_shop_id={dc_shop_id}"""
    if date_string is None or date_string == "":
        date_sql = ""
    else:
        year = dateutil.parser.parse(date_string).year
        month = dateutil.parser.parse(date_string).month
        date_sql = f"""and total_year={year} and total_month={month}"""

    offset = f''' LIMIT {int(page_size)} OFFSET {(int(page) - 1) * int(page_size)} '''
    try:

        sql = f"""SELECT product_sn, shop_name, product_tag, product_season_prop, min_date, max_date, min_gross_profit, gross_profit, dev_prop, dc_shop_id, total_year, total_month
FROM hmcdata.bi_inman_product_gross_profit_monitor WHERE 1=1  {date_sql} {dc_sql} 
            ORDER BY gmt_created DESC {offset} """
        ret = Scope['bi_saas'].execute(sql)
        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_4(val[i])
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                elif val[i] is None:
                    data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)

        context['data'] = data
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()


@good.route('/get_product_gross_profit_monitor_total', methods=['GET'],
            endpoint='get_product_gross_profit_monitor_total')
@__token_wrapper
def get_product_gross_profit_monitor_total(context):
    '''
    获取茵曼商品毛利监控商品总数
    :param context:
    :return:
    '''
    dc_shop_id = request.args.get('dc_shop_id')
    date_string = request.args.get('dateString')

    if dc_shop_id == 'all' or dc_shop_id is None:
        dc_sql = ""
    else:
        dc_sql = f""" and dc_shop_id={dc_shop_id}"""
    if date_string is None or date_string == "":
        date_sql = ""
    else:
        year = dateutil.parser.parse(date_string).year
        month = dateutil.parser.parse(date_string).month
        date_sql = f"""and total_year={year} and total_month={month}"""

    try:

        sql = f"""SELECT  count(*)
             FROM hmcdata.bi_inman_product_gross_profit_monitor WHERE 1=1  {date_sql} {dc_sql} 
            """

        ret = Scope['bi_saas'].execute(sql)
        total = ret.fetchone()[0]

        context['data'] = total
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()


@good.route('/export_product_gross_profit_monitor', methods=['GET'], endpoint='export_product_gross_profit_monitor')
@__token_download
def export_product_gross_profit_monitor(context):
    '''
    导出茵曼商品毛利监控商品
    :param context:
    :return:
    '''
    product_tag = request.args.get('product_tag')
    dc_shop_id = request.args.get('dc_shop_id')
    date_string = request.args.get('dateString')

    if dc_shop_id == 'all' or dc_shop_id is None:
        dc_sql = ""
    else:
        dc_sql = f""" and dc_shop_id={dc_shop_id}"""
    if product_tag == 'all' or product_tag is None:
        product_sql = ""
    else:
        product_sql = f""" and product_tag='{product_tag}'"""
    if date_string is None or date_string == "":
        date_sql = ""
    else:
        year = dateutil.parser.parse(date_string).year
        month = dateutil.parser.parse(date_string).month
        date_sql = f"""and total_year={year} and total_month={month}"""

    try:

        sql = f"""SELECT product_sn as '款号', shop_name as '店铺', product_tag as '商品标签', product_season_prop as '销售季节', min_date as '监控开始时间',
max_date as '监控结束时间', min_gross_profit as '最低毛利要求', gross_profit as '当前毛利', dev_prop as '销售属性',  total_year as '年', total_month as '月'
FROM hmcdata.bi_inman_product_gross_profit_monitor WHERE 1=1  {date_sql} {dc_sql}   """

        ret = Scope['bi_saas'].execute(sql)
        columns = ret.keys()

        return export_file(columns, ret, f"""茵曼女装商品毛利监控{datetime.datetime.now().strftime('%Y-%m-%d %H_%M_%S')}""")

        # return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()


@good.route('/monitor_label_import', methods=['POST'], endpoint='monitor_label_import')
@__token_wrapper
def monitor_label_import(context):
    '''
    导入茵曼商品监控标签
    :param context:
    :return:
    '''

    import os
    try:
        if request.method == 'POST':
            # 获取前端传输的文件(对象)
            f = request.files.get('file')
            # secure_filename：检测中文是否合法
            filename = secure_filename(f.filename)
            # 验证文件格式
            types = ['xls', 'xlsx']
            if filename.split('.')[-1] in types:
                path = os.path.join(UPLOAD_FOLDER, "product_monitor_label.xlsx")
                f.save(path)
                # f"""{UPLOAD_FOLDER}product_monitor_label.xlsx"""
                succ, msg = import_bi_inman_product_label("bi_inman_product_final_label", path)
                if succ:
                    context['message'] = "上传文件成功！"
                    context['statusCode'] = 1
                    context['success'] = True
                    context['data'] = []
                    return jsonify(context)
                else:
                    context['message'] = msg
                    context['statusCode'] = 1
                    context['success'] = False
                    context['data'] = []
                    return jsonify(context)
            else:
                context['message'] = "文件格式不合法！"
                context['statusCode'] = -1
                context['success'] = False
                context['data'] = []
                return jsonify(context)
        else:
            context['message'] = "请求方法不正确！"
            context['statusCode'] = -1
            context['success'] = False
            context['data'] = []
            return jsonify(context)

    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)


@good.route('/get_product_monitor_label_last_time', methods=['GET'], endpoint='get_product_monitor_label_last_time')
@__token_wrapper
def get_product_monitor_label_last_time(context):
    '''
    获取导入文件最新事件
    :param context:
    :return:
    '''

    try:

        sql = f"""select MAX(gmt_created ) as gmt_created FROM hmcdata.bi_inman_product_final_label;  """

        ret = Scope['vertica'].execute(sql)
        max_time = ret.fetchone()[0]
        if max_time is None:
            max_time = 0
        else:
            max_time = date_format(max_time)
        context['data'] = max_time
        return jsonify(context)
    except Exception as e:
        logger.error(e)
        context['message'] = "获取数据时出错，请联系管理员！"
        context['statusCode'] = -1
        context['success'] = False
        context['data'] = []
        return jsonify(context)
    finally:
        Scope['vertica'].remove()
