# -*- coding: utf-8 -*-
# @Time    : 2021/4/9 11:49
# @Author  : Joshua
# @File    : product_dist_pool_analysis

from flask import jsonify, request
from bi_flask.__token import __token_wrapper
import logging
import json
from bi_flask._sessions import sessions, sessions_scopes, get_cache
from bi_flask.utils import *
from bi_flask.goods.api import good

logger = logging.getLogger('bi')
Scope = sessions_scopes(sessions)
cache = get_cache()


@good.route('/get_dist_pool_category', methods=['GET'], endpoint='get_dist_pool_category')
@__token_wrapper
def get_dist_pool_category(context):
    '''
        获取调拨池类目分布
        :param context:
        :return:
        '''
    try:
        business_unit_id = context['data']['businessUnitId']
        category = request.args.get('category')
        shopIds = request.args.get('shopIds')
        product_sn = request.args.get('productSn')
        if product_sn is None or product_sn == '':
            product_sn_sql = """"""
        else:
            product_sn_sql = f"""and product_sn like '%{product_sn}%' """
        if category is None or category == '':
            category_sql = """"""
        else:
            category_sql = f"""and category ='{category}' """
        if shopIds is None or shopIds == '':
            shopIds_sql = """"""
        else:
            shopIds_sql = f"""and dc_shop_id in ({shopIds}) """

        sql = f"""
            SELECT category as name ,SUM(stock_num) as value FROM   hmcdata.bi_dist_pool_analysis where business_unit_id=:business_unit_id {category_sql} {shopIds_sql} {product_sn_sql}
            group by category
            order by value desc
                        """
        ret = Scope['vertica'].execute(sql, {'business_unit_id': business_unit_id})
        columns = ret.keys()
        data = []
        for rank, val in enumerate(ret):
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
        Scope['vertica'].remove()


@good.route('/get_dist_pool_info', methods=['GET'], endpoint='get_dist_pool_info')
@__token_wrapper
def get_dist_pool_info(context):
    '''
        获取调拨池基础数据
        :param context:
        :return:
        '''
    try:
        business_unit_id = context['data']['businessUnitId']
        shopIds = request.args.get('shopIds')
        category = request.args.get('category')
        product_sn = request.args.get('productSn')
        if product_sn is None or product_sn == '':
            product_sn_sql = """"""
        else:
            product_sn_sql = f"""and product_sn like '%{product_sn}%' """
        if category is None or category == '':
            category_sql = """"""
        else:
            category_sql = f"""and category ='{category}' """

        if shopIds is None or shopIds == '':
            shopIds_sql = """"""
        else:
            shopIds_sql = f"""and dc_shop_id in ({shopIds}) """

        sql = f"""
            SELECT COUNT(DISTINCT product_sn) as product_count,sum(stock_num) as stock_num,COUNT(DISTINCT dc_shop_id) as shop_count 
            FROM hmcdata.bi_dist_pool_analysis
            where business_unit_id=:business_unit_id {category_sql} {shopIds_sql} {product_sn_sql}
                        """
        ret = Scope['vertica'].execute(sql, {'business_unit_id': business_unit_id})
        columns = ret.keys()
        data_dict = {}
        for rank, val in enumerate(ret):
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
            # data.append(data_dict)
        context['data'] = data_dict
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_dist_pool_season', methods=['GET'], endpoint='get_dist_pool_season')
@__token_wrapper
def get_dist_pool_season(context):
    '''
        获取调拨池类目分布
        :param context:
        :return:
        '''
    try:
        business_unit_id = context['data']['businessUnitId']
        category = request.args.get('category')
        shopIds = request.args.get('shopIds')
        product_sn = request.args.get('productSn')
        if product_sn is None or product_sn == '':
            product_sn_sql = """"""
        else:
            product_sn_sql = f"""and product_sn like '%{product_sn}%' """
        if category is None or category == '':
            category_sql = """"""
        else:
            category_sql = f"""and category ='{category}' """
        if shopIds is None or shopIds == '':
            shopIds_sql = """"""
        else:
            shopIds_sql = f"""and dc_shop_id in ({shopIds}) """

        sql = f"""
            SELECT product_season as name ,SUM(stock_num) as value FROM   hmcdata.bi_dist_pool_analysis where business_unit_id=:business_unit_id
            {category_sql} {shopIds_sql} {product_sn_sql}
            group by product_season 
            order by value desc
                        """
        ret = Scope['vertica'].execute(sql, {'business_unit_id': business_unit_id})
        columns = ret.keys()
        data = []
        for rank, val in enumerate(ret):
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
        Scope['vertica'].remove()


@good.route('/get_dist_pool_transfer_shop_rank', methods=['GET'], endpoint='get_dist_pool_transfer_shop_rank')
@__token_wrapper
def get_dist_pool_transfer_shop_rank(context):
    '''
        获取调拨池调出店铺TOP5
        :param context:
        :return:
        '''
    try:
        business_unit_id = context['data']['businessUnitId']
        shopIds = request.args.get('shopIds')
        category = request.args.get('category')
        product_sn = request.args.get('productSn')
        if product_sn is None or product_sn == '':
            product_sn_sql = """"""
        else:
            product_sn_sql = f"""and product_sn like '%{product_sn}%' """
        if category is None or category == '':
            category_sql = """"""
        else:
            category_sql = f"""and category ='{category}' """

        if shopIds is None or shopIds == '':
            shopIds_sql = """"""
        else:
            shopIds_sql = f"""and to_dc_shop_id in ({shopIds}) """

        sql_in = f"""
                    SELECT to_dc_shop_id as dc_shop_id,to_shop_name as shop_name,to_level_name as level_name ,sum(actual_quantity) as tranfer_nums FROM hmcdata.bi_dist_model_transfer_base
                    where status in('待收货',
                            '已收货',
                            '待发货') and assign_type =0 and date(create_time)=date(NOW()) and
                        business_unit_id =:business_unit_id  {category_sql} {shopIds_sql} {product_sn_sql}
                    group by to_shop_name ,to_level_name,to_dc_shop_id
                    ORDER by tranfer_nums desc limit 5
                        """
        ret_in = Scope['vertica'].execute(sql_in, {'business_unit_id': business_unit_id})
        columns = ret_in.keys()
        data = []
        for rank, val in enumerate(ret_in):
            data_dict = {'rank': rank + 1, 'key': f'running{rank}'}
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
        Scope['vertica'].remove()


@good.route('/get_dist_pool_transfer_category', methods=['GET'], endpoint='get_dist_pool_transfer_category')
@__token_wrapper
def get_dist_pool_transfer_category(context):
    '''
        获取调拨池类目
        :param context:
        :return:
        '''
    try:
        if cache.has(f'pool_transfer_category'):
            data = cache.get(f'pool_transfer_category')
            context['data'] = json.loads(data)
            return jsonify(context)
        else:
            sql = f"""
                        SELECT category,sum(actual_quantity) as actual_quantity FROM hmcdata.bi_dist_model_transfer_base
                        where category is not null
                        group by category
                        order by actual_quantity desc
                            """
            ret = Scope['vertica'].execute(sql)
            columns = ret.keys()
            data = []
            for rank, val in enumerate(ret):
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
            cache.set('pool_transfer_category', json.dumps(data), timeout=10 * 60)
            context['data'] = data
            return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_dist_pool_goods', methods=['GET'], endpoint='get_dist_pool_goods')
@__token_wrapper
def get_dist_pool_goods(context):
    '''
        获取调拨池商品基础数据
        :param context:
        :return:
        '''
    try:
        business_unit_id = context['data']['businessUnitId']
        page = request.args.get('page')
        page_size = request.args.get('pageSize')
        sorter = request.args.get('sorter')
        category = request.args.get('category')
        product_sn = request.args.get('productSn')
        if product_sn is None or product_sn == '':
            product_sn_sql = """"""
        else:
            product_sn_sql = f"""and s.product_sn like '%{product_sn}%' """
        if category is None or category == '':
            category_sql = """"""
        else:
            category_sql = f"""and category ='{category}' """
        order = request.args.get('order')
        order_sql = ""
        if sorter is None or sorter == '':
            sorter_sql = f""""""
        else:
            sorter_sql = f"""order by {sorter}"""
            if order == 'descend':
                order_sql = f"""desc"""
            else:
                order_sql = f"""asc"""

        if page_size is None:
            page_size = 10
        offset = f''' LIMIT {int(page_size)} OFFSET {(int(page) - 1) * int(page_size)} '''
        sql = f"""
            SELECT s.id as key,s.product_sn,s.color,s.product_name,s.product_season,s.product_year,s.fixed_price,s.dist_pool_stock_num,s.shop_count,s.stock_now,s.product_tag,
            s.sold_out_rate,s.dist_pool_shop_count,i.img_src,category,dev_prop FROM hmcdata.bi_dist_pool_goods_analysis s left join hmcdata.bi_product_img i on s.product_sn=i.product_sn 
            where s.business_unit_id=:business_unit_id {category_sql} {product_sn_sql} {sorter_sql} {order_sql} {offset} 
                        """
        ret = Scope['vertica'].execute(sql, {'business_unit_id': business_unit_id})
        columns = ret.keys()
        data = []
        for rank, val in enumerate(ret):
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
        total_sql = f"""
                    SELECT count(*) FROM hmcdata.bi_dist_pool_goods_analysis s 
                    where business_unit_id=:business_unit_id  {category_sql}  {product_sn_sql}
                                """
        ret_total = Scope['vertica'].execute(total_sql, {'business_unit_id': business_unit_id})
        total = ret_total.fetchone()[0]
        context['data'] = data
        context['total'] = total
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_bi_dist_pool_shops_stock_reason', methods=['GET'], endpoint='get_bi_dist_pool_shops_stock_reason')
@__token_wrapper
def get_bi_dist_pool_shops_stock_reason(context):
    '''
        获取调拨池商品基础数据
        :param context:
        :return:
        '''
    try:
        business_unit_id = context['data']['businessUnitId']
        product_sn = request.args.get('product_sn')
        color = request.args.get('color')
        page = request.args.get('page')
        shopType = request.args.get('shopType')
        shop = request.args.get('shop')
        page_size = request.args.get('pageSize')
        sorter = request.args.get('sorter')
        order = request.args.get('order')
        order_sql = ""
        if sorter is None or sorter == '':
            sorter_sql = f""""""
        else:
            sorter_sql = f"""order by {sorter}"""
            if order == 'descend':
                order_sql = f"""desc"""
            else:
                order_sql = f"""asc"""
        if shopType is None or shopType == 'all':
            shop_type_sql = ""
        else:
            shop_type_sql = f"""and reason is not null"""
        if shop is None or shop == '':
            shop_sql = ""
        else:
            shop_sql = f"""and shop_name like '%{shop}%' """

        if page_size is None:
            page_size = 10
        offset = f''' LIMIT {int(page_size)} OFFSET {(int(page) - 1) * int(page_size)} '''
        sql = f"""
            SELECT id as key,shop_name ,stock ,province_name,reason,level_name FROM hmcdata.bi_dist_pool_shops_stock_reason 
            where product_sn=:product_sn and color_name=:color and business_unit_id=:business_unit_id {shop_type_sql} {shop_sql} {sorter_sql} {order_sql} {offset} 
                        """
        ret = Scope['vertica'].execute(sql,
                                       {'business_unit_id': business_unit_id, 'product_sn': product_sn, 'color': color})
        columns = ret.keys()
        data = []
        for rank, val in enumerate(ret):
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
        total_sql = f"""
                    SELECT count(*) FROM hmcdata.bi_dist_pool_shops_stock_reason s 
                    where product_sn=:product_sn and color_name=:color and business_unit_id=:business_unit_id {shop_type_sql} {shop_sql}
                                """
        ret_total = Scope['vertica'].execute(total_sql, {'business_unit_id': business_unit_id, 'product_sn': product_sn,
                                                         'color': color})
        total = ret_total.fetchone()[0]
        context['data'] = data
        context['total'] = total
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_dist_pool_status', methods=['GET'], endpoint='get_dist_pool_status')
@__token_wrapper
def get_dist_pool_status(context):
    '''
        获取调拨池状态
        :param context:
        :return:
        '''
    try:
        business_unit_id = context['data']['businessUnitId']
        if cache.has(f'dist_pool_status_{business_unit_id}'):
            data = cache.get(f'dist_pool_status_{business_unit_id}')
            context['data'] =data
            return jsonify(context)
        else:
            sql = f"""
                        SELECT COUNT(*) FROM dist_unsale_goods_pool
                            """
            ret = Scope['bi_saas'].execute(sql)
            bi_total = ret.fetchone()[0]
            product_sql = f"""select count(*) from inventory_transfer_pool """
            product_ret = Scope['hmc_product_center'].execute(product_sql)
            product_total = product_ret.fetchone()[0]
            status = 0
            if bi_total == product_total:
                status = 1
            context['data'] = status
            #缓存调拨池状态
            cache.set(f'dist_pool_status_{business_unit_id}', status, timeout=120 * 60)
            return jsonify(context)
    finally:
        Scope['bi_saas'].remove()
        Scope['hmc_product_center'].remove()
