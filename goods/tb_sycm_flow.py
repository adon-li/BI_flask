# -*- coding: utf-8 -*-
# @Author  : Joshua
# @File    : tb_sycm_flow

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


@good.route('/get_sycm_category', methods=['GET'], endpoint='get_sycm_category')
@__token_wrapper
def get_sycm_category(context):
    '''
        获取商品流量类目
        :param context:
        :return:
        '''
    try:
        if cache.has(f'sycm_category'):
            data = cache.get(f'sycm_category')
            context['data'] = json.loads(data)
            return jsonify(context)
        else:
            sql = f"""
                       	SELECT DISTINCT product_category2 FROM hmcdata.iom_scm_product_list ispl where CategoryClass in('茵曼服装','茵曼鞋子','茵曼饰品','茵曼包') order by product_category2 

                            """
            ret = Scope['vertica'].execute(sql)
            data = []
            for category in ret:
                data.append(category[0])
            cache.set('sycm_category', json.dumps(data), timeout=10 * 60)
            context['data'] = data
            return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_sycm_flow_info', methods=['GET'], endpoint='get_sycm_flow_info')
@__token_wrapper
def get_sycm_flow_info(context):
    '''
        获取生意参谋流量汇总
        :param context:
        :return:
        '''
    try:
        product_sn = request.args.get('productSn')
        date_start = request.args.get('date_start', type=str)
        date_end = request.args.get('date_end', type=str)

        if date_start is None or date_end is None:
            return jsonify(build_context(context, '时间不能为空', False, -1))

        sql = f"""

                    SELECT f.flow_source,sum(f.uv) as uv,sum(f.pv) as pv,sum(f.pay_amount) as pay_amount,sum(f.shop_join_in) as shop_join_in,
                SUM(shop_join_out) as shop_join_out,SUM(collect_count) as collect_count,
                SUM(add_cart) as add_cart,SUM(order_user_count) as  order_user_count,SUM(pay_products) as pay_products ,
                sum(pay_user_count) as pay_user_count,sum(pay_direct_user_count) as pay_direct_user_count ,sum(collect_pay_user_count) as collect_pay_user_count ,
                sum(fans_pay_user_count) as fans_pay_user_count,sum(add_cart_pay_user_count) as add_cart_pay_user_count,
                case when sum(f.uv)=0 then 0 else sum(f.order_user_count) /sum(f.uv) end as conversion_rate,
                case when sum(f.uv)=0 then 0 else sum(f.pay_user_count) /sum(f.uv) end as pay_conversion_rate,
                case when sum(f.uv)=0 then 0 else sum(f.add_cart) /sum(f.uv) end as add_cart_rate
                FROM hmcdata.tb_sycm_product_flow f 
                where list_type =1  and st_date>='{date_start}' and st_date<='{date_end}' 
                GROUP BY f.flow_source
                        """
        ret = Scope['vertica'].execute(sql, {'product_sn': product_sn})
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


@good.route('/get_sycm_flow_product_info', methods=['GET'], endpoint='get_sycm_flow_product_info')
@__token_wrapper
def get_sycm_flow_product_info(context):
    '''
        获取生意参谋商品流量
        :param context:
        :return:
        '''
    try:
        page = request.args.get('page', type=int)
        page_size = request.args.get('pageSize')
        sorter = request.args.get('sorter')
        product_sn = request.args.get('productSn')
        category = request.args.get('category')
        date_start = request.args.get('date_start', type=str)
        date_end = request.args.get('date_end', type=str)
        order = request.args.get('order')
        flow_source = request.args.get('flowSource')

        if date_start is None or date_end is None:
            return jsonify(build_context(context, '时间不能为空', False, -1))
        if product_sn is None or product_sn == '':
            product_sql = "and f.product_sn is not null"
        else:
            product_sql = f"""and f.product_sn='{product_sn}' """

        if category is None or category == 'all' or category == '':
            category_sql = ""
        else:
            category_sql = f"""and i.category='{category}' """
        offset = page_helper(sorter=sorter, order=order, page_size=page_size, page=page)

        sql = f"""
                    SELECT i.product_title,i.img_src ,f.product_sn,sum(f.uv) as uv,sum(f.pv) as pv,sum(f.pay_amount) as pay_amount,sum(f.shop_join_in) as shop_join_in,
                SUM(shop_join_out) as shop_join_out,SUM(collect_count) as collect_count,
                SUM(add_cart) as add_cart,SUM(order_user_count) as  order_user_count,SUM(pay_products) as pay_products ,
                sum(pay_user_count) as pay_user_count,sum(pay_direct_user_count) as pay_direct_user_count ,sum(collect_pay_user_count) as collect_pay_user_count ,
                sum(fans_pay_user_count) as fans_pay_user_count,sum(add_cart_pay_user_count) as add_cart_pay_user_count,
                case when sum(f.uv)=0 then 0 else sum(f.order_user_count) /sum(f.uv) end as conversion_rate,
                case when sum(f.uv)=0 then 0 else sum(f.pay_user_count) /sum(f.uv) end as pay_conversion_rate,
                case when sum(f.uv)=0 then 0 else sum(f.add_cart) /sum(f.uv) end as add_cart_rate
                FROM hmcdata.tb_sycm_product_flow f 
                left join hmcdata.bi_new_product_analyse_info i on f.product_sn =i.product_sn 
                where list_type =1 and f.flow_source=:flow_source and st_date>='{date_start}' and st_date<='{date_end}' {product_sql} {category_sql}
                GROUP BY i.img_src ,f.product_sn,i.product_title
                {offset}
                        """
        ret = Scope['vertica'].execute(sql, {'flow_source': flow_source})
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
        total_sql = f"""
                select count(*) from(
                    SELECT 1 FROM hmcdata.tb_sycm_product_flow f 
                left join hmcdata.bi_new_product_analyse_info i on f.product_sn =i.product_sn 
                    where list_type =1  and st_date>='{date_start}' and st_date<='{date_end}' {product_sql} {category_sql}
                    GROUP BY i.img_src ,f.product_sn
                )a
                       """
        ret_total = Scope['vertica'].execute(total_sql)
        total = ret_total.fetchone()[0]
        context['total'] = total
        context['data'] = data
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_sycm_flow_product_detail', methods=['GET'], endpoint='get_sycm_flow_product_detail')
@__token_wrapper
def get_sycm_flow_product_detail(context):
    '''
        获取生意参谋商品流量款明细
        :param context:
        :return:
        '''
    try:
        product_sn = request.args.get('productSn')
        date_start = request.args.get('date_start', type=str)
        date_end = request.args.get('date_end', type=str)

        if date_start is None or date_end is None:
            return jsonify(build_context(context, '时间不能为空', False, -1))

        sql = f"""

                    SELECT f.flow_source,f.product_sn,sum(f.uv) as uv,sum(f.pv) as pv,sum(f.pay_amount) as pay_amount,sum(f.shop_join_in) as shop_join_in,
                SUM(shop_join_out) as shop_join_out,SUM(collect_count) as collect_count,
                SUM(add_cart) as add_cart,SUM(order_user_count) as  order_user_count,SUM(pay_products) as pay_products ,
                sum(pay_user_count) as pay_user_count,sum(pay_direct_user_count) as pay_direct_user_count ,sum(collect_pay_user_count) as collect_pay_user_count ,
                sum(fans_pay_user_count) as fans_pay_user_count,sum(add_cart_pay_user_count) as add_cart_pay_user_count,
                case when sum(f.uv)=0 then 0 else sum(f.order_user_count) /sum(f.uv) end as conversion_rate,
                case when sum(f.uv)=0 then 0 else sum(f.pay_user_count) /sum(f.uv) end as pay_conversion_rate,
                case when sum(f.uv)=0 then 0 else sum(f.add_cart) /sum(f.uv) end as add_cart_rate
                FROM hmcdata.tb_sycm_product_flow f 
                where list_type =1  and st_date>='{date_start}' and st_date<='{date_end}' and f.product_sn=:product_sn
                GROUP BY f.product_sn,f.flow_source
                        """
        ret = Scope['vertica'].execute(sql, {'product_sn': product_sn})
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


@good.route('/get_sycm_flow_trend', methods=['GET'], endpoint='get_sycm_flow_trend')
@__token_wrapper
def get_sycm_flow_trend(context):
    '''
        获取生意参谋商品流量曲线图
        :param context:
        :return:
        '''
    try:
        product_sn = request.args.get('productSn')
        date_start = request.args.get('date_start', type=str)
        date_end = request.args.get('date_end', type=str)
        flow_source = request.args.get('flow_source', type=str)
        if date_start is None or date_end is None:
            return jsonify(build_context(context, '时间不能为空', False, -1))

        sql = f"""
                    SELECT st_date ,sum(uv) as uv,sum(add_cart) as add_cart,SUM(collect_count) as collect_count FROM hmcdata.tb_sycm_product_flow
                where list_type =1 and st_date>='{date_start}' and st_date<='{date_end}' and product_sn=:product_sn and flow_source =:flow_source
                group BY st_date
                order by st_date ASC
                        """
        ret = Scope['vertica'].execute(sql, {'product_sn': product_sn, 'flow_source': flow_source})
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
