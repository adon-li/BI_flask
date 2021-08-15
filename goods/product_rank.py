#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author  : kingsley kwong
# @Site    : 线下用户分析
# @File    : member\offline_member_analysis.py
# @Software: BI flask
# @Function:

from flask import jsonify, request
from bi_flask.__token import __token_wrapper
import logging
import json
from bi_flask._sessions import sessions, sessions_scopes
from bi_flask.utils import *
from bi_flask.goods.api import good

logger = logging.getLogger('bi')
Scope = sessions_scopes(sessions)


@good.route('/product_rank', methods=['GET'], endpoint='product_rank')
@__token_wrapper
def product_rank(context):
    '''
    商品销售排名
    :param context:
    :return:
    '''
    try:
        business_unit_id = context['data']['businessUnitId']
        shop_id = context['data']['online_shop_id_list'] + context['data']['offline_shop_id_list']
        date_start = request.args.get('dateStart')
        date_end = request.args.get('dateEnd')
        business_id = request.args.get('businessId')
        dc_shop_id = request.args.get('shopId')
        page = request.args.get('page')
        page_size = request.args.get('pageSize')
        sort_field = request.args.get('sortField')
        order = request.args.get('order')

        sort_dict = {'price_sale': 'SUM(price_sale)', 'order_products': 'SUM(order_products)',
                     'order_count': 'SUM(order_count)','onway_nums':'max(st.onway_nums)','stock':'max(st.stock)','return_nums':'return_nums'}

        if business_id == 'all' or business_id is None or business_id == "":
            business_id_str = ""
        else:
            business_id_str = f"""AND s.business_clazz_id={business_id}"""

        if order is None:
            order_by = "ORDER BY SUM(price_sale) DESC"
        else:
            order_by = f''' ORDER BY {sort_dict[sort_field]} {'DESC' if order == 'descend' else 'ASC'} '''
        if dc_shop_id == 'all' or dc_shop_id is None or dc_shop_id == "":
            sql_str = ""
        else:
            sql_str = f"""AND g.dc_shop_id={dc_shop_id}"""

        if page_size is None:
            page_size = 20
        offset = f''' LIMIT {int(page_size)} OFFSET {(int(page) - 1) * int(page_size)} '''

        sql = f"""SELECT i.img_src,g.product_sn,SUM(price_sale) as price_sale,
                (SUM(price_sale)-SUM(sale_cost))/SUM(price_sale) as GM,SUM(order_products) as order_products,SUM(order_count) as order_count,
                ROUND(SUM(price_sale)/SUM(order_products),2) as unit_price,product_year,product_season,g.product_category2,max(la.product_label) as product_label,
                max(st.onway_nums) as onway_nums,max(st.stock) as stock,sum(ifnull(return_nums,0)) as return_nums FROM bi_shop_saleinfo_goods g
                LEFT JOIN bi_shops_new s ON g.dc_shop_id=s.shop_id
                LEFT JOIN bi_product_img i ON g.product_sn=i.product_sn AND i.relate_type=1 
        		LEFT JOIN product_sales_label la on la.product_sn=g.product_sn
                LEFT JOIN bi_stock_now st on g.product_sn=st.product_sn
                LEFT JOIN bi_shop_saleinfo_return_goods rg on g.totalday=rg.total_day and g.dc_shop_id=rg.dc_shop_id and g.product_sn=rg.product_sn
                WHERE totalday>='{date_start}' AND totalday<='{date_end}' {business_id_str} 
                {sql_str} 
                GROUP BY g.product_sn,g.product_category2
                {order_by}
                {offset}
                """
        # print(sql)
        ret = Scope['bi_saas'].execute(sql)
        # data = {'now': {}, 'last': {}}
        sql = f"""
        SELECT count(*) as total from(
        SELECT 1 FROM bi_shop_saleinfo_goods g
        LEFT JOIN bi_shops_new s ON g.dc_shop_id=s.shop_id
        LEFT JOIN iom_scm_product_list p ON g.product_sn=p.ProductSN
        WHERE totalday>='{date_start}' AND totalday<='{date_end}' {business_id_str} 
        {sql_str}  
        GROUP BY g.product_sn,g.product_category2
        )a
        """
        # print(sql)
        ret_total = Scope['bi_saas'].execute(sql)
        total = ret_total.fetchall()[0][0]
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

        context['data'] = {'data': data, 'total': total}
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
