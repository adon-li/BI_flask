# -*- coding: utf-8 -*-
# @Author  : Joshua
# @File    : product_season_plan

from flask import jsonify, request
from bi_flask.__token import __token_wrapper
import logging
import json
from bi_flask._sessions import sessions, sessions_scopes
from bi_flask.utils import *
from bi_flask.goods.api import good

logger = logging.getLogger('bi')
Scope = sessions_scopes(sessions)


@good.route('/get_product_season_plan', methods=['GET'], endpoint='get_product_season_plan')
@__token_wrapper
def get_product_season_plan(context):
    '''

    :param context:
    :return:
    '''
    try:
        bi_business_clazz_id_list = context['data']['bi_business_clazz_id_list']
        if len(bi_business_clazz_id_list) == 0:
            context['data'] = []
            return jsonify(context)
        business_id = request.args.get('business_id')
        year = request.args.get('year')
        season = request.args.get('season')

        sql = f"""
            SELECT id as 'key',category_class, category, doc_year, doc_season, product_dev_plan, purchase_cost_plan, actual_purchase_product_nums,
actual_purchase_cost, product_sale_nums, plan_total, tran_total, production_onway_nums, stock_nums, out_of_stock, sale_cost,
return_cost, sale_cost_plan, sale_rate_plan, plan_total_cost, tran_total_cost, production_onway_nums_cost, stock_cost, 
ifnull(out_of_stock_cost,0) as out_of_stock_cost,sell_through_rate_target,first_order_nums,reorder_nums ,transfer_nums,transfer_nums_cost,actual_produce_nums, 
transfer_product_nums,designer_dev_product_nums,ODM,OEM,total_sell_through_rate_target
FROM hmcdata.bi_product_season_plan_category c where doc_year=:doc_year and doc_season=:doc_season 
    and exists (
    select 1 from bi_business_brand_new n where n.business_id={business_id} and c.category_class=n.CategoryClass and c.is_online=n.is_online 
    )
    order by actual_purchase_cost desc
                """
        ret = Scope['bi_saas'].execute(sql, {'doc_year': year,
                                             'doc_season': season})
        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_num(val[i], 4)
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_num(val[i], 4)
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                elif val[i] is None:
                    data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)

        sql_total = f"""
                    select sum(b.all_season_tran_total) as all_season_tran_total,sum(b.all_season_stock) as all_season_stock,sum(b.last_year_designer_dev_product_nums) as last_year_designer_dev_product_nums
    ,sum(b.last_year_actual_purchase_product_nums) as last_year_actual_purchase_product_nums ,sum(ifnull(b.last_year_product_dev_plan,0)) as last_year_product_dev_plan FROM (
                    select SUM(tran_total) as all_season_tran_total,SUM(IFNULL(production_onway_nums,0)+IFNULL(stock_nums,0)) as all_season_stock,
                    a.last_year_designer_dev_product_nums ,a.last_year_actual_purchase_product_nums,d.last_year_product_dev_plan
                    FROM hmcdata.bi_product_season_plan_category c
                    left join(
                    SELECT SUM(designer_dev_product_nums) as last_year_designer_dev_product_nums,SUM(actual_purchase_product_nums) as last_year_actual_purchase_product_nums,category_class 
                    FROM hmcdata.bi_product_season_plan_category cc
                    where doc_year={int(year) - 1} and doc_season =:doc_season 
                    and exists (
                        select 1 from bi_business_brand_new n where n.business_id={business_id} and cc.category_class=n.CategoryClass and cc.is_online=n.is_online 
                    )
                    group by category_class
                    ) a on c.category_class =a.category_class
                    left join (
                        SELECT category_class,SUM(product_dev_plan) as last_year_product_dev_plan FROM hmcdata.bi_product_season_plan_base bb where doc_year ={int(year) - 1} 
                    and doc_season =:doc_season 
                    and exists (
                        select 1 from bi_business_brand_new n where n.business_id={business_id} and bb.category_class=n.CategoryClass and bb.is_online=n.is_online 
                    )
                        group by category_class
                    )d on c.category_class=d.category_class
                    where   doc_year=:doc_year and exists (
                    select 1 from bi_business_brand_new n where n.business_id={business_id} and c.category_class=n.CategoryClass and c.is_online=n.is_online 
                    )
                    group by c.category_class,doc_year,a.last_year_designer_dev_product_nums ,a.last_year_actual_purchase_product_nums,d.last_year_product_dev_plan
            ) b"""
        all_data = []
        ret_total = Scope['bi_saas'].execute(sql_total, {'doc_year': year, 'doc_season': season})
        all_columns = ret_total.keys()

        for val in ret_total:
            data_dict = {}
            for i, column in enumerate(all_columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_num(val[i], 4)
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_num(val[i], 4)
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                elif val[i] is None:
                    data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            all_data.append(data_dict)
        context['data'] = data
        if len(all_data) == 1:
            context['total'] = all_data[0]
        else:
            context['total'] = {}

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


@good.route('/get_product_season_plan_detail', methods=['GET'], endpoint='get_product_season_plan_detail')
@__token_wrapper
def get_product_season_plan_detail(context):
    '''
        获取商品企划明细
        :param context:
        :return:
        '''
    try:
        business_id = request.args.get('business_id')
        doc_season = request.args.get('doc_season')
        doc_year = request.args.get('doc_year')
        cat = request.args.get('cat')
        sql = f"""SELECT d.id as 'key',(i.img_src) as img_src, category, d.doc_year, d.doc_season, d.product_sn, d.color_name, production_onway_nums, stock_nums, planorder_nums,
                produced_nums, out_of_stock, cost, tag, actual_produce_nums, transfer_nums,sale_cost as sale_cost,origin_amount as origin_amount,return_cost as return_cost,sale_amount as sale_amount
                FROM hmcdata.bi_product_season_plan_category_detail d
                left join hmcdata.bi_product_img i on d.product_sn=i.product_sn
                where category=:cat and d.doc_year=:doc_year and doc_season=:doc_season  and exists (
                    select 1 from bi_business_brand_new n where n.business_id={business_id} and d.category_class=n.CategoryClass and d.is_online=n.is_online 
                )
                order by d.product_sn"""

        ret = Scope['bi_saas'].execute(sql, {'doc_year': doc_year, 'doc_season': doc_season, 'cat': cat,
                                             })
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
        Scope['bi_saas'].remove()


@good.route('/get_product_season_golden_triangle', methods=['GET'], endpoint='get_product_season_golden_triangle')
@__token_wrapper
def get_product_season_golden_triangle(context):
    '''
        获取商品金三角明细
        :param context:
        :return:
        '''
    try:
        business_id = request.args.get('business_id')
        doc_season = request.args.get('season')
        doc_year = request.args.get('year')
        cat = request.args.get('cat')
        sql = f"""
                select a.*,i.img_src from (
                SELECT product_sn,sum(stock_nums+production_onway_nums) as total_stock,sum(planorder_nums) as planorder_nums,ifnull(golden_triangle,'未知') as golden_triangle,fixed_price,
                ifnull(product_style,'未知') as product_style,ifnull(big_style,'未知') as big_style FROM hmcdata.bi_product_season_plan_category_detail d
                where category=:cat and doc_year=:doc_year and doc_season=:doc_season  and exists (
                  select 1 from bi_business_brand_new n where n.business_id={business_id} and d.category_class=n.CategoryClass and d.is_online=n.is_online 
                )
                group by product_sn,golden_triangle ,fixed_price,product_style
                )a left join bi_product_img i on a.product_sn=i.product_sn
    """
        ret = Scope['bi_saas'].execute(sql, {'doc_year': doc_year, 'doc_season': doc_season, 'cat': cat})
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

        sql_total = f""" 
                    select sum(a.last_year_designer_dev_product_nums) as last_year_designer_dev_product_nums,sum(a.last_year_actual_purchase_product_nums) as last_year_actual_purchase_product_nums,
                    sum(b.last_year_product_dev_plan) as last_year_product_dev_plan from (
                    SELECT SUM(designer_dev_product_nums) as last_year_designer_dev_product_nums,SUM(actual_purchase_product_nums) as last_year_actual_purchase_product_nums,category_class 
                    FROM hmcdata.bi_product_season_plan_category cc
                    where doc_year={int(doc_year)-1} and doc_season =:doc_season and category=:category
                    and exists (
                        select 1 from bi_business_brand_new n where n.business_id={business_id} and cc.category_class=n.CategoryClass and cc.is_online=n.is_online 
                    )
                    )a left join (									
                    SELECT category_class,SUM(product_dev_plan) as last_year_product_dev_plan FROM hmcdata.bi_product_season_plan_base bb 
                    where doc_year={int(doc_year)-1} and doc_season =:doc_season and category=:category
                    and exists (
                        select 1 from bi_business_brand_new n where n.business_id={business_id} and bb.category_class=n.CategoryClass and bb.is_online=n.is_online 
                    )
                    )b on a.category_class=b.category_class

"""
        all_data = []
        ret_total = Scope['bi_saas'].execute(sql_total,
                                             {'doc_year': doc_year, 'doc_season': doc_season, 'category': cat})
        all_columns = ret_total.keys()

        for val in ret_total:
            data_dict = {}
            for i, column in enumerate(all_columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_num(val[i], 4)
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_num(val[i], 4)
                elif isinstance(val[i], datetime.date):
                    data_dict[column] = date_format(val[i])
                elif val[i] is None:
                    data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            all_data.append(data_dict)
        context['data'] = data
        if len(all_data) == 1:
            context['total'] = all_data[0]
        else:
            context['total'] = {}

        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()
