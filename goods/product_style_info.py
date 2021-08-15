# -*- coding: utf-8 -*-
# @Author  : Joshua
# @File    : product_style_info
from flask import jsonify, request
from bi_flask.__token import __token_wrapper
import logging
import json
from bi_flask._sessions import sessions, sessions_scopes
from bi_flask.utils import *
from bi_flask.goods.api import good

logger = logging.getLogger('bi')
Scope = sessions_scopes(sessions)


@good.route('/get_category_class_product_style_info', methods=['GET'], endpoint='get_category_class_product_style_info')
@__token_wrapper
def get_category_class_product_style_info(context):
    '''
        获取年度风格线企划明细
        :param context:
        :return:
        '''
    try:
        business_id = request.args.get('business_id')
        doc_year = request.args.get('year')
        product_style = request.args.get('product_style')
        big_style = request.args.get('big_style')
        season = request.args.get('season')
        filter = request.args.get('filter')

        if product_style is not None and product_style != "其他":
            product_style_sql = f""" and d.product_style='{product_style}' """
        elif product_style == "其他":
            product_style_sql = f""" and d.product_style is null """
        else:
            product_style_sql = ""
        if big_style is not None and big_style != "其他":
            big_style_sql = f""" and d.big_style='{big_style}' """
        elif big_style == "其他":
            big_style_sql = f""" and d.big_style is null """
        else:
            big_style_sql = ""

        if filter is None or filter == '':
            context['data'] = []
            context['total'] = {'spring': 0, 'summer': 0, 'autumn': 0, 'winter': 0}
            return jsonify(context)
        if 'OEM' in filter and 'ODM' in filter:
            oem_sql = """ """
        elif 'OEM' in filter:
            oem_sql = """ and d.is_ODM=0 """
        elif 'ODM' in filter:
            oem_sql = """ and d.is_ODM=1 """
        else:
            oem_sql = """ """
        if '延续款' in filter:
            if 'OEM' in filter or 'ODM' in filter:
                product_sql = """  """
            else:
                product_sql = """ and d.tag='延续款' """
        else:
            product_sql = """ and d.tag<>'延续款' """

        sql = f"""
                select a.*,i.img_src from (
                SELECT doc_season,product_sn,sum(stock_nums+production_onway_nums) as total_stock,sum(planorder_nums) as planorder_nums,fixed_price,
                ifnull(product_style,'未知') as product_style,ifnull(big_style,'未知') as big_style FROM hmcdata.bi_product_season_plan_category_detail d
                where doc_year=:doc_year and doc_season=:doc_season {product_style_sql} {big_style_sql} {oem_sql} {product_sql}  and exists (
                  select 1 from bi_business_brand_new n where n.business_id={business_id} and d.category_class=n.CategoryClass and d.is_online=n.is_online 
                )
                group by product_sn,fixed_price,product_style,doc_season,big_style
                )a left join bi_product_img i on a.product_sn=i.product_sn
    """
        ret = Scope['bi_saas'].execute(sql, {'doc_year': doc_year, 'business_id': business_id, 'doc_season': season})
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


@good.route('/get_category_class_product_style_plan', methods=['GET'], endpoint='get_category_class_product_style_plan')
@__token_wrapper
def get_category_class_product_style_plan(context):
    '''
        获取年度风格线企划明细
        :param context:
        :return:
        '''
    try:
        business_id = request.args.get('business_id')
        doc_year = request.args.get('year')
        filter = request.args.get('filter')
        if filter is None or filter == '':
            context['data'] = []
            context['total'] = {'spring': 0, 'summer': 0, 'autumn': 0, 'winter': 0}
            return jsonify(context)
        if 'OEM' in filter and 'ODM' in filter:
            oem_sql = """ """
        elif 'OEM' in filter:
            oem_sql = """ and d.is_ODM=0 """
        elif 'ODM' in filter:
            oem_sql = """ and d.is_ODM=1 """
        else:
            oem_sql = """ """
        if '延续款' in filter:
            if 'OEM' in filter or 'ODM' in filter:
                product_sql = """  """
            else:
                product_sql = """ and d.tag='延续款' """
        else:
            product_sql = """ and d.tag<>'延续款' """

        sql = f"""
                select a.big_style,a.product_style,a.doc_season,a.plan_nums,ifnull(b.product_count,0) as product_count FROM (
                select big_style,product_style,doc_season ,sum(plan_nums) as plan_nums FROM hmcdata.bi_product_style_plan d where  doc_year={doc_year} 
                and exists (
                    select 1 from bi_business_brand_new n where n.business_id={business_id} and d.category_class=n.CategoryClass and d.is_online=n.is_online 
                )
                GROUP by big_style,product_style,doc_season 
                )a
                left join(
                SELECT ifnull(big_style,'其他') as big_style ,ifnull(product_style,'其他') as product_style,count(DISTINCT product_sn) as product_count,doc_season 
                FROM hmcdata.bi_product_season_plan_category_detail d where doc_year={doc_year} {oem_sql} {product_sql}
                and exists (
                    select 1 from bi_business_brand_new n where n.business_id={business_id} and d.category_class=n.CategoryClass and d.is_online=n.is_online 
                )
                GROUP by big_style ,product_style,doc_season
                )b on a.big_style=b.big_style and a.product_style=b.product_style and a.doc_season=b.doc_season
                UNION 
                select b.big_style,b.product_style,b.doc_season,ifnull(a.plan_nums,0) as plan_nums,b.product_count FROM (
                select big_style,product_style,doc_season ,sum(plan_nums) as plan_nums FROM hmcdata.bi_product_style_plan d where  doc_year={doc_year} 
                and exists (
                    select 1 from bi_business_brand_new n where n.business_id={business_id} and d.category_class=n.CategoryClass and d.is_online=n.is_online 
                )
                GROUP by big_style,product_style,doc_season 
                )a
                right join(
                SELECT ifnull(big_style,'其他') as big_style ,ifnull(product_style,'其他') as product_style,count(DISTINCT product_sn) as product_count,doc_season 
                FROM hmcdata.bi_product_season_plan_category_detail d where doc_year={doc_year}  {oem_sql} {product_sql}
                and exists (
                    select 1 from bi_business_brand_new n where n.business_id={business_id} and d.category_class=n.CategoryClass and d.is_online=n.is_online 
                )
                GROUP by big_style ,product_style,doc_season
                )b on a.big_style=b.big_style and a.product_style=b.product_style and a.doc_season=b.doc_season;
    """
        ret = Scope['bi_saas'].execute(sql, {'doc_year': doc_year, 'business_id': business_id})
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
        hash_dict = {}
        total_obj = {'spring': 0, 'summer': 0, 'autumn': 0, 'winter': 0}
        for val in data:
            if val['big_style'] in hash_dict:
                hash_dict[val['big_style']].append(val)
            else:
                hash_dict[val['big_style']] = []
                hash_dict[val['big_style']].append(val)

            if val['doc_season'] == '春季':
                total_obj['spring'] += val['product_count']
            elif val['doc_season'] == '夏季':
                total_obj['summer'] += val['product_count']
            elif val['doc_season'] == '秋季':
                total_obj['autumn'] += val['product_count']
            elif val['doc_season'] == '冬季':
                total_obj['winter'] += val['product_count']

        context['data'] = hash_dict
        context['total'] = total_obj

        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()
