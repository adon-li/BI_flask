# -*- coding: utf-8 -*-
# @Time    : 2021/4/14 11:43
# @Author  : Joshua
# @File    : transfer_year_compare

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


@good.route('/get_transfer_year_compare', methods=['GET'], endpoint='get_transfer_year_compare')
@__token_wrapper
def get_transfer_year_compare(context):
    '''
        获取调拨年份对比
        :param context:
        :return:
        '''
    try:
        business_unit_id = context['data']['businessUnitId']
        category = request.args.get('category')
        season = request.args.get('season')
        assign_type = request.args.get('assignType')
        if category is None or category == '':
            category_sql = ''
        else:
            category_sql = f""" and category='{category}' """
        if season is None or season == '':
            season_sql = ''
        else:
            season_sql = f""" and product_season='{season}' """
        if assign_type is None or assign_type == '' or (isinstance(assign_type, int) and int(assign_type) >= 3):
            assign_sql = """"""
        else:
            assign_sql = f"""and assign_type ={assign_type} """
        sql = f"""
                      SELECT w.define_year ,w.define_week ,sum(received_quantity) as quantity FROM hmcdata.bi_dist_model_transfer_base b
                        left join hmcdata.bi_week w on date(b.create_time)=w.total_day 
                        where status ='已收货' AND business_unit_id =:business_unit_id and w.define_year>=year(now())-3 {category_sql} {season_sql} {assign_sql}
                        GROUP BY define_year,define_week 
                        order by define_week asc 
                        """
        ret = Scope['vertica'].execute(sql, {'business_unit_id': business_unit_id})
        data_dict = {}
        for curr_year, curr_week, quantity in ret:
            if curr_year in data_dict.keys():
                data_dict[curr_year].update({curr_week: (quantity)})
            else:
                data_dict[curr_year] = {curr_week: (quantity)}
        context['data'] = data_dict
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_transfer_year_compare_category', methods=['GET'], endpoint='get_transfer_year_compare_category')
@__token_wrapper
def get_transfer_year_compare_category(context):
    '''
        获取调拨年份类目对比
        :param context:
        :return:
        '''
    try:
        business_unit_id = context['data']['businessUnitId']
        category = request.args.get('category')
        season = request.args.get('season')
        assign_type=request.args.get('assignType')
        if category is None or category == '':
            category_sql = ''
        else:
            category_sql = f""" and category='{category}' """
        if season is None or season == '':
            season_sql = ''
        else:
            season_sql = f""" and product_season='{season}' """
        if assign_type is None or assign_type == '' or (isinstance(assign_type, int) and int(assign_type) >= 3):
            assign_sql = """"""
        else:
            assign_sql = f"""and assign_type ={assign_type} """
        sql = f"""
                SELECT w.define_year,category,sum(received_quantity) as quantity FROM hmcdata.bi_dist_model_transfer_base b
                left join hmcdata.bi_week w on date(b.create_time)=w.total_day 
                where status ='已收货' AND business_unit_id =:business_unit_id and w.define_year>=year(now())-3 {category_sql} {season_sql} {assign_sql}
                GROUP BY define_year,category
                """
        ret = Scope['vertica'].execute(sql, {'business_unit_id': business_unit_id})
        data_dict = {}
        for curr_year, category, quantity in ret:
            if curr_year in data_dict.keys():
                data_dict[curr_year].update({category: (quantity)})
            else:
                data_dict[curr_year] = {category: (quantity)}
        context['data'] = data_dict
        sql_category = f"""SELECT category FROM hmcdata.bi_dist_model_transfer_base b
                left join hmcdata.bi_week w on date(b.create_time)=w.total_day 
                where status ='已收货' AND business_unit_id =:business_unit_id and w.define_year>=year(now())-3 {category_sql} {season_sql} {assign_sql}
                GROUP BY category"""
        ret_category = Scope['vertica'].execute(sql_category, {'business_unit_id': business_unit_id})
        data = []
        for category in ret_category:
            data.append(category[0])
        context['category'] = data
        return jsonify(context)
    finally:
        Scope['vertica'].remove()



@good.route('/get_week_date', methods=['GET'], endpoint='get_week_date')
@__token_wrapper
def get_week_date(context):
    '''
        获取今年周所对应日期
        :param context:
        :return:
        '''
    try:

        sql = f"""
                SELECT define_week,to_char(MAX(total_day),'MM.DD') as max_week_day,to_char(MIN(total_day),'MM.DD') as min_week_day
                FROM hmcdata.bi_week 
                where define_year =year(now()) group by define_year,define_week
                ORDER by define_week
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
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)
        context['data'] = data
        return jsonify(context)
    finally:
        Scope['vertica'].remove()