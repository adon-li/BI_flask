# -*- coding: utf-8 -*-
# @Author  : Joshua
# @File    : target_achievement

from flask import jsonify, request
from bi_flask.__token import __token_wrapper
import logging
import json
from bi_flask._sessions import sessions, sessions_scopes
from bi_flask.utils import *
from bi_flask.goods.api import good

logger = logging.getLogger('bi')
Scope = sessions_scopes(sessions)


@good.route('/get_target_achievement_month', methods=['GET'], endpoint='get_target_achievement_month')
@__token_wrapper
def get_target_achievement_month(context):
    '''
        获取业务线月目标达成情况
        :param context:
        :return:
        '''
    try:
        business_id = request.args.get('business_id')
        total_year = request.args.get('total_year')

        sql = f"""
                select actual_year,actual_month,cost_target,avg(gross_profit_target) as gross_profit_target,sum(origin_amount) as origin_amount,sum(return_cost) as return_cost,sum(return_nums) as return_nums,
                sum(sale_amount) as sale_amount,sum(sale_cost) as sale_cost,
                sum(sale_nums) as sale_nums,sum(sale_target) as sale_target,tag from bi_product_category_class_target_achievement_month t where actual_year=:total_year
                and business_id=:business_id
                group by actual_year,actual_month,tag
        """
        ret = Scope['bi_saas'].execute(sql,
                                       {'total_year': total_year,'business_id':business_id})
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


@good.route('/get_target_achievement_week', methods=['GET'], endpoint='get_target_achievement_week')
@__token_wrapper
def get_target_achievement_week(context):
    '''
        获取业务线周目标达成情况
        :param context:
        :return:
        '''
    try:
        business_id = request.args.get('business_id')
        total_year = request.args.get('total_year')
        sql = f"""
                select total_year,total_week,cost_target,avg(gross_profit_target) as gross_profit_target,sum(origin_amount) as origin_amount,sum(return_cost) as return_cost,sum(return_nums) as return_nums,
                sum(sale_amount) as sale_amount,sum(sale_cost) as sale_cost,
                sum(sale_nums) as sale_nums,sum(sale_target) as sale_target,tag from bi_product_category_class_target_achievement_week t where total_year=:total_year
                and business_id=:business_id
                group by total_year,total_week,tag
                """

        ret = Scope['bi_saas'].execute(sql,
                                       {'total_year': total_year,'business_id':business_id})
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
