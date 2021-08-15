# -*- coding: utf-8 -*-
# @Author  : Joshua
# @File    : season_battle
from flask import jsonify, request
from bi_flask.__token import __token_wrapper
import logging
import json
from bi_flask._sessions import sessions, sessions_scopes
from bi_flask.utils import *
from bi_flask.datawall.api import datawall

logger = logging.getLogger('bi')
Scope = sessions_scopes(sessions)


@datawall.route('/get_month_sales_today', methods=['GET'], endpoint='get_month_sales_today')
@__token_wrapper
def get_month_sales_today(context):
    '''
        获取业务线月目标达成情况
        :param context:
        :return:
        '''
    try:
        sql = f"""
            SELECT c.price_sale,d.targetNum,c.business_clazz_id FROM(
 SELECT SUM(a.price_sale) as price_sale
        ,a.business_clazz_id FROM (
        SELECT totalday,totalhour,MAX(price_sale) as price_sale,MAX(order_count) as order_count,MAX(sale_cost) as sale_cost,sh.shop_name,dc_shop_id,sh.business_clazz_id FROM bi_shop_saleinfo_hour_today t
				        LEFT JOIN bi_shops_new sh on t.dc_shop_id=sh.shop_id
        WHERE totalday=DATE(NOW()) and sh.business_clazz_id in(4,50)
        GROUP BY totalday,totalhour,sh.shop_name,dc_shop_id,sh.business_clazz_id
        )a
        GROUP BY a.business_clazz_id
				)c left join (
				 SELECT sum(targetNum) as targetNum,s.business_clazz_id FROM bi_sdb_dist_report_target_day  t
            LEFT JOIN bi_shops_new s on t.dc_shop_id=s.shop_id
            WHERE t.business_unit_id=1 and s.business_clazz_id in(50)
			AND targetDay=CURRENT_DATE
			)d on c.business_clazz_id=d.business_clazz_id
			union 
			SELECT ifnull(c.price_sale,0) as price_sale,d.targetNum,c.business_clazz_id FROM(
 SELECT SUM(a.price_sale) as price_sale
        ,a.business_clazz_id FROM (
        SELECT totalday,totalhour,MAX(price_sale) as price_sale,MAX(order_count) as order_count,MAX(sale_cost) as sale_cost,sh.shop_name,dc_shop_id,sh.business_clazz_id FROM bi_shop_saleinfo_hour_today t
				        LEFT JOIN bi_shops_new sh on t.dc_shop_id=sh.shop_id
        WHERE totalday=DATE(NOW()) and sh.business_clazz_id in(4,50)
        GROUP BY totalday,totalhour,sh.shop_name,dc_shop_id,sh.business_clazz_id
        )a
        GROUP BY a.business_clazz_id
				)c right join (
				 SELECT sum(targetNum) as targetNum,s.business_clazz_id FROM bi_sdb_dist_report_target_day  t
            LEFT JOIN bi_shops_new s on t.dc_shop_id=s.shop_id
            WHERE t.business_unit_id=1 and s.business_clazz_id in(50)
			AND targetDay=CURRENT_DATE
			)d on c.business_clazz_id=d.business_clazz_id
        """
        ret = Scope['bi_saas'].execute(sql)
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


@datawall.route('/get_month_sales', methods=['GET'], endpoint='get_month_sales')
@__token_wrapper
def get_month_sales(context):
    '''
        获取业务线月目标达成情况
        :param context:
        :return:
        '''
    try:
        year = datetime.datetime.now().year
        month = datetime.datetime.now().month
        firstDay = datetime.date(year=year, month=month, day=1)
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        sql = f"""
           SELECT a.price_sale,b.targetNum,a.business_clazz_id from (
				 SELECT sum(t.sale) as price_sale,sh.business_clazz_id  
        FROM (SELECT SUM(price_sale) AS sale, dc_shop_id FROM bi_shop_saleinfo_hour_nottoday n
        WHERE totalday>= '{firstDay.strftime('%Y-%m-%d')}' AND totalday <= '{today}'
        GROUP BY dc_shop_id ) t 
        LEFT JOIN bi_shops_new sh on t.dc_shop_id=sh.shop_id
				where  sh.business_clazz_id in(4,50)
        GROUP BY sh.business_clazz_id
				)a left join(
				SELECT sum(targetNum) as targetNum, business_clazz_id FROM bi_target_month t
        LEFT JOIN bi_shops_new sh on t.dc_shop_id=sh.shop_id
        WHERE total_year={year} AND total_month ={month} and sh.business_clazz_id in(4,50)
        GROUP BY sh.business_clazz_id
				)b on a.business_clazz_id=b.business_clazz_id
				union
				SELECT a.price_sale,b.targetNum,a.business_clazz_id from (
				 SELECT sum(t.sale) as price_sale,sh.business_clazz_id  
        FROM (SELECT SUM(price_sale) AS sale, dc_shop_id FROM bi_shop_saleinfo_hour_nottoday n
        WHERE totalday>= '{firstDay}' AND totalday <= '{today}'
        GROUP BY dc_shop_id ) t 
        LEFT JOIN bi_shops_new sh on t.dc_shop_id=sh.shop_id
				where  sh.business_clazz_id in(4,50)
        GROUP BY sh.business_clazz_id
				)a right join(
				SELECT sum(targetNum) as targetNum, business_clazz_id FROM bi_target_month t
        LEFT JOIN bi_shops_new sh on t.dc_shop_id=sh.shop_id
        WHERE total_year={year} AND total_month ={month} and sh.business_clazz_id in(4,50)
        GROUP BY sh.business_clazz_id
				)b on a.business_clazz_id=b.business_clazz_id;
        """
        ret = Scope['bi_saas'].execute(sql)
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


@datawall.route('/get_season_sales', methods=['GET'], endpoint='get_season_sales')
@__token_wrapper
def get_season_sales(context):
    '''
        获取业务线季度目标达成情况
        :param context:
        :return:
        '''
    try:
        business_id = request.args.get('business_id')
        sql = f"""
           SELECT c.total_month,b.price_sale,c.targetNum from(
                SELECT total_month,SUM(a.price_sale) as price_sale,sh.business_clazz_id FROM (
                SELECT total_month,SUM(price_sale) as price_sale,dc_shop_id FROM bi_shop_saleinfo_hour_nottoday
                WHERE total_year=2021 and total_month in(4,5,6)
                GROUP BY dc_shop_id,total_month
                )a
                LEFT JOIN bi_shops_new sh on a.dc_shop_id=sh.shop_id
                WHERE sh.business_clazz_id =:business_id
                GROUP BY 
                total_month,business_clazz_id
                )b
                RIGHT JOIN(
                SELECT total_month,sum(targetNum) as targetNum, business_clazz_id FROM bi_target_month t
                LEFT JOIN bi_shops_new sh on t.dc_shop_id=sh.shop_id
                WHERE total_year=2021 AND business_clazz_id=:business_id and total_month in(4,5,6)
                GROUP BY sh.business_clazz_id,total_month
                )c ON b.total_month=c.total_month AND b.business_clazz_id=c.business_clazz_id
                order by c.total_month asc
        """
        ret = Scope['bi_saas'].execute(sql, {'business_id': business_id})
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
