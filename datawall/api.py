#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/05/02
# @Author  : kingsley kwong
# @Site    :
# @File    : shop\api.py.bak.bai
# @Software: BI 不漏 flask
# @Function:

from flask import jsonify, Blueprint, request
from bi_flask.__token import __token_wrapper
import logging
from bi_flask._sessions import sessions_scopes, sessions, get_cache
from bi_flask.utils import *
import os
import importlib
from . import datawall

logger = logging.getLogger('bi')
Scope = sessions_scopes(sessions)
cache = get_cache()


@datawall.route('/getBranchAmtAndFlow', methods=['GET'], endpoint='getBranchAmtAndFlow')
@__token_wrapper
def getBranchAmtAndFlow(context):
    try:
        ret = Scope['bi_saas'].execute(''' SELECT st_date,shop_name,MAX(pay_trade_amt),ifnull(MAX(uv_visitors_cnt), 0) FROM st_op_shop_online_everymin where 
st_date=DATE(NOW())
GROUP BY st_date,shop_name
ORDER BY MAX(pay_trade_amt) DESC
LIMIT 10 ''')  # DATE(NOW())
        data = []
        for value in ret:
            record = {}
            record['title'] = value[1]
            record['payAmt'] = int(value[2]) if value[2] is not None else 0
            record['uv'] = int(value[3]) if value[3] is not None else 0
            data.append(record)
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
        # SessionO2O.remove()


@datawall.route('/getPayment', methods=['GET'], endpoint='getPayment')
@__token_wrapper
def getPayment(context):
    try:
        ret_jd = Scope['bi_saas'].execute(''' SELECT SUM(a.pay_trade_amt_all) FROM
(
SELECT MAX(pay_trade_amt) as pay_trade_amt_all,st_date,platform,shop_name FROM st_op_shop_online_everymin
WHERE platform='京东' AND st_date=DATE(NOW())
GROUP BY platform,shop_name,st_date
) a''')
        ret_njd = Scope['bi_saas'].execute(''' SELECT SUM(a.pay_trade_amt_all),SUM(uv_visitors_cnt_all) FROM
(
SELECT MAX(pay_trade_amt) as pay_trade_amt_all,MAX(uv_visitors_cnt) as uv_visitors_cnt_all,platform,shop_name FROM st_op_shop_online_everymin WHERE st_date=DATE(NOW()) AND platform<>'京东'
GROUP BY platform,shop_name
) a ''')
        data = {}
        for value in ret_jd:
            jd_amt = int(value[0]) if value[0] is not None else 0

        for value in ret_njd:
            njd_amt = int(value[0]) if value[0] is not None else 0
            uv = int(value[1]) if value[1] is not None else 0

        data['payAmt'] = jd_amt + njd_amt
        data['uv'] = uv
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
        # SessionO2O.remove()


@datawall.route('/geto2o', methods=['GET'], endpoint='geto2o')
@__token_wrapper
def geto2o(context):
    try:
        # data = cache.get(f'o2o_sales_hourly_1')
        # # print(json.loads(data))
        # a = json.loads(data)
        # print(a)
        # return jsonify(context)

        today = datetime.date.today()
        sql = """SELECT shop_id, SUM(final_amount) as final_amount FROM trade_sales_order 
            WHERE type=1 AND is_swap=0 AND pay_status=1 AND order_status<>3
            AND pay_time >= DATE(NOW()) AND business_unit_id = 1
            AND shop_id<>96
            GROUP BY shop_id
            ORDER BY final_amount DESC LIMIT 10"""

        ret = Scope['hmc_trade_center_read'].execute(sql)
        shop_data = {}
        shop_id = []
        for val1, val2 in ret:
            # data.append({'shop_id':val1,'final_amount':val2})
            shop_data[f'{val1}'] = format_(val2)
            shop_id.append(str(val1))

        top10_data = []
        if len(shop_id) > 0:
            shop_id_list = ','.join(shop_id)
            sql = f''' SELECT id,name from member_shop where id in({shop_id_list});
                          '''
            retm = Scope['hmc_member_center'].execute(sql)
            for shop_id, shop_name in retm:
                shop = {}
                shop['shopname'] = shop_name
                shop['payAmt'] = shop_data[f'{shop_id}']
                top10_data.append({"shopname": shop_name, "payAmt": shop_data[f"{shop_id}"]})
                # shop.append({"shop_name": shop_name, "final_amount": data[f'{shop_id}']})
        data = {}
        # top10_data = []
        # for value in ret1:
        #     shop = {}
        #     shop['shopname'] = value[1]
        #     shop['payAmt'] = int(value[2]) if value[2] is not None else 0
        #     top10_data.append(shop)
        #        ret2 = SessionO2O.execute(f''' SELECT ifnull(SUM(d.nums*d.price), 0) AS 'total_amount',ifnull(COUNT(DISTINCT a.shop_id), 0)
        # FROM sdb_dist_split_orders a,sdb_dist_split_order_items d,sdb_dist_shops c WHERE a.order_id=d.order_id AND a.pay_status='1' AND a.status<>'dead' AND d.item_type='product' AND a.shop_id=c.shop_id
        # AND a.confirm_time BETWEEN {time.mktime(time.strptime(datetime.datetime.today().strftime('%Y-%m'), '%Y-%m'))} AND {time.mktime(time.strptime((datetime.datetime.today()+datetime.timedelta(days=1)).strftime('%Y-%m-%d'), '%Y-%m-%d'))}  ''')
        sql = f"""SELECT SUM(o.final_amount),count(*) from trade_sales_order o WHERE o.order_status<>3 AND o.pay_status=1 AND o.type=1 
        AND o.is_swap=0 AND o.pay_time>='{today}'"""
        ret2 = Scope['hmc_trade_center_read'].execute(sql)
        # print(top10_data)
        for value in ret2:
            data['o2opayment'] = int(value[0]) if value[0] is not None else 0
            data['dealCount'] = int(value[1]) if value[1] is not None else 0
        data['top10'] = top10_data
        # print(data)
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
        # SessionO2O.remove()
        Scope['hmc_member_center'].remove()
        Scope['hmc_trade_center_read'].remove()


@datawall.route('/geto2oSales', methods=['GET'], endpoint='geto2oSales')
@__token_wrapper
def geto2oSales(context):
    try:
        today = datetime.date.today()
        sql = f"""SELECT SUM(o.final_amount) as final_amount from trade_sales_order o WHERE o.order_status<>3 AND o.pay_status=1 AND o.type=1 AND o.is_swap=0 
AND o.pay_time>='{today}'"""
        ret = Scope['hmc_trade_center_read'].execute(sql)
        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_0(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_0(val[i])
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
        Scope['hmc_trade_center_read'].remove()


@datawall.route('/getPaymentTrend', methods=['GET'], endpoint='getPaymentTrend')
@__token_wrapper
def getPaymentTrend(context):
    try:
        ret = Scope['bi_saas'].execute(''' SELECT SUM(a.pay_trade_amt_all),ifnull(SUM(uv_visitors_cnt_all), 0),a.h FROM
(
SELECT MAX(pay_trade_amt) as pay_trade_amt_all,MAX(uv_visitors_cnt) as uv_visitors_cnt_all,platform,shop_name,HOUR(gmt_created) as h FROM st_op_shop_online_everymin WHERE st_date=DATE(NOW()) 
GROUP BY platform,shop_name,HOUR(gmt_created)
) a
GROUP BY a.h ''')  #
        payAmt = []
        uv = []
        for value in ret:
            payAmt.append(int(value[0]) if value[0] is not None else 0)
            uv.append(int(value[1]) if value[1] is not None else 0)

        context['data'] = {'payAmt': payAmt, 'uv': uv}
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
        # SessionO2O.remove()


@datawall.route('/getTop3', methods=['GET'], endpoint='getTop3')
@__token_wrapper
def getTop3(context):
    try:
        ret = Scope['bi_saas'].execute(''' SELECT st_date,shop_name,MAX(pay_trade_amt) FROM st_op_shop_online_everymin where 
st_date=DATE(NOW())
GROUP BY st_date,shop_name
ORDER BY MAX(pay_trade_amt) DESC
LIMIT 3 ''')
        data = []

        for value in ret:
            record = {}
            record['payAmt'] = int(value[2]) if value[2] is not None else 0
            record['shopName'] = value[1]
            data.append(record)
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
        # SessionO2O.remove()


@datawall.route('/getyuduProvinceData', methods=['GET'], endpoint='getyuduProvinceData')
@__token_wrapper
def getyuduProvinceData(context):
    try:
        # ret = Scope['bi_saas'].execute(''' SELECT SUM(pay_trade_amt),REPLACE(province,'省','') as p FROM st_op_shop_online_region GROUP BY p ''')
        ret = Scope['bi_saas'].execute("""SELECT SUM(pay_trade_amt),CASE REPLACE(province,'省','')
	WHEN '内蒙古自治区' THEN
		'内蒙古'
	WHEN '宁夏回族自治区' THEN
		'宁夏'
	WHEN '广西壮族自治区' THEN
		'广西'
	WHEN '新疆维吾尔自治区' THEN
		'新疆'
	WHEN '西藏自治区' THEN 
		'西藏'
	ELSE REPLACE(province,'省','')
	END 
as p FROM st_op_shop_online_region GROUP BY p ORDER BY SUM(pay_trade_amt) DESC""")

        data = []

        for value in ret:
            record = {}
            record['value'] = float(value[0]) if value[0] is not None else 0
            record['name'] = value[1]
            data.append(record)

        result = {}
        result["all"] = data
        context['data'] = result
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


@datawall.route('/getShopRegion', methods=['GET'], endpoint='getShopRegion')
@__token_wrapper
def getShopRegion(context):
    try:
        business_id = request.args.get('business_id')
        shop_id = request.args.get('shop_id')
        if shop_id is None:
            ret = Scope['bi_saas'].execute(f"""SELECT SUM(price_sale) as `value`,CASE REPLACE(region_name,'省','')
                WHEN '内蒙古自治区' THEN
                    '内蒙古'
                WHEN '宁夏回族自治区' THEN
                    '宁夏'
                WHEN '广西壮族自治区' THEN
                    '广西'
                WHEN '新疆维吾尔自治区' THEN
                    '新疆'
                WHEN '西藏自治区' THEN 
                    '西藏'
                ELSE REPLACE(region_name,'省','')
                END 
            as `name` FROM bi_shop_saleinfo_region
            WHERE business_clazz_id={business_id}
            GROUP BY `name` ORDER BY `value` DESC""")
        else:
            ret = Scope['bi_saas'].execute(f"""SELECT SUM(price_sale) as `value`,CASE REPLACE(region_name,'省','')
                            WHEN '内蒙古自治区' THEN
                                '内蒙古'
                            WHEN '宁夏回族自治区' THEN
                                '宁夏'
                            WHEN '广西壮族自治区' THEN
                                '广西'
                            WHEN '新疆维吾尔自治区' THEN
                                '新疆'
                            WHEN '西藏自治区' THEN 
                                '西藏'
                            ELSE REPLACE(region_name,'省','')
                            END 
                        as `name` FROM bi_shop_saleinfo_region
                        WHERE dc_shop_id={shop_id}
                        GROUP BY `name` """)

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


@datawall.route('/getyuduPayment', methods=['GET'], endpoint='getyuduPayment')
@__token_wrapper
def getPayment(context):
    try:
        ret_jd = Scope['vertica'].execute("""SELECT COUNT(*)+2120 FROM hmcdata.e3_order_info WHERE order_status<>3 AND DATE(to_timestamp(shipping_time_fh))=DATE(NOW())
""")
        for value in ret_jd:
            pack = int(value[0]) if value[0] is not None else 0
        #         ret_njd = Scope['bi_saas'].execute(''' SELECT SUM(a.pay_trade_amt_all),SUM(uv_visitors_cnt_all),SUM(pay_order_cnt) FROM
        # (
        # SELECT MAX(pay_trade_amt) as pay_trade_amt_all,MAX(uv_visitors_cnt) as uv_visitors_cnt_all,MAX(pay_order_cnt) as pay_order_cnt,platform,shop_name FROM st_op_shop_online_everymin WHERE st_date=DATE(NOW())
        # GROUP BY platform,shop_name
        # ) a''')
        ret_njd = Scope['bi_saas'].execute('''	SELECT SUM(a.price_sale) as payAmt,MAX(uv) as 'uv_visitors_cnt_all',SUM(pay_order_cnt) FROM (
    SELECT totalday,totalhour,MAX(price_sale) as price_sale,t.shop_name,MAX(order_count) as pay_order_cnt,MAX(uv) as uv FROM bi_shop_saleinfo_hour_today t
    LEFT JOIN bi_shops_new s on s.shop_id=t.dc_shop_id
    WHERE totalday=DATE(NOW()) and s.business_clazz_id not in(50,51,52,60,61)
    GROUP BY totalday,totalhour,t.shop_name
    )a''')
        data = {}
        # for value in ret_jd:
        #     jd_amt = int(value[0]) if value[0] is not None else 0

        for value in ret_njd:
            njd_amt = int(value[0]) if value[0] is not None else 0
            uv = int(value[1]) if value[1] is not None else 0
            pay_order_cnt = int(value[2]) if value[2] is not None else 0

        data['payAmt'] = njd_amt
        data['uv'] = uv
        data['pay_order_cnt'] = pay_order_cnt
        data['pack'] = pack
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
        # SessionO2O.remove()
        Scope['vertica'].remove()


@datawall.route('/geto2ojiangxi', methods=['GET'], endpoint='geto2ojiangxi')
@__token_wrapper
def geto2ojiangxi(context):
    try:
        ret1 = Scope['bi_saas'].execute(
            f'''SELECT city,SUM(pay_trade_amt) FROM st_op_shop_jiangxi_city GROUP BY city ORDER BY SUM(pay_trade_amt) DESC''')
        data = []
        for value in ret1:
            shop = {}
            shop['city'] = value[0]
            shop['payAmt'] = float(value[1]) if value[1] is not None else 0
            data.append(shop)
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
        # SessionO2O.remove()


@datawall.route('/getAllPaymentTrend', methods=['GET'], endpoint='getAllPaymentTrend')
@__token_wrapper
def getAllPaymentTrend(context):
    try:
        #         ret = Scope['bi_saas'].execute(''' SELECT SUM(pay_trade_amt),hour_pay FROM st_op_shop_hour_day WHERE st_date=DATE(NOW()) GROUP BY hour_pay
        # ORDER BY hour_pay asc''')
        #
        business_id = request.args.get('business_id')
        shop_id = request.args.get('shop_id')
        if business_id is None:
            businessSql = ""
        else:
            businessSql = f""" AND s.business_clazz_id={business_id}"""
        if shop_id is None:
            ret = Scope['bi_saas'].execute(f''' SELECT SUM(a.price_sale) as price_sale,a.totalhour FROM (
        SELECT totalday,totalhour,MAX(price_sale) as price_sale,t.shop_name FROM bi_shop_saleinfo_hour_today t
        LEFT JOIN bi_shops_new s on s.shop_id=t.dc_shop_id
            LEFT JOIN bi_business_info info on s.business_clazz_id=info.business_id
        WHERE totalday=DATE(now()) and info.isshow=1 {businessSql}  and t.src_business_id<>'天猫爬虫'
        GROUP BY totalday,totalhour,t.shop_name
        )a
        GROUP BY a.totalday,a.totalhour  ''')
        else:
            ret = Scope['bi_saas'].execute(f''' SELECT SUM(a.price_sale) as price_sale,a.totalhour FROM (
            SELECT totalday,totalhour,MAX(price_sale) as price_sale,t.shop_name FROM bi_shop_saleinfo_hour_today t
            WHERE totalday=DATE(now()) AND t.dc_shop_id={shop_id}
            GROUP BY totalday,totalhour,t.shop_name
            )a
            GROUP BY a.totalday,a.totalhour  ''')
        today = []

        for value in ret:
            d = {}
            d["pay_trade_amt"] = float(value[0]) if value[0] is not None else 0
            d["hour"] = int(value[1]) if value[1] is not None else 0
            today.append(d)

        context['data'] = {'today': today}
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
        # SessionO2O.remove()


@datawall.route('/getAllPaymentTrendTM', methods=['GET'], endpoint='getAllPaymentTrendTM')
@__token_wrapper
def getAllPaymentTrendTM(context):
    try:
        # yesterday = []
        # data={}
        ret = Scope['bi_saas'].execute('''SELECT total_hour,sum(price_sale) as price_sale FROM bi_saleinfo_hour_data t
        WHERE  totalday=date_sub(DATE(NOW()),INTERVAL 1 year)
        GROUP BY total_hour ORDER BY total_hour ASC''')

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


@datawall.route('/getTOP10', methods=['GET'], endpoint='getTOP10')
@__token_wrapper
def getTOP10(context):
    try:
        # yesterday = []
        # data={}
        business_id = request.args.get('business_id')
        if business_id is None:
            ret = Scope['bi_saas'].execute('''SELECT MAX(b.price_sale) as price_sale,b.shop_name FROM(
    SELECT SUM(a.price_sale) as price_sale,a.shop_name FROM (
    SELECT totalday,totalhour,MAX(price_sale) as price_sale,t.shop_name FROM bi_shop_saleinfo_hour_today t
		LEFT JOIN bi_shops_new sh on t.dc_shop_id=sh.shop_id
		LEFT JOIN bi_business_info info on sh.business_clazz_id=info.business_id
    WHERE totalday=date(now()) AND t.shop_name is NOT NULL AND src_business_id<>'天猫爬虫' and info.isshow=1
    GROUP BY totalday,totalhour,t.shop_name
    )a
    GROUP BY a.shop_name
    UNION ALL
    SELECT MAX(price_sale),shop_name FROM bi_shop_saleinfo_hour_today WHERE totalday=date(now())
    and src_business_id='天猫爬虫'
    GROUP BY shop_name
    )b GROUP BY b.shop_name
    ORDER BY price_sale DESC
    LIMIT 10''')
        else:
            ret = Scope['bi_saas'].execute(f'''SELECT MAX(b.price_sale) as price_sale,b.shop_name FROM(
            SELECT SUM(a.price_sale) as price_sale,a.shop_name FROM (
            SELECT totalday,totalhour,MAX(price_sale) as price_sale,t.shop_name FROM bi_shop_saleinfo_hour_today t
            LEFT JOIN bi_shops_new s on t.dc_shop_id=s.shop_id
            WHERE totalday=date(now()) AND s.business_clazz_id={business_id} 
            GROUP BY totalday,totalhour,t.shop_name
            )a
            GROUP BY a.shop_name
            )b GROUP BY b.shop_name
            ORDER BY price_sale DESC
            LIMIT 10''')

        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_0(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_0(val[i])
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


@datawall.route('/getTMAmount', methods=['GET'], endpoint='getTMAmount')
@__token_wrapper
def getTMAmount(context):
    try:
        # yesterday = []
        # data={}
        ret_jd = Scope['vertica'].execute(
            """SELECT COUNT(*) FROM hmcdata.e3_order_info WHERE order_status<>3 AND DATE(to_timestamp(shipping_time_fh))=date(now()) """)
        for value in ret_jd:
            pack = int(value[0]) if value[0] is not None else 0

        #         ret = Scope['bi_saas'].execute('''SELECT SUM(a.price_sale) as price_sale from(
        # SELECT MAX(b.price_sale) as price_sale,b.shop_name FROM(
        # SELECT SUM(a.price_sale) as price_sale,a.shop_name FROM (
        # SELECT totalday,totalhour,MAX(price_sale) as price_sale,t.shop_name FROM bi_shop_saleinfo_hour_today t
        # WHERE totalday=DATE(NOW()) AND t.shop_name is NOT NULL AND src_business_id<>'天猫爬虫'
        # AND upper(t.shop_name) NOT like '%lazada%'
        # GROUP BY totalday,totalhour,t.shop_name
        # )a
        # GROUP BY a.shop_name
        # UNION ALL
        # SELECT MAX(price_sale),shop_name FROM bi_shop_saleinfo_hour_today WHERE totalday=DATE(NOW())
        # and src_business_id='天猫爬虫'
        # GROUP BY shop_name
        # )b GROUP BY b.shop_name
        # )a''')
        ret = Scope['bi_saas'].execute('''		SELECT SUM(a.price_sale) as price_sale from(			
            SELECT MAX(b.price_sale) as price_sale,b.shop_name FROM(
            SELECT SUM(a.price_sale) as price_sale,a.shop_name FROM (
            SELECT totalday,totalhour,MAX(price_sale) as price_sale,t.shop_name FROM bi_shop_saleinfo_hour_today t
						LEFT JOIN bi_shops_new sh on t.dc_shop_id=sh.shop_id
						LEFT JOIN bi_business_info info on sh.business_clazz_id=info.business_id
            WHERE totalday=date(now()) AND t.shop_name is NOT NULL AND src_business_id<>'天猫爬虫' and info.isshow=1 AND online_offline=1
            GROUP BY totalday,totalhour,t.shop_name
            )a
            GROUP BY a.shop_name
            UNION ALL
            SELECT MAX(price_sale),shop_name FROM bi_shop_saleinfo_hour_today WHERE totalday=date(now())
            and src_business_id='天猫爬虫'
            GROUP BY shop_name
            )b GROUP BY b.shop_name
            )a''')
        for value in ret:
            price_sale = int(value[0]) if value[0] is not None else 0

            # columns = ret.keys()
            # data = []
            # for val in ret:
            #     data_dict = {}
            #     for i, column in enumerate(columns):
            #         if isinstance(val[i], decimal.Decimal):
            #             data_dict[column] = format_0(val[i])
            #         elif isinstance(val[i], datetime.datetime):
            #             data_dict[column] = datetime_format(val[i])
            #         elif isinstance(val[i], float):
            #             data_dict[column] = format_0(val[i])
            #         elif isinstance(val[i], datetime.date):
            #             data_dict[column] = datetime_format(val[i])
            #         elif val[i] is None:
            #             data_dict[column] = 0
            #         else:
            #             data_dict[column] = val[i]
            #     data.append(data_dict)
            data = {"price_sale": price_sale, "pack": pack}
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
        Scope['vertica'].remove()


@datawall.route('/getGroupSales', methods=['GET'], endpoint='getGroupSales')
@__token_wrapper
def getGroupSales(context):
    try:
        business_id = request.args.get('business_id')
        # yesterday = []
        # data={}
        last_year_day = (datetime.datetime.now() - dateutil.relativedelta.relativedelta(years=1)).strftime("%Y-%m-%d")
        if business_id is None:
            ret = Scope['bi_saas'].execute(
                f'''SELECT 'now' as total_day,SUM(b.price_sale) as price_sale,SUM(b.sale_cost) as sale_cost,SUM(order_products) as order_products FROM(
                             SELECT SUM(a.price_sale) as price_sale,SUM(sale_cost) as sale_cost,SUM(order_products) as order_products,a.shop_name FROM (
                              SELECT totalday,totalhour,MAX(price_sale) as price_sale,MAX(sale_cost) as sale_cost,MAX(order_products) as order_products,t.shop_name FROM bi_shop_saleinfo_hour_today t
                                LEFT JOIN bi_shops_new s ON t.dc_shop_id=s.shop_id
                                LEFT JOIN bi_business_info info on s.business_clazz_id=info.business_id
                WHERE totalday=date(now()) AND info.isshow=1
                GROUP BY totalday,totalhour,t.shop_name
                            )a  GROUP BY a.shop_name
                            )b
                            UNION ALL
                            SELECT  'last_year' as total_day,SUM(price_sale) as price_sale,SUM(sale_cost) as sale_cost,SUM(order_products) as order_products FROM bi_shop_saleinfo_hour_nottoday t
                            LEFT JOIN bi_shops_new s ON t.dc_shop_id=s.shop_id
                                LEFT JOIN bi_business_info info on s.business_clazz_id=info.business_id
                            WHERE totalday='{last_year_day}' AND info.isshow=1''')
        else:
            ret = Scope['bi_saas'].execute(
                f'''SELECT 'now' as total_day,SUM(b.price_sale) as price_sale,SUM(b.sale_cost) as sale_cost,SUM(order_products) as order_products FROM(
                         SELECT SUM(a.price_sale) as price_sale,SUM(sale_cost) as sale_cost,SUM(order_products) as order_products,a.shop_name FROM (
                          SELECT totalday,totalhour,MAX(price_sale) as price_sale,MAX(sale_cost) as sale_cost,MAX(order_products) as order_products,t.shop_name FROM bi_shop_saleinfo_hour_today t
                            LEFT JOIN bi_shops_new s ON t.dc_shop_id=s.shop_id
            WHERE totalday=date(now()) AND s.business_clazz_id={business_id}
            GROUP BY totalday,totalhour,t.shop_name
                        )a  GROUP BY a.shop_name
                        )b
                        UNION ALL
                        SELECT  'last_year' as total_day,SUM(price_sale) as price_sale,SUM(sale_cost) as sale_cost,SUM(order_products) as order_products FROM bi_shop_saleinfo_hour_nottoday t
                        LEFT JOIN bi_shops_new s ON t.dc_shop_id=s.shop_id
                        WHERE totalday='{last_year_day}' AND s.business_clazz_id={business_id} ''')

        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_0(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_0(val[i])
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


@datawall.route('/getClazzSales', methods=['GET'], endpoint='getClazzSales')
@__token_wrapper
def getClazzSales(context):
    try:
        # yesterday = []
        # data={}
        ret = Scope['bi_saas'].execute('''SELECT b.price_sale,info.business_name from(	
SELECT sum(a.price_sale) as price_sale,sh.business_clazz_id
	from(
 SELECT totalday,totalhour,MAX(price_sale) as price_sale,MAX(order_count) as order_count,MAX(sale_cost) as sale_cost,shop_name,dc_shop_id FROM bi_shop_saleinfo_hour_today
WHERE totalday=DATE(NOW())   
GROUP BY totalday,totalhour,shop_name,dc_shop_id
)a
LEFT JOIN bi_shops_new sh on a.dc_shop_id=sh.shop_id
GROUP BY a.totalday,sh.business_clazz_id
)b
LEFT JOIN bi_business_info info on b.business_clazz_id=info.business_id
WHERE info.isshow=1
ORDER BY b.price_sale DESC limit 10 ''')

        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_0(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_0(val[i])
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


@datawall.route('/getTMYushou', methods=['GET'], endpoint='getTMYushou')
@__token_wrapper
def getTMYushou(context):
    try:
        # yesterday = []
        # data={}
        ret = Scope['vertica'].execute('''SELECT sh.shop_name,SUM(payment) as payment,SUM(step_paid_fee) as step_paid_fee FROM hmcdata.e3_taobao_trade t
        LEFT JOIN hmcdata.dc_shop sh ON t.shop_id=sh.outer_shop_id AND sh.src_business_id=t.src_business_id AND sh.platform_id=1
        WHERE step_trade_status IN ('FRONT_PAID_FINAL_NOPAID','FRONT_PAID_FINAL_PAID')
        AND DATE(created)>='2019-09-09' AND pay_time<'2019-11-12' AND  status<>'TRADE_CLOSED'
        GROUP BY sh.shop_name
        ORDER BY payment DESC''')

        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_0(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_0(val[i])
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


@datawall.route('/getChannel', methods=['GET'], endpoint='getChannel')
@__token_wrapper
def getChannel(context):
    try:
        ret = Scope['bi_saas'].execute('''SELECT SUM(c.value)	as value,c.name FROM(	
SELECT MAX(b.price_sale) as value,name,b.shop_name FROM(	
SELECT SUM(a.price_sale) as price_sale,a.shop_name,CASE IFNULL(a.channel,1)
	WHEN 1 THEN
		'淘系'
	WHEN 101 THEN
		'淘系'
	WHEN 5 THEN
		'京东'
	WHEN 39 THEN
		'唯品'
	WHEN 77 THEN 
		'唯品'
	ELSE
		'其他'
END as name FROM (
    SELECT totalday,totalhour,MAX(price_sale) as price_sale,t.shop_name,s.channel FROM bi_shop_saleinfo_hour_today t
    LEFT JOIN bi_shops_new s on s.shop_id=t.dc_shop_id
		LEFT JOIN bi_business_info info on s.business_clazz_id=info.business_id
    WHERE t.totalday=DATE(NOW()) AND s.business_clazz_id not in(50,51,52)  -- AND s.online=1 
		and info.isshow=1
    GROUP BY totalday,totalhour,t.shop_name,s.channel
    )a
    GROUP BY name,a.shop_name
		UNION ALL
		SELECT IFNULL(SUM(a.price_sale),0) as price_sale,a.shop_name,'淘系' as name FROM (
		SELECT totalday,totalhour,MAX(price_sale) as price_sale,t.shop_name FROM bi_shop_saleinfo_hour_today t
		WHERE totalday=DATE(NOW()) AND t.shop_name is NOT NULL AND totalhour is NULL AND src_business_id='天猫爬虫'
		GROUP BY totalday,totalhour,t.shop_name
		)a GROUP BY a.shop_name
		)b GROUP BY b.name,b.shop_name
		)c GROUP BY c.name ORDER BY c.name DESC
		''')

        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_0(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_0(val[i])
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


@datawall.route('/getGroupChannel', methods=['GET'], endpoint='getGroupChannel')
@__token_wrapper
def getGroupChannel(context):
    try:
        business_id = request.args.get('business_id')
        if business_id is None:
            businessSql = ""
        else:
            businessSql = f"""AND s.business_clazz_id={business_id}"""
        ret = Scope['bi_saas'].execute(f'''	SELECT SUM(c.value)	as value,c.name FROM(	
SELECT MAX(b.price_sale) as value,name,b.shop_name FROM(	
SELECT SUM(a.price_sale) as price_sale,a.shop_name,CASE IFNULL(a.channel,1)
	WHEN 1 THEN
		'淘系'
	WHEN 101 THEN
		'淘系'
	WHEN 5 THEN
		'京东'
	WHEN 39 THEN
		'唯品'
	WHEN 77 THEN 
		'唯品'
		WHEN 13020 THEN
		'线下'
		WHEN 13019 THEN
		'线下'
	ELSE
		'其他'
END as name FROM (
    SELECT totalday,totalhour,MAX(price_sale) as price_sale,t.shop_name,s.channel FROM bi_shop_saleinfo_hour_today t
    LEFT JOIN bi_shops_new s on s.shop_id=t.dc_shop_id
		LEFT JOIN bi_business_info info on s.business_clazz_id=info.business_id
    WHERE t.totalday=DATE(NOW())  AND info.isshow=1 {businessSql}
    GROUP BY totalday,totalhour,t.shop_name,s.channel
    )a
    GROUP BY name,a.shop_name
		)b GROUP BY b.name,b.shop_name
		)c GROUP BY c.name ORDER BY c.name DESC
		''')

        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_0(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_0(val[i])
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


@datawall.route('/getDocSeasonSale', methods=['GET'], endpoint='getDocSeasonSale')
@__token_wrapper
def getDocSeasonSale(context):
    try:
        business_id = request.args.get('business_id')
        # if business_id is None:
        #     businessSql = ""
        # else:
        #     businessSql = f"""AND s.business_clazz_id={business_id}"""
        ret = Scope['bi_saas'].execute(f'''	SELECT SUM(price_sale) as sale_amount, SUM(price_original) as sale_amount_ori, ifnull(ifnull(c.doc_season,l.productSeason),'未知') as season_name
            , SUM(order_products) AS sale_num, SUM(sale_cost) as sale_cost
            ,( SUM(price_sale) - SUM(sale_cost) ) / SUM(price_sale) * 100 as profit_rate
             , SUM(price_sale) / SUM(price_original) * 100 AS discount_rate, s.business_name ,case ifnull(ifnull(c.doc_season,l.productSeason),'未知') when '春季' then 68 when '夏季' then 69 when '秋季' then 70 when '冬季' then 71 when '全季' then 72 when '未知' then 73 end as sorter
            FROM `bi_shop_saleinfo_goods` n 
                            left join product_season_new c on n.product_sn=c.product_sn
                            left join iom_scm_product_list l on n.product_sn=l.ProductSN
            LEFT JOIN (SELECT b.business_name, shop_id, b.business_id FROM bi_shops_new s LEFT JOIN bi_business_info b ON s.business_clazz_id=b.business_id  ) s
            ON s.shop_id=n.dc_shop_id
            WHERE business_id = {business_id}   AND totalday = CURRENT_DATE 
            GROUP BY  season_name, s.business_name
            HAVING SUM(order_products) > 0 -- and season_name is not null
            ORDER BY sorter ASC
		''')

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


@datawall.route('/getGoodTypeSale', methods=['GET'], endpoint='getGoodTypeSale')
@__token_wrapper
def getGoodTypeSale(context):
    try:
        business_id = request.args.get('business_id')
        # if business_id is None:
        #     businessSql = ""
        # else:
        #     businessSql = f"""AND s.business_clazz_id={business_id}"""
        ret = Scope['bi_saas'].execute(f''' SELECT SUM(price_sale) as sale_amount, SUM(price_original) as sale_amount_ori, SUM(order_products) AS sale_num, SUM(sale_cost) as sale_cost
                ,( SUM(price_sale) - SUM(sale_cost) ) / SUM(price_sale) * 100 as profit_rate
                 , SUM(price_sale) / SUM(price_original) * 100 AS discount_rate, s.business_name ,if(ifnull(c.doc_year,l.productYear)>=year(CURRENT_DATE),'新货','旧货') as goods_type
								 
                FROM `bi_shop_saleinfo_goods` n 
								left join product_season_new c on n.product_sn=c.product_sn
								left join iom_scm_product_list l on n.product_sn=l.ProductSN
                LEFT JOIN (SELECT b.business_name, shop_id, b.business_id FROM bi_shops_new s LEFT JOIN bi_business_info b ON s.business_clazz_id=b.business_id  ) s
                ON s.shop_id=n.dc_shop_id
                WHERE business_id = {business_id}   AND totalday = CURRENT_DATE 
                GROUP BY s.business_name,goods_type
                HAVING SUM(order_products) > 0 
		''')

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


@datawall.route('/getAllPaymentTrendlast', methods=['GET'], endpoint='getAllPaymentTrendlast')
@__token_wrapper
def getAllPaymentTrendlast(context):
    try:
        yesterday = []
        last_30 = []
        data = {}
        # ret_yes = Scope['bi_saas'].execute('''SELECT SUM(pay_trade_amt),hour_pay FROM st_op_shop_hour_day WHERE st_date=DATE_SUB(curdate(),INTERVAL 1 DAY) GROUP BY hour_pay
        # ORDER BY hour_pay asc''')

        #
        # ret_30=Scope['bi_saas'].execute('''SELECT AVG(pay_trade_amt),hour_pay FROM st_op_shop_hour_day WHERE st_date BETWEEN DATE_SUB(curdate(),INTERVAL 30 DAY) and
        # DATE_SUB(curdate(),INTERVAL 1 DAY)
        # GROUP BY hour_pay
        # ORDER BY hour_pay asc''')

        ret = Scope['bi_saas'].execute('''SELECT a.total_hour AS hour,ifnull(a.thirty,0) as pay_trade_amt_30,ifnull(b.yesterday,0) pay_trade_amt_last from(
		SELECT total_hour,SUM(price_sale)/30 as thirty FROM bi_saleinfo_hour_30days b
		WHERE dc_shop_id not in(SELECT shop_id from bi_shops_new WHERE business_clazz_id=60)
		GROUP BY total_hour )a
		LEFT JOIN 
		(
		SELECT total_hour,SUM(price_sale) as yesterday FROM bi_saleinfo_hour_30days  b
		LEFT JOIN bi_shops_new s on b.dc_shop_id=s.shop_id
		WHERE totalday=DATE_SUB(curdate(),INTERVAL 1 DAY) AND s.business_clazz_id not in(60)
		GROUP BY total_hour )b
		on a.total_hour=b.total_hour''')

        for value in ret:
            d = {}
            d["pay_trade_amt"] = float(value[2]) if value[2] is not None else 0
            d["hour"] = int(value[0]) if value[0] is not None else 0
            y = {}
            y["pay_trade_amt"] = float(value[1]) if value[1] is not None else 0
            y["hour"] = int(value[0]) if value[0] is not None else 0
            yesterday.append(d)
            last_30.append(y)

        # for value in ret_yes:
        #     d = {}
        #     d["pay_trade_amt"] = float(value[0]) if value[0] is not None else 0
        #     d["hour"] = int(value[1]) if value[1] is not None else 0
        #     yesterday.append(d)
        #
        # for value in ret_30:
        #     d = {}
        #     d["pay_trade_amt"] = float(value[0]) if value[0] is not None else 0
        #     d["hour"] = int(value[1]) if value[1] is not None else 0
        #     last_30.append(d)
        data["yesterday"] = yesterday
        data["last_30"] = last_30
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
        # SessionO2O.remove()


@datawall.route('/getProvinceData', methods=['GET'], endpoint='getProvinceData')
@__token_wrapper
def getProvinceData(context):
    try:
        ret = Scope['bi_saas'].execute(
            ''' SELECT SUM(pay_trade_amt),REPLACE(province,'省','') as p FROM st_op_shop_online_region GROUP BY p ''')
        data = []

        for value in ret:
            record = {}
            record['value'] = int(value[0]) if value[0] is not None else 0
            record['name'] = value[1]
            data.append(record)
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
        # SessionO2O.remove()


@datawall.route('/getGroupSaleTrend', methods=['GET'], endpoint='getGroupSaleTrend')
@__token_wrapper
def getGroupSaleTrend(context):
    try:
        business_id = request.args.get('business_id')
        shop_id = request.args.get('shop_id')
        if business_id is None:
            businessSql = ""
        else:
            businessSql = f"""AND s.business_clazz_id={business_id}"""

        if shop_id is None:
            ret = Scope['bi_saas'].execute(f'''
                    SELECT a.total_hour AS hour,ifnull(a.thirty,0) as pay_trade_amt_30,ifnull(b.yesterday,0) pay_trade_amt_last from(
                   SELECT total_hour,SUM(price_sale)/30 as thirty FROM bi_saleinfo_hour_30days 
                   WHERE dc_shop_id in(
                                         SELECT shop_id FROM bi_shops_new s LEFT JOIN bi_business_info info on s.business_clazz_id=info.business_id
                                         WHERE info.isshow=1 {businessSql}
                                         )
                   GROUP BY total_hour  )a
                   LEFT JOIN 
                   (
                   SELECT total_hour,SUM(price_sale) as yesterday FROM bi_saleinfo_hour_30days 
                    WHERE dc_shop_id in(
                                         SELECT shop_id FROM bi_shops_new s LEFT JOIN bi_business_info info on s.business_clazz_id=info.business_id
                                         WHERE info.isshow=1 {businessSql}
                                         ) and totalday=DATE_SUB(curdate(),INTERVAL 1 DAY)
                   GROUP BY total_hour )b
                   on a.total_hour=b.total_hour
            ''')
        else:
            ret = Scope['bi_saas'].execute(f'''
                                SELECT a.total_hour AS hour,ifnull(a.thirty,0) as pay_trade_amt_30,ifnull(b.yesterday,0) pay_trade_amt_last from(
                               SELECT total_hour,SUM(price_sale)/30 as thirty FROM bi_saleinfo_hour_30days 
                               WHERE dc_shop_id={shop_id}
                               GROUP BY total_hour  )a
                               LEFT JOIN 
                               (
                               SELECT total_hour,SUM(price_sale) as yesterday FROM bi_saleinfo_hour_30days 
                                WHERE dc_shop_id={shop_id} and totalday=DATE_SUB(curdate(),INTERVAL 1 DAY)
                               GROUP BY total_hour )b
                               on a.total_hour=b.total_hour
                        ''')

        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_0(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_0(val[i])
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


@datawall.route('/getShopMonthSale', methods=['GET'], endpoint='getShopMonthSale')
@__token_wrapper
def getShopMonthSale(context):
    try:
        shop_id = request.args.get('shop_id')
        ret = Scope['bi_saas'].execute(
            f"""SELECT price_sale,totalday,sale_cost FROM bi_shop_saleinfo_hour_nottoday WHERE dc_shop_id={shop_id} AND totalday>=DATE_SUB(CURRENT_DATE,INTERVAL 30 DAY)
ORDER BY totalday ASC""")

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


@datawall.route('/getBusinessMonthSale', methods=['GET'], endpoint='getBusinessMonthSale')
@__token_wrapper
def getBusinessMonthSale(context):
    try:
        business_id = request.args.get('business_id')
        ret = Scope['bi_saas'].execute(
            f"""SELECT sum(price_sale) as price_sale,totalday,sum(sale_cost) as sale_cost FROM bi_shop_saleinfo_hour_nottoday n 
LEFT JOIN bi_shops_new s on n.dc_shop_id=s.shop_id
WHERE s.business_clazz_id={business_id} AND totalday>=DATE_SUB(CURRENT_DATE,INTERVAL 30 DAY)
GROUP BY totalday
ORDER BY totalday ASC""")

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


@datawall.route('/getShopSales', methods=['GET'], endpoint='getShopSales')
@__token_wrapper
def getShopSales(context):
    try:
        shop_id = request.args.get('shop_id')
        # yesterday = []
        # data={}
        last_year_day = (datetime.datetime.now() - dateutil.relativedelta.relativedelta(years=1)).strftime("%Y-%m-%d")
        ret = Scope['bi_saas'].execute(
            f'''SELECT 'now' as total_day,SUM(b.price_sale) as price_sale,SUM(b.sale_cost) as sale_cost,SUM(order_products) as order_products FROM(
                         SELECT SUM(a.price_sale) as price_sale,SUM(sale_cost) as sale_cost,SUM(order_products) as order_products,a.shop_name FROM (
                          SELECT totalday,totalhour,MAX(price_sale) as price_sale,MAX(sale_cost) as sale_cost,MAX(order_products) as order_products,t.shop_name FROM bi_shop_saleinfo_hour_today t
            WHERE totalday=date(now()) AND t.dc_shop_id={shop_id}
            GROUP BY totalday,totalhour,t.shop_name
                        )a  GROUP BY a.shop_name
                        )b
                        UNION ALL
                        SELECT  'last_year' as total_day,SUM(price_sale) as price_sale,SUM(sale_cost) as sale_cost,SUM(order_products) as order_products FROM bi_shop_saleinfo_hour_nottoday t
												WHERE t.dc_shop_id={shop_id}
                        AND totalday='{last_year_day}' ''')

        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_0(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_0(val[i])
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


@datawall.route('/getShopMonthSalesTarget', methods=['GET'], endpoint='getShopMonthSalesTarget')
@__token_wrapper
def getShopMonthSalesTarget(context):
    try:
        shop_id = request.args.get('shop_id')
        today = datetime.datetime.now()
        ret = Scope['bi_saas'].execute(f'''SELECT a.price_sale,b.targetNum FROM(
SELECT total_year,total_month,SUM(price_sale)  as price_sale FROM bi_shop_saleinfo_hour_nottoday WHERE dc_shop_id={shop_id} AND total_month={today.month} AND total_year={today.year}
)a LEFT JOIN(
SELECT total_year,total_month,targetNum FROM bi_target_month WHERE dc_shop_id={shop_id} AND total_year={today.year} AND total_month={today.month}
)b ON a.total_year=b.total_year AND a.total_month=b.total_month ''')

        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_0(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_0(val[i])
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


@datawall.route('/getShopGoodsTop15', methods=['GET'], endpoint='getShopGoodsTop15')
@__token_wrapper
def getShopGoodsTop15(context):
    try:
        shop_id = request.args.get('shop_id')
        if shop_id is None:
            context['data'] = []
            return jsonify(context)
        ret = Scope['bi_saas'].execute(f'''SELECT SUM(g.price_sale) as price_sale,g.product_sn,max(i.img_src) as  img_src 
        FROM bi_shop_saleinfo_goods g LEFT JOIN bi_product_img i ON g.product_sn=i.product_sn
        WHERE g.dc_shop_id={shop_id} AND g.totalday=DATE(now())
        GROUP BY product_sn
        order by price_sale DESC LIMIT 15 ''')

        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_0(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_0(val[i])
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


@datawall.route('/getGrossProfit', methods=['GET'], endpoint='getGrossProfit')
@__token_wrapper
def getGrossProfit(context):
    try:
        business_unit_id = context['data']['businessUnitId']
        if business_unit_id is None:
            context['data'] = []
            return jsonify(context)
        ret = Scope['bi_saas'].execute(
            f'''SELECT 'now' as total_day,SUM(b.price_sale) as price_sale,SUM(b.sale_cost) as sale_cost FROM(
 SELECT SUM(a.price_sale) as price_sale,SUM(sale_cost) as sale_cost,SUM(order_products) as order_products,a.shop_name FROM (
	SELECT totalday,totalhour,MAX(price_sale) as price_sale,MAX(sale_cost) as sale_cost,MAX(order_products) as order_products,t.shop_name FROM bi_shop_saleinfo_hour_today t
		LEFT JOIN bi_shops_new s ON t.dc_shop_id=s.shop_id
WHERE totalday=date(now())  AND s.online=0 AND s.business_unit_id={business_unit_id}
GROUP BY totalday,totalhour,t.shop_name
)a  GROUP BY a.shop_name
)b
UNION ALL												
SELECT 'last_30' as total_day,sum(price_sale) as price_sale,sum(sale_cost) as sale_cost FROM bi_saleinfo_hour_30days where business_unit_id={business_unit_id} and online_offline=2 ''')

        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_0(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_0(val[i])
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


@datawall.route('/getTodayTarget', methods=['GET'], endpoint='getTodayTarget')
@__token_wrapper
def getTodayTarget(context):
    try:
        business_unit_id = context['data']['businessUnitId']
        if business_unit_id is None:
            context['data'] = []
            return jsonify(context)
        ret = Scope['bi_saas'].execute(f''' 
        SELECT sum(targetNum) as targetNum FROM bi_sdb_dist_report_target_day WHERE targetDay=DATE(now()) AND business_unit_id={business_unit_id}
        ''')
        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_0(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_0(val[i])
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


@datawall.route('/get_group_year_compare', methods=['GET'], endpoint='get_group_year_compare')
@__token_wrapper
def get_group_year_compare(context):
    try:
        sql = """SELECT a.price_sale as last_price_sale,b.price_sale as price_sale,a.total_month FROM(
            SELECT sum(price_sale) as price_sale,total_month,total_year FROM bi_business_sales_analyze b LEFT JOIN bi_business_info i on b.business_id=i.business_id
            where i.isshow=1 and total_year=year(now())-1
            GROUP BY total_month,total_year
            )a
            LEFT JOIN(
            SELECT sum(price_sale) as price_sale,total_month,total_year FROM bi_business_sales_analyze b LEFT JOIN bi_business_info i on b.business_id=i.business_id
            where i.isshow=1 and total_year=year(now())
            GROUP BY total_month,total_year
            )b ON a.total_month=b.total_month"""

        ret = Scope['bi_saas'].execute(sql)

        columns = ret.keys()
        data = []
        for val in ret:
            data_dict = {}
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data_dict[column] = format_0(val[i])
                elif isinstance(val[i], datetime.datetime):
                    data_dict[column] = datetime_format(val[i])
                elif isinstance(val[i], float):
                    data_dict[column] = format_0(val[i])
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
