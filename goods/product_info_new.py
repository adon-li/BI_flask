# -*- coding: utf-8 -*-
# @Author  : Joshua
# @File    : product_info_new

from flask import jsonify, request
from bi_flask.__token import __token_wrapper
import logging
import json
from bi_flask._sessions import sessions, sessions_scopes
from bi_flask.utils import *
from bi_flask.goods.api import good

logger = logging.getLogger('bi')
Scope = sessions_scopes(sessions)


@good.route('/get_product_info', methods=['GET'], endpoint='get_product_info')
@__token_wrapper
def get_product_info(context):
    try:
        product_sn = request.args.get('product_sn', type=str, default=None)
        sql = f"""
                SELECT i.category,i.is_odm,i.designer_name,i.product_style ,i.golden_triangle,i.product_title,l.props_list,i.img_src,i.sales_price,
                ifnull(doc_year,product_year) as product_new_year,ifnull(doc_season,product_season) as product_new_season    FROM hmcdata.bi_new_product_analyse_info i 
                left join hmcdata.bi_new_product_analyse_info_props_list l on i.product_sn=l.product_sn 
                where i.product_sn =:product_sn
        """

        ret = Scope['vertica'].execute(sql, {'product_sn': product_sn})
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
                    data_dict[column] = ''
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)
        if len(data) > 0:
            context['data'] = data[0]
        else:
            context['data'] = {'category': '', 'is_odm': 0, 'designer_name': '', 'product_style': '',
                               'golden_triangle': '', 'product_title': '', 'props_list': '', 'img_src': '',
                               'sales_price': 0}

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


@good.route('/get_product_sale_trend', methods=['GET'], endpoint='get_product_sale_trend')
@__token_wrapper
def get_product_sale_trend(context):
    try:
        product_sn = request.args.get('product_sn', type=str, default=None)
        start_date = request.args.get('start_date', type=str, default=None)
        end_date = request.args.get('end_date', type=str, default=None)
        is_online = request.args.get('is_online', type=int, default=1)
        day_type = request.args.get('day_type', type=str, default=None)
        if day_type == 'day':
            today_sql = 'total_day'
        elif day_type == 'week':
            today_sql = """concat(year(total_day),concat('-',week_iso(total_day)))"""
        elif day_type == 'month':
            today_sql = """to_char(total_day,'YYYYMM')"""
        else:
            today_sql = 'total_day'

        if int(is_online) == 1:
            sql = f"""
                select color_name ,sum(goods_number) as goods_number,{today_sql}  as total_day
                    FROM hmcdata.product_reorder_recommendation_day_sales s 
                    where total_day >=:start_date and total_day<=:end_date and product_sn =:product_sn 
                    GROUP BY product_sn,{today_sql},color_name
                    ORDER BY total_day ASC
                        """
            sql_total = f"""
                select a.product_sn,a.goods_number,a.payment,a.cost,b.return_nums,a.origin_amount from(
                select s.product_sn ,sum(goods_number) as goods_number,sum(payment) as payment,SUM(c.costamount*s.goods_number) as cost,sum(goods_number*l.SalesPrice) as origin_amount 
                FROM hmcdata.product_reorder_recommendation_day_sales s 
                left join hmcdata.bi_goods_cost c on s.product_sn =c.goods_bn 
                left join hmcdata.iom_scm_product_list l on s.product_sn=l.product_sn
                where total_day >=:start_date and total_day<=:end_date and s.product_sn =:product_sn
                GROUP BY s.product_sn
                )a
                LEFT JOIN (
                SELECT product_sn ,sum(return_nums) as return_nums FROM hmcdata.product_reorder_recommendation_day_return prrdr 
                where total_day >=:start_date and total_day<=:end_date and product_sn =:product_sn
                GROUP by product_sn
                )b on a.product_sn=b.product_sn;
            """
        else:
            sql = f"""
                           select color_name ,sum(goods_number) as goods_number,sum(payment) as payment,SUM(c.costamount*s.goods_number) as cost ,total_day
                               FROM hmcdata.product_reorder_recommendation_offline_day_sales s 
                               left join hmcdata.bi_goods_cost c on s.product_sn =c.goods_bn 
                               where total_day >=:start_date and total_day<=:end_date and product_sn =:product_sn 
                               GROUP BY product_sn,total_day,color_name
                               ORDER BY total_day ASC
                                   """
            sql_total = f"""
                            select a.product_sn,a.goods_number,a.payment,a.cost,b.return_nums,a.origin_amount from(
                            select s.product_sn ,sum(goods_number) as goods_number,sum(payment) as payment,SUM(c.costamount*s.goods_number) as cost ,sum(goods_number*l.SalesPrice) as origin_amount
                            FROM hmcdata.product_reorder_recommendation_offline_day_sales s 
                            left join hmcdata.bi_goods_cost c on s.product_sn =c.goods_bn 
                            left join hmcdata.iom_scm_product_list l on s.product_sn=l.product_sn
                            where total_day >=:start_date and total_day<=:end_date and s.product_sn =:product_sn
                            GROUP BY s.product_sn
                            )a
                            LEFT JOIN (
                            SELECT product_sn ,sum(return_nums) as return_nums FROM hmcdata.product_reorder_recommendation_offline_day_return prrdr 
                            where total_day >=:start_date and total_day<=:end_date and product_sn =:product_sn
                            GROUP by product_sn
                            )b on a.product_sn=b.product_sn;"""
        ret = Scope['vertica'].execute(sql, {'product_sn': product_sn, 'start_date': start_date, 'end_date': end_date})
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
                # elif val[i] is None:
                #     data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)
        ret_total = Scope['vertica'].execute(sql_total,
                                             {'product_sn': product_sn, 'start_date': start_date, 'end_date': end_date})
        total_obj = {'goods_number': 0, 'payment': 0, 'cost': 0, 'return_nums': 0, 'origin_amount': 0}
        for product_sn, goods_number, payment, cost, return_nums, origin_amount in ret_total.fetchall():
            total_obj['goods_number'] = goods_number
            total_obj['payment'] = format_num(payment, 2)
            total_obj['cost'] = format_num(cost, 2)
            total_obj['return_nums'] = return_nums
            total_obj['origin_amount'] = format_num(origin_amount, 2)

        context['data'] = data
        context['total'] = total_obj

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


@good.route('/get_product_shop_saleinfo', methods=['GET'], endpoint='get_product_shop_saleinfo')
@__token_wrapper
def get_product_shop_saleinfo(context):
    try:
        product_sn = request.args.get('product_sn', type=str, default=None)
        start_date = request.args.get('start_date', type=str, default=None)
        end_date = request.args.get('end_date', type=str, default=None)
        is_online = request.args.get('is_online', type=int, default=1)
        if is_online == 1:
            sql = f"""
                    select shop_name,s.product_sn,color_name,SUM(payment) as payment ,SUM(goods_number) as goods_number,SUM(goods_number*c.costamount) as cost,
                    SUM(goods_number*l.SalesPrice) as origin_price FROM hmcdata.product_reorder_recommendation_shop_day_sales s
                    left join hmcdata.bi_goods_cost c on s.product_sn =c.goods_bn 
                    left join hmcdata.iom_scm_product_list l on s.product_sn =l.product_sn 
                    where s.product_sn=:product_sn and s.total_day>=:start_date and s.total_day<=:end_date
                    GROUP BY shop_name,s.product_sn,color_name
                    ORDER BY goods_number desc
            """
            ret = Scope['vertica'].execute(sql,
                                           {'product_sn': product_sn, 'start_date': start_date, 'end_date': end_date})
        else:
            sql = f"""
                               SELECT color_name ,b_area,first_in_time ,first_sale_time,is_model ,in_model,stock,is_sale ,transfer_pool,is_sale ,is_model ,
                        sale_amount,return_amount,cost,out_sale,fixed_price ,
                    sys_model,in_transfer,out_transfer
                        FROM hmcdata.level1_area_product_color_stock where product_sn =:product_sn 
                        """

            ret = Scope['vertica'].execute(sql, {'product_sn': product_sn})
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
                    data_dict[column] = ''
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


@good.route('/get_product_reorder_sale_trend_all_color', methods=['GET'],
            endpoint='get_product_reorder_sale_trend_all_color')
@__token_wrapper
def get_product_reorder_sale_trend_all_color(context):
    '''
    获取快返销售趋势
    :param context:
    :return:
    '''
    product_sn = request.args.get('product_sn')
    doc_year = request.args.get('doc_year')
    doc_season = request.args.get('doc_season')

    try:
        sql = f"""SELECT total_year,color,total_week,SUM(sale_nums) as sale_nums,SUM(sale_amount) as sale_amount,SUM(sale_cost) as sale_cost,SUM(sale_origin) as sale_origin,sale_type
FROM product_reorder_week_sales WHERE product_sn='{product_sn}' AND doc_year={doc_year} AND doc_season='{doc_season}'
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
        sql = f"""SELECT product_level,
                        color,
                        product_tag,
                        sold_out_rate,
                        total_sales_nums,
                        suggest_nums,
                        turnover,
                        sales_forecast
                        FROM hmcdata.product_reorder_recommendation 
                        WHERE  product_sn=:product_sn and doc_year=:doc_year and doc_season=:doc_season"""
        ret = Scope['bi_saas'].execute(sql, {'product_sn': product_sn, 'doc_year': doc_year, 'doc_season': doc_season})
        color_list = {}
        for product_level, color, product_tag, sold_out_rate, total_sales_nums, suggest_nums, turnover, sales_forecast in ret.fetchall():
            color_list[color] = {'product_level': product_level, 'color': color, 'product_tag': product_tag,
                                 'sold_out_rate': format_4(sold_out_rate),
                                 'total_sales_nums': total_sales_nums, 'suggest_nums': suggest_nums,
                                 'turnover': format_4(turnover), 'sales_forecast': sales_forecast}

        context['data'] = data
        context['color_list'] = color_list
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


@good.route('/get_product_reorder_color_info', methods=['GET'], endpoint='get_product_reorder_color_info')
@__token_wrapper
def get_product_reorder_color_info(context):
    '''
    获取快返颜色信息
    :param context:
    :return:
    '''
    product_sn = request.args.get('product_sn')
    doc_year = request.args.get('doc_year')
    doc_season = request.args.get('doc_season')

    try:
        sql = f"""SELECT product_level,
                color,
                product_tag,
                sold_out_rate,
                total_sales_nums,
                suggest_nums
                FROM hmcdata.product_reorder_recommendation 
                WHERE  product_sn=:product_sn and doc_year=:doc_year and doc_season=:doc_season"""
        ret = Scope['bi_saas'].execute(sql, {'product_sn': product_sn, 'doc_year': doc_year, 'doc_season': doc_season})
        data = {}
        for product_level, color, product_tag, sold_out_rate, total_sales_nums, suggest_nums in ret.fetchall():
            data[color] = {'product_level': product_level, 'color': color, 'product_tag': product_tag,
                           'sold_out_rate': format_4(sold_out_rate),
                           'total_sales_nums': total_sales_nums, 'suggest_nums': suggest_nums}
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


@good.route('/get_product_stock_info_now', methods=['GET'], endpoint='get_product_stock_info_now')
@__token_wrapper
def get_product_stock_info_now(context):
    '''
    获取库存状况
    :param context:
    :return:
    '''
    product_sn = request.args.get('product_sn')

    try:
        sql = f"""SELECT a.sku_count,b.zong,c.return_nums,d.offline_nums,d.shop_count FROM (
                SELECT product_sn,COUNT(*) as sku_count FROM hmcdata.iom_scm_sku_scu s where product_sn =:product_sn GROUP BY product_sn
                ) a
                left join (
                SELECT ifnull(SUM(es.sl-es.sl2),0) as zong,es.goods_sn FROM hmcdata.e3_spkcb es left join hmcdata.e3_cangku c on es.ck_id =c.cangku_id AND es.src_business_id =c.src_business_id 
                WHERE es.goods_sn =:product_sn AND c.cwlb_id>=1 and c.cwlb_id not in(3,9)
                GROUP BY es.goods_sn
                ) b on a.product_sn =b.goods_sn
                left join (
                SELECT ifnull(SUM(es.sl-es.sl2),0) as return_nums,es.goods_sn FROM hmcdata.e3_spkcb es left join hmcdata.e3_cangku c on es.ck_id =c.cangku_id 
                AND es.src_business_id =c.src_business_id 
                WHERE es.goods_sn =:product_sn AND c.cwlb_id in(3,9)
                group by es.goods_sn
                ) c on a.product_sn =c.goods_sn
                left join (
                SELECT ifnull(SUM(quantity+in_transit_quantity),0) as offline_nums,a.product_sn ,COUNT(DISTINCT shop_id) as shop_count FROM  hmcdata.hmc_inventory_stock a
                LEFT JOIN hmcdata.hmc_inventory_warehouse b 
                ON a.warehouse_id=b.inventory_warehouse_id AND a.business_unit_id=b.business_unit_id
                WHERE b."type" = 4 AND a.product_sn =:product_sn and quantity+in_transit_quantity>0
                GROUP by a.product_sn 
                ) d on a.product_sn =d.product_sn;"""
        ret = Scope['vertica'].execute(sql, {'product_sn': product_sn})
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


@good.route('/get_product_stock_online_distribute', methods=['GET'], endpoint='get_product_stock_online_distribute')
@__token_wrapper
def get_product_stock_online_distribute(context):
    '''
    获取库存线上状况
    :param context:
    :return:
    '''
    product_sn = request.args.get('product_sn')
    is_online = request.args.get('is_online', type=int, default=1)
    try:
        if is_online == 1:
            sql = f"""SELECT c.ckmc ,s.size_name,s.color_name ,ifnull(SUM(es.sl-es.sl2),0) as stock_nums,s.size_id  FROM hmcdata.e3_spkcb es 
                    left join hmcdata.e3_cangku c on es.ck_id =c.cangku_id AND es.src_business_id =c.src_business_id 
                    left join hmcdata.iom_scm_sku_scu s on es.sku =s.sku_barcode 
                    WHERE es.goods_sn =:product_sn AND c.cwlb_id>=1 and es.sl-es.sl2>0
                    GROUP BY c.ckmc,s.size_name,s.color_name,s.size_id
                    order by s.size_id ;"""
        else:
            sql = f"""SELECT ifnull(SUM(quantity),0) as stock_nums,COUNT(DISTINCT b.shop_id) as shop_count,bz.zone_name as ckmc,iss.size_id,iss.size_name,iss.color_name,sum(in_transit_quantity) as onway_nums FROM  hmcdata.hmc_inventory_stock a
                    LEFT JOIN hmcdata.hmc_inventory_warehouse b ON a.warehouse_id=b.inventory_warehouse_id AND a.business_unit_id=b.business_unit_id
                    LEFT JOIN hmcdata.iom_scm_sku_scu iss on a.sku_code =iss.sku_barcode 
                    left join hmcdata.dc_shop ds on b.shop_id =ds.outer_shop_id and ds.is_online =0 and b.business_unit_id =ds.business_unit_id 
                    left join hmcdata.bi_province_zone z on ds.bi_province_id =z.id
                    left join hmcdata.bi_big_zone bz on z.zone_id =bz.id 
                    WHERE b."type" = 4 and a.product_sn =:product_sn and quantity+in_transit_quantity>0
                    GROUP BY bz.zone_name,iss.size_id,iss.size_name,iss.color_name 
                    ORDER BY iss.size_id ASC"""
        ret = Scope['vertica'].execute(sql, {'product_sn': product_sn})
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
