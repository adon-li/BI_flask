# -*- coding: utf-8 -*-
# @Time    : 2021/4/12 19:13
# @Author  : Joshua
# @File    : transfer_goods

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


@good.route('/get_transfer_goods', methods=['GET'], endpoint='get_transfer_goods')
@__token_wrapper
def get_transfer_goods(context):
    '''
        获取调拨商品明细
        :param context:
        :return:
        '''
    try:
        business_unit_id = context['data']['businessUnitId']
        category = request.args.get('category')
        product_sn = request.args.get('productSn')
        date_start = request.args.get('date_start', type=str)
        date_end = request.args.get('date_end', type=str)
        assign_type = request.args.get('assignType', type=int)
        to_shop_ids = request.args.get('toShopIds', type=str)
        shop_ids = request.args.get('shopIds', type=str)
        page = request.args.get('page', type=int)
        page_size = request.args.get('pageSize')
        sorter = request.args.get('sorter')
        order = request.args.get('order')
        order_sql = ""
        if date_start is None or date_end is None:
            return jsonify(build_context(context, '时间不能为空', False, -1))
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
        if product_sn is None or product_sn == '':
            product_sn_sql = """"""
        else:
            product_sn_sql = f"""and product_sn like '%{product_sn}%' """
        if category is None or category == '':
            category_sql = """"""
        else:
            category_sql = f"""and category ='{category}' """
        if to_shop_ids is None or to_shop_ids == '':
            to_shop_ids_sql = """"""
            other_to_shop_ids_sql = ""
        else:
            to_shop_ids_sql = f"""and to_dc_shop_id in ({to_shop_ids}) """
            other_to_shop_ids_sql = f"""and dc_shop_id in({to_shop_ids}) """

        if shop_ids is None or shop_ids == '':
            shop_ids_sql = """"""
            other_shop_ids_sql = ""
        else:
            shop_ids_sql = f"""and from_dc_shop_id in ({shop_ids}) """
            other_shop_ids_sql = f"""and dc_shop_id in({shop_ids}) """

        if assign_type is None or assign_type == '' or (isinstance(assign_type, int) and int(assign_type) >= 4):
            assign_sql = """"""
        else:
            assign_sql = f"""and assign_type ={assign_type} """
        if assign_type != 3:
            # 调拨
            sql = f"""
            select a.product_sn,a.category,a.color,a.apply_quantity,a.apply_to_send_quantity,a.onway_nums,a.received_quantity,b.into_shop_count,b.out_shop_count,a.product_name,
            ifnull(c.channel_shop_count,0) as channel_shop_count,ifnull(c.channel_quantity,0) as channel_quantity,d.sale_nums,i.img_src,a.product_year,a.product_season,a.fixed_price FROM (
            SELECT product_sn,category,color ,sum(apply_quantity) as apply_quantity,SUM(case status when '待发货' then apply_quantity else 0 end) as apply_to_send_quantity,
            sum(case status when '待收货' then delivery_quantity-received_quantity else 0 end) as onway_nums,
            sum(received_quantity) as received_quantity,max(product_name) as product_name,max(fixed_price) as fixed_price,product_year,product_season
            FROM hmcdata.bi_dist_model_transfer_base
            where  create_time >='{date_start} 00:00:00' and create_time<='{date_end} 23:59:59' {assign_sql} {shop_ids_sql} {product_sn_sql} {category_sql} {to_shop_ids_sql}
            GROUP BY product_sn,category,color ,product_year,product_season
            )a left join(
            SELECT product_sn,color,COUNT(DISTINCT from_dc_shop_id) as into_shop_count,
            COUNT(DISTINCT to_dc_shop_id) as out_shop_count FROM hmcdata.bi_dist_model_transfer_base 
            where status <>'已取消' AND create_time >='{date_start} 00:00:00' and create_time<='{date_end} 23:59:59' {assign_sql} {shop_ids_sql} {product_sn_sql} {category_sql} {to_shop_ids_sql}
            group by product_sn,color
            )b on a.product_sn=b.product_sn and a.color=b.color
            left join (
            SELECT product_sn ,color ,COUNT(DISTINCT dc_shop_id) as channel_shop_count,SUM(qty_send) as channel_quantity FROM hmcdata.bi_channel_order_info i
--             left join hmcdata.dc_shop ds on i.dc_shop_id =ds.id
--             left join hmcdata.bi_province_zone z on ds.bi_province_id=z.id
            where order_status<>3 and total_day>='{date_start} 00:00:00' and total_day<='{date_end} 23:59:59' {other_shop_ids_sql} {product_sn_sql} {category_sql} {other_to_shop_ids_sql}
            group by product_sn ,color
            )c on a.product_sn=c.product_sn and a.color=c.color
            left join(
            SELECT product_sn,color_name,SUM(goods_number) as sale_nums FROM hmcdata.product_reorder_recommendation_offline_shop_day_sales i
--             left join hmcdata.dc_shop ds on i.dc_shop_id =ds.id
--             left join hmcdata.bi_province_zone z on ds.bi_province_id=z.id
            where  total_day>='{date_start} 00:00:00' and total_day<='{date_end} 23:59:59' {other_shop_ids_sql} {product_sn_sql} {other_to_shop_ids_sql}
            group by product_sn,color_name
            )d on a.product_sn=d.product_sn and a.color=d.color_name
            left join hmcdata.bi_product_img i on a.product_sn =i.product_sn
            {sorter_sql} {order_sql} {offset};
            """
        else:
            # 渠道发货
            sql = f"""select i.img_src,a.product_sn,a.color,a.channel_shop_count,a.channel_quantity,a.category,b.name as product_name,b.base_price as fixed_price,b.int_year as product_year,
            b.season as product_season,c.apply_quantity,c.apply_to_send_quantity,c.onway_nums,c.received_quantity,d.into_shop_count,d.out_shop_count,e.sale_nums FROM (
            SELECT product_sn ,color ,COUNT(DISTINCT dc_shop_id) as channel_shop_count,SUM(qty_send) as channel_quantity,category FROM hmcdata.bi_channel_order_info i
--             left join hmcdata.dc_shop ds on i.dc_shop_id =ds.id
--             left join hmcdata.bi_province_zone z on ds.bi_province_id=z.id
            where order_status<>3 and total_day>='{date_start} 00:00:00' and total_day<='{date_end}  23:59:59' {other_shop_ids_sql} {product_sn_sql} {category_sql} {other_to_shop_ids_sql}
            group by product_sn ,color,category 
            )a 
            left join hmcdata.level4_product_color_info b on a.product_sn=b.product_sn and a.color=b.color_name 
            left join hmcdata.bi_product_img i on a.product_sn =i.product_sn
            left join(
            SELECT product_sn,color ,sum(apply_quantity) as apply_quantity,SUM(case status when '待发货' then apply_quantity else 0 end) as apply_to_send_quantity,
            sum(case status when '待收货' then delivery_quantity-received_quantity else 0 end) as onway_nums,
            sum(received_quantity) as received_quantity
            FROM hmcdata.bi_dist_model_transfer_base
            where  create_time >='{date_start} 00:00:00' and create_time<='{date_end} 23:59:59' {assign_sql} {shop_ids_sql} {product_sn_sql} {category_sql} {to_shop_ids_sql}
            GROUP BY product_sn,color
            )c on a.product_sn=c.product_sn and a.color=c.color
            left join(
            SELECT product_sn,color,COUNT(DISTINCT from_dc_shop_id) as into_shop_count,
            COUNT(DISTINCT to_dc_shop_id) as out_shop_count FROM hmcdata.bi_dist_model_transfer_base 
            where status <>'已取消' AND create_time >='{date_start} 00:00:00' and create_time<='{date_end} 23:59:59' {assign_sql} {shop_ids_sql} {product_sn_sql} {category_sql} {to_shop_ids_sql}
            group by product_sn,color
            )d on a.product_sn=d.product_sn and a.color=d.color
             left join(
            SELECT product_sn,color_name,SUM(goods_number) as sale_nums FROM hmcdata.product_reorder_recommendation_offline_shop_day_sales i
--             left join hmcdata.dc_shop ds on i.dc_shop_id =ds.id
--             left join hmcdata.bi_province_zone z on ds.bi_province_id=z.id
            where  total_day>='{date_start} 00:00:00' and total_day<='{date_end} 23:59:59' {other_shop_ids_sql} {product_sn_sql} {other_to_shop_ids_sql}
            group by product_sn,color_name
            )e on a.product_sn=e.product_sn and a.color=e.color_name
            {sorter_sql} {order_sql} {offset};
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
        if assign_type != 3:
            total_sql = f"""
                           select count(*) from(
                           SELECT product_sn,category,color ,product_year,product_season FROM hmcdata.bi_dist_model_transfer_base s 
                           where business_unit_id=:business_unit_id and  create_time >='{date_start} 00:00:00' and create_time<='{date_end} 23:59:59'
                            {assign_sql} {shop_ids_sql} {product_sn_sql} {category_sql} {to_shop_ids_sql}
                            group by product_sn,category,color ,product_year,product_season
                            )a"""
        else:
            total_sql = f"""
                          select count(*) from(
                          SELECT product_sn ,color,category FROM hmcdata.bi_channel_order_info i
--             left join hmcdata.dc_shop ds on i.dc_shop_id =ds.id
--             left join hmcdata.bi_province_zone z on ds.bi_province_id=z.id
            where total_day>='{date_start} 00:00:00' and total_day<='{date_end}  23:59:59' {other_shop_ids_sql} {product_sn_sql} {category_sql} {other_to_shop_ids_sql}
            group by product_sn ,color,category 
                           )a"""
        ret_total = Scope['vertica'].execute(total_sql, {'business_unit_id': business_unit_id})
        total = ret_total.fetchone()[0]
        context['total'] = total
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_transfer_goods_detail_info', methods=['GET'], endpoint='get_transfer_goods_detail_info')
@__token_wrapper
def get_transfer_goods_detail_info(context):
    '''
        获取商品信息
        :param context:
        :return:
        '''
    try:
        product_sn = request.args.get('product_sn')
        color = request.args.get('color')
        sql = f"""
                    SELECT img.img_src,i.product_sn,i.color_name as color,i.season as product_season ,i.int_year as product_year,i.name as product_name,i.category_2_name as category,
            i.base_price as fixed_price,s.product_tag ,d.DevProperty as dev_prop,sku.sku_list FROM hmcdata.level4_product_color_info i 
            left join hmcdata.bi_offline_product_skc_base s on i.product_sn =s.product_sn and i.color_name =s.color 
            left join hmcdata.iom_scm_inman_product_label d on i.product_sn =d.ProductSn 
            left join hmcdata.bi_product_img img on i.product_sn=img.product_sn
            left join (
                SELECT product_sn,color_name ,GROUP_CONCAT(sku_barcode) over (PARTITION by product_sn ,color_name)  as sku_list
                FROM hmcdata.iom_scm_sku_scu isss where product_sn =:product_sn and color_name =:color
            ) sku on i.product_sn=sku.product_sn and i.color_name=sku.color_name
            where i.product_sn =:product_sn and i.color_name =:color
                        """
        ret = Scope['vertica'].execute(sql, {'product_sn': product_sn, 'color': color})
        columns = ret.keys()
        data_dict = {}
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
        context['data'] = data_dict
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_transfer_goods_sku_info', methods=['GET'], endpoint='get_transfer_goods_sku_info')
@__token_wrapper
def get_transfer_goods_sku_info(context):
    '''
        获取商品sku信息
        :param context:
        :return:
        '''
    try:
        product_sn = request.args.get('product_sn')
        color = request.args.get('color')
        date_start = request.args.get('date_start', type=str)
        date_end = request.args.get('date_end', type=str)
        assign_type = request.args.get('assignType', type=int)
        province_ids = request.args.get('provinceIds', type=str)
        if province_ids is None or province_ids == '':
            province_ids_sql = """"""
            other_province_ids_sql = ""
        else:
            province_ids_sql = f"""and from_province_zone_id in ({province_ids}) """
            other_province_ids_sql = f"""and z.id in({province_ids}) """
        if assign_type is None or assign_type == '' or (isinstance(assign_type, int) and int(assign_type) >= 4):
            assign_sql = """"""
        else:
            assign_sql = f"""and assign_type ={assign_type} """
        if date_start is None or date_end is None:
            return jsonify(build_context(context, '时间不能为空', False, -1))
        sql = f"""
                   SELECT a.sku,a.size_name,b.sale_nums,c.apply_quantity,c.apply_to_send_quantity,c.onway_nums,c.received_quantity,d.channel_quantity FROM (
                    SELECT sku_barcode as sku,size_name FROM hmcdata.iom_scm_sku_scu isss where product_sn =:product_sn and color_name =:color 
                    )a left join(
                    SELECT sku,SUM(goods_number) as sale_nums FROM hmcdata.product_reorder_recommendation_offline_sku_day_sales i
                    left join hmcdata.dc_shop ds on i.dc_shop_id =ds.id
                    left join hmcdata.bi_province_zone z on ds.bi_province_id=z.id
                    where  total_day>='{date_start} 00:00:00' and total_day<='{date_end} 23:59:59' and product_sn=:product_sn  and color_name=:color {other_province_ids_sql}
                    group by sku
                    )b on a.sku=b.sku
                    left join (
                    SELECT sku ,sum(apply_quantity) as apply_quantity,SUM(case status when '待发货' then apply_quantity else 0 end) as apply_to_send_quantity,
                    sum(case status when '待收货' then delivery_quantity-received_quantity else 0 end) as onway_nums,
                    sum(received_quantity) as received_quantity
                    FROM hmcdata.bi_dist_model_transfer_base
                    where  create_time >='{date_start} 00:00:00' and create_time<='{date_end} 23:59:59' and product_sn=:product_sn  and color=:color {province_ids_sql} {assign_sql}
                    GROUP BY sku
                    )c on a.sku=c.sku
                    left join (
                     SELECT sku ,SUM(qty_send) as channel_quantity FROM hmcdata.bi_channel_order_info i
                    left join hmcdata.dc_shop ds on i.dc_shop_id =ds.id
                    left join hmcdata.bi_province_zone z on ds.bi_province_id=z.id
                    where order_status<>3 and total_day>='{date_start} 00:00:00' and total_day<='{date_end} 23:59:59' and product_sn=:product_sn  and color=:color {other_province_ids_sql}
                    group by sku
                    )d on a.sku=d.sku
                        """
        ret = Scope['vertica'].execute(sql, {'product_sn': product_sn, 'color': color})
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


@good.route('/get_transfer_in_max', methods=['GET'], endpoint='get_transfer_in_max')
@__token_wrapper
def get_transfer_in_max(context):
    '''
        获取调入最多
        :param context:
        :return:
        '''
    try:
        product_sn = request.args.get('product_sn')
        color = request.args.get('color')
        date_start = request.args.get('date_start', type=str)
        date_end = request.args.get('date_end', type=str)
        assign_type = request.args.get('assignType', type=int)
        province_ids = request.args.get('provinceIds', type=str)
        sku = request.args.get('sku', type=str)
        shop_name = request.args.get('shopName', type=str)
        if shop_name is None or shop_name == '':
            shop_name_sql = ''
        else:
            shop_name_sql = f"""and from_shop_name like '%{shop_name}%'"""
        if sku is None or sku == '':
            sku_sql = ''
        else:
            sku_sql = f"""and sku='{sku}'"""
        if province_ids is None or province_ids == '':
            province_ids_sql = """"""
            other_province_ids_sql = ""
        else:
            province_ids_sql = f"""and from_province_zone_id in ({province_ids}) """
            other_province_ids_sql = f"""and z.id in({province_ids}) """
        if assign_type is None or assign_type == '' or (isinstance(assign_type, int) and int(assign_type) >= 4):
            assign_sql = """"""
        else:
            assign_sql = f"""and assign_type ={assign_type} """
        if date_start is None or date_end is None:
            return jsonify(build_context(context, '时间不能为空', False, -1))
        sql_max_zone = f"""
                   SELECT SUM(apply_quantity) as quantity,z.province_name as name FROM hmcdata.bi_dist_model_transfer_base b
                left join hmcdata.bi_province_zone z on b.from_province_zone_id =z.id
                where create_time >='{date_start} 00:00:00' and create_time<='{date_end} 23:59:59' and product_sn=:product_sn and color=:color and apply_quantity>0
                {other_province_ids_sql} {assign_sql} {sku_sql} {shop_name_sql}
                GROUP BY province_name 
                ORDER BY quantity desc limit 1
                        """
        sql_max_shop = f""" 
                SELECT SUM(apply_quantity) as quantity,b.from_shop_name as name FROM hmcdata.bi_dist_model_transfer_base b
                where create_time >='{date_start} 00:00:00' and create_time<='{date_end} 23:59:59' and product_sn=:product_sn and color=:color and apply_quantity>0 
                {province_ids_sql} {assign_sql} {sku_sql} {shop_name_sql}
                GROUP BY b.from_shop_name  
                ORDER BY quantity desc limit 1
               """
        sql_max_receive = f""" 
                SELECT SUM(received_quantity) as quantity,b.from_shop_name as name FROM hmcdata.bi_dist_model_transfer_base b
                where create_time >='{date_start} 00:00:00' and create_time<='{date_end} 23:59:59' and product_sn=:product_sn and color=:color and received_quantity>0
                 {province_ids_sql} {assign_sql} {sku_sql} {shop_name_sql}
                GROUP BY b.from_shop_name  
                ORDER BY quantity desc limit 1"""
        ret_max_zone = Scope['vertica'].execute(sql_max_zone, {'product_sn': product_sn, 'color': color})
        max_zone_dict = query_list_to_object(ret_max_zone, ret_max_zone.keys())

        ret_max_shop = Scope['vertica'].execute(sql_max_shop, {'product_sn': product_sn, 'color': color})
        max_shop_dict = query_list_to_object(ret_max_shop, ret_max_shop.keys())

        ret_max_receive = Scope['vertica'].execute(sql_max_receive, {'product_sn': product_sn, 'color': color})
        max_receive = query_list_to_object(ret_max_receive, ret_max_receive.keys())
        data_dict = {}
        data_dict['max_zone_dict'] = max_zone_dict
        data_dict['max_shop_dict'] = max_shop_dict
        data_dict['max_receive'] = max_receive

        context['data'] = data_dict
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_transfer_out_max', methods=['GET'], endpoint='get_transfer_out_max')
@__token_wrapper
def get_transfer_out_max(context):
    '''
        获取调去最多
        :param context:
        :return:
        '''
    try:
        product_sn = request.args.get('product_sn')
        color = request.args.get('color')
        date_start = request.args.get('date_start', type=str)
        date_end = request.args.get('date_end', type=str)
        assign_type = request.args.get('assignType', type=int)
        province_ids = request.args.get('provinceIds', type=str)
        sku = request.args.get('sku', type=str)
        shop_name = request.args.get('shopName', type=str)
        if shop_name is None or shop_name == '':
            shop_name_sql = ''
        else:
            shop_name_sql = f"""and to_shop_name like '%{shop_name}%'"""
        if sku is None or sku == '':
            sku_sql = ''
        else:
            sku_sql = f"""and sku='{sku}'"""
        if province_ids is None or province_ids == '':
            province_ids_sql = """"""
            other_province_ids_sql = ""
        else:
            province_ids_sql = f"""and to_province_zone_id in ({province_ids}) """
            other_province_ids_sql = f"""and z.id in({province_ids}) """
        if assign_type is None or assign_type == '' or (isinstance(assign_type, int) and int(assign_type) >= 4):
            assign_sql = """"""
        else:
            assign_sql = f"""and assign_type ={assign_type} """
        if date_start is None or date_end is None:
            return jsonify(build_context(context, '时间不能为空', False, -1))
        sql_max_zone = f"""
                   SELECT SUM(apply_quantity) as quantity,z.province_name as name FROM hmcdata.bi_dist_model_transfer_base b
                left join hmcdata.bi_province_zone z on b.to_province_zone_id =z.id
                where create_time >='{date_start} 00:00:00' and create_time<='{date_end} 23:59:59' and product_sn=:product_sn and color=:color and apply_quantity>0 
                {other_province_ids_sql} {assign_sql} {sku_sql} {shop_name_sql}
                GROUP BY province_name 
                ORDER BY quantity desc limit 1
                        """
        sql_max_shop = f""" 
                SELECT SUM(apply_quantity) as quantity,b.to_shop_name as name FROM hmcdata.bi_dist_model_transfer_base b
                where create_time >='{date_start} 00:00:00' and create_time<='{date_end} 23:59:59' and product_sn=:product_sn and color=:color and apply_quantity>0
                {province_ids_sql} {assign_sql} {sku_sql} {shop_name_sql}
                GROUP BY b.to_shop_name  
                ORDER BY quantity desc limit 1
               """
        ret_max_zone = Scope['vertica'].execute(sql_max_zone, {'product_sn': product_sn, 'color': color})
        max_zone_dict = query_list_to_object(ret_max_zone, ret_max_zone.keys())

        ret_max_shop = Scope['vertica'].execute(sql_max_shop, {'product_sn': product_sn, 'color': color})
        max_shop_dict = query_list_to_object(ret_max_shop, ret_max_shop.keys())

        data_dict = {}
        data_dict['max_zone_dict'] = max_zone_dict
        data_dict['max_shop_dict'] = max_shop_dict

        context['data'] = data_dict
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_transfer_channel_max', methods=['GET'], endpoint='get_transfer_channel_max')
@__token_wrapper
def get_transfer_channel_max(context):
    '''
        获取渠道发货最多
        :param context:
        :return:
        '''
    try:
        product_sn = request.args.get('product_sn')
        color = request.args.get('color')
        date_start = request.args.get('date_start', type=str)
        date_end = request.args.get('date_end', type=str)
        province_ids = request.args.get('provinceIds', type=str)
        sku = request.args.get('sku', type=str)
        shop_name = request.args.get('shopName', type=str)
        if shop_name is None or shop_name == '':
            shop_name_sql = ''
        else:
            shop_name_sql = f"""and ds.shop_name like '%{shop_name}%'"""
        if sku is None or sku == '':
            sku_sql = ''
        else:
            sku_sql = f"""and sku='{sku}'"""
        if province_ids is None or province_ids == '':
            other_province_ids_sql = ""
        else:
            other_province_ids_sql = f"""and z.id in({province_ids}) """

        if date_start is None or date_end is None:
            return jsonify(build_context(context, '时间不能为空', False, -1))
        sql_max_zone = f"""
                   SELECT SUM(qty_send) as quantity,z.province_name as name FROM hmcdata.bi_channel_order_info i
                left join hmcdata.dc_shop ds on i.dc_shop_id =ds.id
                left join hmcdata.bi_province_zone z on ds.bi_province_id =z.id
                where order_status<>3 and total_day>='{date_start} 00:00:00' and total_day<='{date_end} 23:59:59' and product_sn=:product_sn and color=:color and qty_send>0
                {other_province_ids_sql} {sku_sql} {shop_name_sql}
                group by z.province_name
                ORDER BY quantity desc limit 1

                        """
        sql_max_shop = f""" 
                SELECT SUM(qty_send) as quantity,ds.shop_name as name FROM hmcdata.bi_channel_order_info i
                left join hmcdata.dc_shop ds on i.dc_shop_id =ds.id
                left join hmcdata.bi_province_zone z on ds.bi_province_id =z.id
                where order_status<>3 and total_day>='{date_start} 00:00:00' and total_day<='{date_end} 23:59:59' and product_sn=:product_sn and color=:color and qty_send>0
                {other_province_ids_sql} {sku_sql} {shop_name_sql}
                group by ds.shop_name
                ORDER BY quantity desc limit 1
               """
        ret_max_zone = Scope['vertica'].execute(sql_max_zone, {'product_sn': product_sn, 'color': color})
        max_zone_dict = query_list_to_object(ret_max_zone, ret_max_zone.keys())

        ret_max_shop = Scope['vertica'].execute(sql_max_shop, {'product_sn': product_sn, 'color': color})
        max_shop_dict = query_list_to_object(ret_max_shop, ret_max_shop.keys())

        data_dict = {}
        data_dict['max_zone_dict'] = max_zone_dict
        data_dict['max_shop_dict'] = max_shop_dict

        context['data'] = data_dict
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_transfer_in_shop_rank', methods=['GET'], endpoint='get_transfer_in_shop_rank')
@__token_wrapper
def get_transfer_in_shop_rank(context):
    '''
        获取调入店铺排名
        :param context:
        :return:
        '''
    try:
        product_sn = request.args.get('product_sn')
        color = request.args.get('color')
        date_start = request.args.get('date_start', type=str)
        date_end = request.args.get('date_end', type=str)
        assign_type = request.args.get('assignType', type=int)
        province_ids = request.args.get('provinceIds', type=str)
        page = request.args.get('page')
        page_size = request.args.get('pageSize')
        sorter = request.args.get('sorter')
        order = request.args.get('order')
        sku = request.args.get('sku', type=str)
        shop_name = request.args.get('shopName', type=str)
        if shop_name is None or shop_name == '':
            shop_name_sql = ''
            other_shop_name_sql = ''
        else:
            shop_name_sql = f"""and from_shop_name like '%{shop_name}%'"""
            other_shop_name_sql = f"""and ds.shop_name like '%{shop_name}%' """
        if sku is None or sku == '':
            sku_sql = ''
        else:
            sku_sql = f"""and sku='{sku}'"""
        order_sql = ""
        if province_ids is None or province_ids == '':
            province_ids_sql = """"""
            other_province_ids_sql = ""
        else:
            province_ids_sql = f"""and from_province_zone_id in ({province_ids}) """
            other_province_ids_sql = f"""and z.id in({province_ids}) """
        if assign_type is None or assign_type == '' or (isinstance(assign_type, int) and int(assign_type) >= 4):
            assign_sql = """"""
        else:
            assign_sql = f"""and b.assign_type ={assign_type} """
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
        if date_start is None or date_end is None:
            return jsonify(build_context(context, '时间不能为空', False, -1))
        sql = f"""
                   SELECT a.province_name,a.from_shop_code as shop_code,a.from_shop_name as shop_name,a.apply_quantity,a.apply_to_send_quantity,a.onway_nums,a.received_quantity,
                    b.sale_nums FROM (
                    SELECT z.province_name ,from_dc_shop_id ,from_shop_code ,from_shop_name ,sum(apply_quantity) as apply_quantity,SUM(case status
                    when '待发货' then apply_quantity else 0 end) as apply_to_send_quantity,
                    sum(case status when '待收货' then delivery_quantity-received_quantity else 0 end) as onway_nums,
                    sum(received_quantity) as received_quantity
                    FROM hmcdata.bi_dist_model_transfer_base b 
                    left join hmcdata.bi_province_zone z on b.from_province_zone_id =z.id
                    where  create_time >='{date_start} 00:00:00' and create_time<='{date_end} 23:59:59' and product_sn=:product_sn and color=:color 
                    {other_province_ids_sql} {assign_sql} {sku_sql} {shop_name_sql}
                    GROUP BY from_shop_name,from_shop_code,from_dc_shop_id,z.province_name  
                    )a left join(
                    SELECT ds.id,SUM(goods_number) as sale_nums FROM hmcdata.product_reorder_recommendation_offline_sku_day_sales i
                    left join hmcdata.dc_shop ds on i.dc_shop_id =ds.id
                    left join hmcdata.bi_province_zone z on ds.bi_province_id=z.id
                    where  total_day>='{date_start} 00:00:00' and total_day<='{date_end} 23:59:59' and product_sn=:product_sn and color_name=:color 
                    {other_province_ids_sql} {sku_sql} {other_shop_name_sql}
                    group by ds.id
                    )b on a.from_dc_shop_id=b.id {sorter_sql} {order_sql} {offset} 
                        """
        ret = Scope['vertica'].execute(sql, {'product_sn': product_sn, 'color': color})
        columns = ret.keys()
        data = []
        for rank, val in enumerate(ret):
            data_dict = {'rank': rank + 1 + (int(page) - 1) * int(page_size)}
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
                    SELECT 1
                    FROM hmcdata.bi_dist_model_transfer_base b 
                    left join hmcdata.bi_province_zone z on b.from_province_zone_id =z.id
                    where  create_time >='{date_start} 00:00:00' and create_time<='{date_end} 23:59:59' 
                    and product_sn=:product_sn and color=:color {other_province_ids_sql} {assign_sql} {sku_sql} {shop_name_sql}
                    GROUP BY from_shop_name,from_shop_code,from_dc_shop_id,z.province_name  
                    )a
                     """
        ret_total = Scope['vertica'].execute(total_sql, {'product_sn': product_sn, 'color': color})
        total = ret_total.fetchone()[0]
        context['data'] = data
        context['total'] = total
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_transfer_out_shop_rank', methods=['GET'], endpoint='get_transfer_out_shop_rank')
@__token_wrapper
def get_transfer_out_shop_rank(context):
    '''
        获取调出店铺排名
        :param context:
        :return:
        '''
    try:
        product_sn = request.args.get('product_sn')
        color = request.args.get('color')
        date_start = request.args.get('date_start', type=str)
        date_end = request.args.get('date_end', type=str)
        assign_type = request.args.get('assignType', type=int)
        province_ids = request.args.get('provinceIds', type=str)
        page = request.args.get('page')
        page_size = request.args.get('pageSize')
        sorter = request.args.get('sorter')
        order = request.args.get('order')
        sku = request.args.get('sku', type=str)
        shop_name = request.args.get('shopName', type=str)
        if shop_name is None or shop_name == '':
            shop_name_sql = ''
            other_shop_name_sql = ''
        else:
            shop_name_sql = f"""and to_shop_name like '%{shop_name}%'"""
            other_shop_name_sql = f"""and ds.shop_name like '%{shop_name}%' """

        if sku is None or sku == '':
            sku_sql = ''
        else:
            sku_sql = f"""and sku='{sku}'"""
        order_sql = ""
        if province_ids is None or province_ids == '':
            province_ids_sql = """"""
            other_province_ids_sql = ""
        else:
            province_ids_sql = f"""and from_province_zone_id in ({province_ids}) """
            other_province_ids_sql = f"""and z.id in({province_ids}) """

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
        if date_start is None or date_end is None:
            return jsonify(build_context(context, '时间不能为空', False, -1))
        sql = f"""
                   SELECT a.province_name,a.to_shop_code as shop_code,a.to_shop_name as shop_name,a.apply_quantity,a.delivery_quantity,
                    b.sale_nums,case when a.apply_quantity=0 then 0 else a.delivery_quantity/a.apply_quantity end as success_rate FROM (
                    SELECT z.province_name ,to_dc_shop_id ,to_shop_code ,to_shop_name ,sum(apply_quantity) as apply_quantity,
                    sum(case  when status='待收货' or status='已收货' then delivery_quantity else 0 end) as delivery_quantity
                    FROM hmcdata.bi_dist_model_transfer_base b 
                    left join hmcdata.bi_province_zone z on b.to_province_zone_id =z.id
                    where  create_time >='{date_start} 00:00:00' and create_time<='{date_end} 23:59:59' and product_sn=:product_sn and color=:color 
                    {other_province_ids_sql} {sku_sql} {shop_name_sql}
                    GROUP BY to_shop_name,to_shop_code,to_dc_shop_id,z.province_name  
                    )a left join(
                    SELECT ds.id,SUM(goods_number) as sale_nums FROM hmcdata.product_reorder_recommendation_offline_sku_day_sales i
                    left join hmcdata.dc_shop ds on i.dc_shop_id =ds.id
                    left join hmcdata.bi_province_zone z on ds.bi_province_id=z.id
                    where  total_day>='{date_start} 00:00:00' and total_day<='{date_end} 23:59:59' and product_sn=:product_sn and color_name=:color 
                    {other_province_ids_sql}  {sku_sql} {other_shop_name_sql}
                    group by ds.id
                    )b on a.to_dc_shop_id=b.id {sorter_sql} {order_sql} {offset} 
                        """
        ret = Scope['vertica'].execute(sql, {'product_sn': product_sn, 'color': color})
        columns = ret.keys()
        data = []
        for rank, val in enumerate(ret):
            data_dict = {'rank': rank + 1 + (int(page) - 1) * int(page_size)}
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
                    SELECT 1
                    FROM hmcdata.bi_dist_model_transfer_base b 
                    left join hmcdata.bi_province_zone z on b.to_province_zone_id =z.id
                    where  create_time >='{date_start} 00:00:00' and create_time<='{date_end} 23:59:59' and product_sn=:product_sn and color=:color 
                    {other_province_ids_sql}  {sku_sql} {shop_name_sql}
                    GROUP BY to_shop_name,to_shop_code,to_dc_shop_id,z.province_name  
                    )a
                     """
        ret_total = Scope['vertica'].execute(total_sql, {'product_sn': product_sn, 'color': color})
        total = ret_total.fetchone()[0]
        context['data'] = data
        context['total'] = total
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_transfer_channel_shop_rank', methods=['GET'], endpoint='get_transfer_channel_shop_rank')
@__token_wrapper
def get_transfer_channel_shop_rank(context):
    '''
        获取渠道订单店铺排名
        :param context:
        :return:
        '''
    try:
        product_sn = request.args.get('product_sn')
        color = request.args.get('color')
        date_start = request.args.get('date_start', type=str)
        date_end = request.args.get('date_end', type=str)
        assign_type = request.args.get('assignType', type=int)
        province_ids = request.args.get('provinceIds', type=str)
        page = request.args.get('page')
        page_size = request.args.get('pageSize')
        sorter = request.args.get('sorter')
        order = request.args.get('order')
        sku = request.args.get('sku', type=str)
        shop_name = request.args.get('shopName', type=str)
        if shop_name is None or shop_name == '':
            shop_name_sql = ''
        else:
            shop_name_sql = f"""and ds.shop_name like '%{shop_name}%'"""
        if sku is None or sku == '':
            sku_sql = ''
        else:
            sku_sql = f"""and sku='{sku}'"""
        if province_ids is None or province_ids == '':
            other_province_ids_sql = ""
        else:
            other_province_ids_sql = f"""and z.id in({province_ids}) """
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
        if date_start is None or date_end is None:
            return jsonify(build_context(context, '时间不能为空', False, -1))
        sql = f"""
                   SELECT a.apply_quantity,a.qty_send,a.shop_name,a.shop_code,a.province_name,a.zone_name,b.sale_nums,
                    case when a.apply_quantity=0 then 0 else a.qty_send/a.apply_quantity end as success_rate FROM (
                    SELECT SUM(quantity) as apply_quantity ,SUM(qty_send) as qty_send,ds.shop_name,ds.shop_code,z.province_name,b.zone_name,i.dc_shop_id FROM hmcdata.bi_channel_order_info i
                    left join hmcdata.dc_shop ds on i.dc_shop_id =ds.id
                    left join hmcdata.bi_province_zone z on ds.bi_province_id=z.id and ds.business_unit_id =z.business_unit_id 
                    left join hmcdata.bi_big_zone b on z.zone_id =b.id 
                    where  create_time>='{date_start} 00:00:00' and create_time<='{date_end} 23:59:59' and product_sn=:product_sn and color=:color 
                    {other_province_ids_sql} {sku_sql} {shop_name_sql}
                    group by ds.shop_name,ds.shop_code,z.province_name,b.zone_name,i.dc_shop_id
                    ) a
                    left join (
                    SELECT ds.id,SUM(goods_number) as sale_nums FROM hmcdata.product_reorder_recommendation_offline_sku_day_sales i
                    left join hmcdata.dc_shop ds on i.dc_shop_id =ds.id
                    left join hmcdata.bi_province_zone z on ds.bi_province_id=z.id
                    where  total_day>='{date_start} 00:00:00' and total_day<='{date_end} 23:59:59' and product_sn=:product_sn and color_name=:color  
                    {other_province_ids_sql} {sku_sql} {shop_name_sql}
                    group by ds.id
                    ) b on a.dc_shop_id=b.id {sorter_sql} {order_sql} {offset} 
                        """
        ret = Scope['vertica'].execute(sql, {'product_sn': product_sn, 'color': color})
        columns = ret.keys()
        data = []
        for rank, val in enumerate(ret):
            data_dict = {'rank': rank + 1 + (int(page) - 1) * int(page_size)}
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
                    SELECT 1 FROM hmcdata.bi_channel_order_info i
                    left join hmcdata.dc_shop ds on i.dc_shop_id =ds.id
                    left join hmcdata.bi_province_zone z on ds.bi_province_id=z.id and ds.business_unit_id =z.business_unit_id 
                    left join hmcdata.bi_big_zone b on z.zone_id =b.id 
                    where  create_time>='{date_start} 00:00:00' and create_time<='{date_end} 23:59:59' and product_sn=:product_sn and color=:color  
                    {other_province_ids_sql} {sku_sql} {shop_name_sql}
                    group by ds.shop_name,ds.shop_code,z.province_name,b.zone_name,i.dc_shop_id
                    )a
                     """
        ret_total = Scope['vertica'].execute(total_sql, {'product_sn': product_sn, 'color': color})
        total = ret_total.fetchone()[0]
        context['data'] = data
        context['total'] = total
        return jsonify(context)
    finally:
        Scope['vertica'].remove()
