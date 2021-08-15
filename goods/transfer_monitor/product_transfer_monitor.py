# -*- coding: utf-8 -*-
# @Time    : 2021/4/1 17:17
# @Author  : Joshua
# @File    : product_transfer_monitor

from flask import jsonify, request
from bi_flask.__token import __token_wrapper
import logging
import json
from bi_flask._sessions import sessions, sessions_scopes
from bi_flask.utils import *
from bi_flask.goods.api import good

logger = logging.getLogger('bi')
Scope = sessions_scopes(sessions)

province_list = ['北京', '天津', '宁夏回族自治区', '新疆维吾尔自治区', '内蒙古自治区', '上海', '广西壮族自治区', '重庆', '西藏自治区', '海外', '中国香港特别行政区',
                 '中国澳门特别行政区']


@good.route('/get_transfer_info', methods=['GET'], endpoint='get_transfer_info')
@__token_wrapper
def get_transfer_info(context):
    '''
        获取调拨单数据
        :param context:
        :return:
        '''
    try:
        business_unit_id = context['data']['businessUnitId']
        date_start = request.args.get('date_start', type=str)
        date_end = request.args.get('date_end', type=str)
        assign_type = request.args.get('assignType', type=int)
        province_ids = request.args.get('provinceIds', type=str)
        # shop_id_list = context['data']['offline_shop_id_list']
        # if len(shop_id_list) == 0:
        #     context['data'] = []
        #     return jsonify(context)
        # shop_ids = ','.join(shop_id_list)
        if date_start is None or date_end is None:
            return jsonify(build_context(context, '时间不能为空', False, -1))
        if assign_type is None or assign_type == '' or (isinstance(assign_type, int) and int(assign_type) >= 3):
            assign_sql = """"""
        else:
            assign_sql = f"""and assign_type ={assign_type} """
        if province_ids is None or province_ids == '':
            province_ids_sql = """"""
        else:
            province_ids_sql = f"""and from_province_zone_id in ({province_ids}) """

        sql = f"""
                        SELECT COUNT(DISTINCT code) as 'order_count',SUM(actual_quantity) as actual_quantity,COUNT(DISTINCT from_dc_shop_id) as 'from_shop_count',
                        COUNT(DISTINCT to_dc_shop_id) as to_shop_count FROM hmcdata.bi_dist_model_transfer_base where status in('待收货',
                        '已收货',
                        '待发货') and create_time >='{date_start} 00:00:00' and create_time <='{date_end} 23:59:59' and business_unit_id =:business_unit_id {assign_sql} {province_ids_sql}
                        """
        ret = Scope['vertica'].execute(sql, {'business_unit_id': business_unit_id})
        columns = ret.keys()
        data_dict = {}
        for rank, val in enumerate(ret):
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
        context['data'] = data_dict
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_transfer_info_season', methods=['GET'], endpoint='get_transfer_info_season')
@__token_wrapper
def get_transfer_info_season(context):
    '''
        获取调拨单季节分布
        :param context:
        :return:
        '''
    try:
        business_unit_id = context['data']['businessUnitId']
        date_start = request.args.get('date_start', type=str)
        date_end = request.args.get('date_end', type=str)
        assign_type = request.args.get('assignType', type=int)
        province_ids = request.args.get('provinceIds', type=str)
        # shop_id_list = context['data']['offline_shop_id_list']
        # if len(shop_id_list) == 0:
        #     context['data'] = []
        #     return jsonify(context)
        # shop_ids = ','.join(shop_id_list)
        if date_start is None or date_end is None:
            return jsonify(build_context(context, '时间不能为空', False, -1))
        if assign_type is None or assign_type == '' or (isinstance(assign_type, int) and int(assign_type) >= 3):
            assign_sql = """"""
        else:
            assign_sql = f"""and assign_type ={assign_type} """
        if province_ids is None or province_ids == '':
            province_ids_sql = """"""
        else:
            province_ids_sql = f"""and from_province_zone_id in ({province_ids}) """

        sql = f"""
                        SELECT SUM(actual_quantity) as value,product_season as name FROM hmcdata.bi_dist_model_transfer_base 
                        where  status in('待收货','已收货',
                        '待发货') and create_time >='{date_start} 00:00:00' and create_time <='{date_end} 23:59:59' and business_unit_id =:business_unit_id {assign_sql} {province_ids_sql}
                        GROUP BY product_season  

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
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_transfer_info_category', methods=['GET'], endpoint='get_transfer_info_category')
@__token_wrapper
def get_transfer_info_category(context):
    '''
        获取调拨单类目分布
        :param context:
        :return:
        '''
    try:
        business_unit_id = context['data']['businessUnitId']
        date_start = request.args.get('date_start', type=str)
        date_end = request.args.get('date_end', type=str)
        assign_type = request.args.get('assignType', type=int)
        province_ids = request.args.get('provinceIds', type=str)
        # shop_id_list = context['data']['offline_shop_id_list']
        # if len(shop_id_list) == 0:
        #     context['data'] = []
        #     return jsonify(context)
        # shop_ids = ','.join(shop_id_list)
        if date_start is None or date_end is None:
            return jsonify(build_context(context, '时间不能为空', False, -1))
        if assign_type is None or assign_type == '' or (isinstance(assign_type, int) and int(assign_type) >= 3):
            assign_sql = """"""
        else:
            assign_sql = f"""and assign_type ={assign_type} """
        if province_ids is None or province_ids == '':
            province_ids_sql = """"""
        else:
            province_ids_sql = f"""and from_province_zone_id in ({province_ids}) """

        sql = f"""
                        SELECT SUM(actual_quantity) as value,category as name FROM hmcdata.bi_dist_model_transfer_base 
                        where  status in('待收货','已收货',
                        '待发货') and create_time >='{date_start} 00:00:00' and create_time <='{date_end} 23:59:59' and business_unit_id =:business_unit_id {assign_sql} {province_ids_sql}
                        GROUP BY category  
                        order by value desc

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
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_transfer_info_province', methods=['GET'], endpoint='get_transfer_info_province')
@__token_wrapper
def get_transfer_info_province(context):
    '''
        获取调拨单省份分布
        :param context:
        :return:
        '''
    try:
        business_unit_id = context['data']['businessUnitId']
        date_start = request.args.get('date_start', type=str)
        date_end = request.args.get('date_end', type=str)
        assign_type = request.args.get('assignType', type=int)
        province_ids = request.args.get('provinceIds', type=str)
        transfer_type = request.args.get('transferType', type=str)
        # shop_id_list = context['data']['offline_shop_id_list']
        # if len(shop_id_list) == 0:
        #     context['data'] = []
        #     return jsonify(context)
        # shop_ids = ','.join(shop_id_list)
        if date_start is None or date_end is None:
            return jsonify(build_context(context, '时间不能为空', False, -1))
        if assign_type is None or assign_type == '' or (isinstance(assign_type, int) and int(assign_type) >= 3):
            assign_sql = """"""
        else:
            assign_sql = f"""and assign_type ={assign_type} """
        if province_ids is None or province_ids == '':
            province_ids_sql = """"""
        else:
            province_ids_sql = f"""and from_province_zone_id in ({province_ids}) """
        if transfer_type == 'transferIn':
            sql = f"""
                            SELECT SUM(actual_quantity) as value,replace(from_province,'省','') as name,from_province as label FROM hmcdata.bi_dist_model_transfer_base where  status in('待收货',
                            '已收货',
                            '待发货') and create_time >='{date_start} 00:00:00' and create_time <='{date_end} 23:59:59' and business_unit_id =:business_unit_id {assign_sql} {province_ids_sql}
                            GROUP BY from_province 
                            """
        else:
            sql = f"""
                    SELECT SUM(actual_quantity) as value,replace(to_province,'省','') as name,to_province as label FROM hmcdata.bi_dist_model_transfer_base where  status in('待收货',
                    '已收货',
                    '待发货') and create_time >='{date_start} 00:00:00' and create_time <='{date_end} 23:59:59' and business_unit_id =:business_unit_id {assign_sql} {province_ids_sql}
                    GROUP BY to_province 
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
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_transfer_info_province_line_map', methods=['GET'], endpoint='get_transfer_info_province_line_map')
@__token_wrapper
def get_transfer_info_province_line_map(context):
    '''
        获取调拨单省份分布
        :param context:
        :return:
        '''
    try:
        business_unit_id = context['data']['businessUnitId']
        date_start = request.args.get('date_start', type=str)
        date_end = request.args.get('date_end', type=str)
        assign_type = request.args.get('assignType', type=int)
        province_ids = request.args.get('provinceIds', type=str)
        transfer_type = request.args.get('transferType', type=str)
        province = request.args.get('province', type=str)
        # shop_id_list = context['data']['offline_shop_id_list']
        # if len(shop_id_list) == 0:
        #     context['data'] = []
        #     return jsonify(context)
        # shop_ids = ','.join(shop_id_list)
        if date_start is None or date_end is None:
            return jsonify(build_context(context, '时间不能为空', False, -1))
        if assign_type is None or assign_type == '' or (isinstance(assign_type, int) and int(assign_type) >= 3):
            assign_sql = """"""
        else:
            assign_sql = f"""and assign_type ={assign_type} """
        if province_ids is None or province_ids == '':
            province_ids_sql = """"""
        else:
            province_ids_sql = f"""and from_province_zone_id in ({province_ids}) """
        if transfer_type == 'transferIn':
            sql = f"""
                            SELECT SUM(actual_quantity) as value,replace(to_province,'省','') as name FROM hmcdata.bi_dist_model_transfer_base where  status in('待收货',
                            '已收货',
                            '待发货') and create_time >='{date_start} 00:00:00' and create_time <='{date_end} 23:59:59' 
                            and business_unit_id =:business_unit_id and from_province=:province {assign_sql} {province_ids_sql} 
                            GROUP BY to_province 
                            order by value desc
                            """
            shop_count_sql = f""" SELECT count(distinct from_shop_code) as shop_count FROM hmcdata.bi_dist_model_transfer_base where status in('待收货',
                            '已收货',
                            '待发货') and create_time >='{date_start} 00:00:00' and create_time <='{date_end} 23:59:59' 
                            and business_unit_id =:business_unit_id and from_province=:province {assign_sql} {province_ids_sql} 
                            """
        else:
            sql = f"""
                    SELECT SUM(actual_quantity) as value,replace(from_province,'省','') as name FROM hmcdata.bi_dist_model_transfer_base where status in('待收货',
                            '已收货',
                            '待发货') and create_time >='{date_start} 00:00:00' and create_time <='{date_end} 23:59:59' 
                            and business_unit_id =:business_unit_id and to_province=:province {assign_sql} {province_ids_sql} 
                            GROUP BY from_province 
                            order by value desc
                    """
            shop_count_sql = f"""  SELECT count(distinct to_shop_code) as shop_count FROM hmcdata.bi_dist_model_transfer_base where status in('待收货',
                            '已收货',
                            '待发货') and create_time >='{date_start} 00:00:00' and create_time <='{date_end} 23:59:59' 
                            and business_unit_id =:business_unit_id and to_province=:province {assign_sql} {province_ids_sql} 
                                """
        ret = Scope['vertica'].execute(sql, {'business_unit_id': business_unit_id, 'province': province})
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
        ret_shop_count = Scope['vertica'].execute(shop_count_sql,
                                                  {'business_unit_id': business_unit_id, 'province': province})
        shop_count = 0
        shop_count_result = ret_shop_count.fetchone()
        if shop_count_result is not None:
            shop_count = shop_count_result[0]
        context['shop_count'] = shop_count
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_transfer_info_province_distribute', methods=['GET'], endpoint='get_transfer_info_province_distribute')
@__token_wrapper
def get_transfer_info_province_distribute(context):
    '''
        获取调拨单状态分布
        :param context:
        :return:
        '''
    try:
        business_unit_id = context['data']['businessUnitId']
        date_start = request.args.get('date_start', type=str)
        date_end = request.args.get('date_end', type=str)
        assign_type = request.args.get('assignType', type=int)
        province_ids = request.args.get('provinceIds', type=str)
        transfer_type = request.args.get('transferType', type=str)
        province = request.args.get('province', type=str)
        # shop_id_list = context['data']['offline_shop_id_list']
        # if len(shop_id_list) == 0:
        #     context['data'] = []
        #     return jsonify(context)
        # shop_ids = ','.join(shop_id_list)
        if date_start is None or date_end is None:
            return jsonify(build_context(context, '时间不能为空', False, -1))
        if assign_type is None or assign_type == '' or (isinstance(assign_type, int) and int(assign_type) >= 3):
            assign_sql = """"""
        else:
            assign_sql = f"""and assign_type ={assign_type} """
        if province_ids is None or province_ids == '':
            province_ids_sql = """"""
        else:
            province_ids_sql = f"""and from_province_zone_id in ({province_ids}) """
        if province is None or province == '':
            province_sql = ""
        else:
            if transfer_type == 'transferIn':
                province_sql = f"""and from_province='{province}' """
            else:
                province_sql = f"""and to_province='{province}' """

        sql = f"""
                        SELECT SUM(actual_quantity) as value,assign_type,status FROM hmcdata.bi_dist_model_transfer_base 
                        where   create_time >='{date_start} 00:00:00' and create_time <='{date_end} 23:59:59' and
                        business_unit_id =:business_unit_id {province_sql} {assign_sql} {province_ids_sql} 
                        GROUP BY assign_type ,status
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
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_transfer_info_Initiation_type', methods=['GET'], endpoint='get_transfer_info_Initiation_type')
@__token_wrapper
def get_transfer_info_Initiation_type(context):
    '''
        获取调拨发起类型分析
        :param context:
        :return:
        '''
    try:
        business_unit_id = context['data']['businessUnitId']
        date_start = request.args.get('date_start', type=str)
        date_end = request.args.get('date_end', type=str)
        assign_type = request.args.get('assignType', type=int)
        province_ids = request.args.get('provinceIds', type=str)
        # shop_id_list = context['data']['offline_shop_id_list']
        # if len(shop_id_list) == 0:
        #     context['data'] = []
        #     return jsonify(context)
        # shop_ids = ','.join(shop_id_list)
        if date_start is None or date_end is None:
            return jsonify(build_context(context, '时间不能为空', False, -1))
        if assign_type is None or assign_type == '' or (isinstance(assign_type, int) and int(assign_type) >= 3):
            assign_sql = """"""
        else:
            assign_sql = f"""and assign_type ={assign_type} """
        if province_ids is None or province_ids == '':
            province_ids_sql = """"""
        else:
            province_ids_sql = f"""and from_province_zone_id in ({province_ids}) """

        sql = f"""
                        SELECT SUM(actual_quantity) as value,type,status FROM hmcdata.bi_dist_model_transfer_base 
                        where   create_time >='{date_start} 00:00:00' and create_time <='{date_end} 23:59:59' and
                        business_unit_id =:business_unit_id  {assign_sql} {province_ids_sql} 
                        GROUP BY type ,status
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
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_transfer_info_transfer_shop_rank', methods=['GET'], endpoint='get_transfer_info_transfer_shop_rank')
@__token_wrapper
def get_transfer_info_transfer_shop_rank(context):
    '''
        获取调拨调入/调出店铺TOP10
        :param context:
        :return:
        '''
    try:
        business_unit_id = context['data']['businessUnitId']
        date_start = request.args.get('date_start', type=str)
        date_end = request.args.get('date_end', type=str)
        assign_type = request.args.get('assignType', type=int)
        province_ids = request.args.get('provinceIds', type=str)
        # shop_id_list = context['data']['offline_shop_id_list']
        # if len(shop_id_list) == 0:
        #     context['data'] = []
        #     return jsonify(context)
        # shop_ids = ','.join(shop_id_list)
        if date_start is None or date_end is None:
            return jsonify(build_context(context, '时间不能为空', False, -1))
        if assign_type is None or assign_type == '' or (isinstance(assign_type, int) and int(assign_type) >= 3):
            assign_sql = """"""
        else:
            assign_sql = f"""and assign_type ={assign_type} """
        if province_ids is None or province_ids == '':
            province_ids_sql = """"""
        else:
            province_ids_sql = f"""and from_province_zone_id in ({province_ids}) """
        result_data = {}
        sql_in = f"""
                    SELECT from_dc_shop_id as dc_shop_id,from_shop_name as shop_name,from_shop_code as shop_code,from_level_name as level_name ,sum(actual_quantity) as tranfer_nums,from_management_type as management_type FROM hmcdata.bi_dist_model_transfer_base b
                    where status in('待收货',
                            '已收货',
                            '待发货') and  create_time >='{date_start} 00:00:00' and create_time <='{date_end} 23:59:59' and
                        business_unit_id =:business_unit_id  {assign_sql} {province_ids_sql} 
                    group by from_shop_name ,from_level_name,from_shop_code,from_management_type,from_dc_shop_id
                    ORDER by tranfer_nums desc limit 10
                        """
        ret_in = Scope['vertica'].execute(sql_in, {'business_unit_id': business_unit_id})
        columns = ret_in.keys()
        data = []
        for rank, val in enumerate(ret_in):
            data_dict = {'rank': rank + 1, 'key': f'running{rank}'}
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
        result_data['data_in'] = data

        sql_out = f"""
                    SELECT to_dc_shop_id as dc_shop_id,to_shop_name as shop_name,to_shop_code as shop_code,to_level_name as level_name,sum(actual_quantity) as tranfer_nums,to_management_type FROM hmcdata.bi_dist_model_transfer_base
                            where status in('待收货',
                            '已收货',
                            '待发货') and create_time >='{date_start} 00:00:00' and create_time <='{date_end} 23:59:59' and
                                business_unit_id =:business_unit_id  {assign_sql} {province_ids_sql} 
                    group by to_shop_name ,to_level_name,to_shop_code,to_management_type,to_dc_shop_id
                            ORDER by tranfer_nums desc limit 10
                                """
        ret_out = Scope['vertica'].execute(sql_out, {'business_unit_id': business_unit_id})
        columns = ret_in.keys()
        data_out = []
        for rank, val in enumerate(ret_out):
            data_dict = {'rank': rank + 1, 'key': f'cancel{rank}'}
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
            data_out.append(data_dict)
        result_data['data_out'] = data_out
        context['data'] = result_data
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_transfer_info_transfer_onway_time', methods=['GET'], endpoint='get_transfer_info_transfer_onway_time')
@__token_wrapper
def get_transfer_info_transfer_onway_time(context):
    '''
        获取调拨收货在途时长
        :param context:
        :return:
        '''
    try:
        business_unit_id = context['data']['businessUnitId']
        date_start = request.args.get('date_start', type=str)
        date_end = request.args.get('date_end', type=str)
        assign_type = request.args.get('assignType', type=int)
        province_ids = request.args.get('provinceIds', type=str)
        date_s_last = datetime.datetime.strptime(date_start, '%Y-%m-%d').date() - dateutil.relativedelta.relativedelta(
            years=1)
        date_e_last = datetime.datetime.strptime(date_end, '%Y-%m-%d').date() - dateutil.relativedelta.relativedelta(
            years=1)
        shop_id_list = context['data']['offline_shop_id_list']
        # if len(shop_id_list) == 0:
        #     context['data'] = []
        #     return jsonify(context)
        # shop_ids = ','.join(shop_id_list)
        if date_start is None or date_end is None:
            return jsonify(build_context(context, '时间不能为空', False, -1))
        if assign_type is None or assign_type == '' or (isinstance(assign_type, int) and int(assign_type) >= 3):
            assign_sql = """"""
        else:
            assign_sql = f"""and assign_type ={assign_type} """
        if province_ids is None or province_ids == '':
            province_ids_sql = """"""
        else:
            province_ids_sql = f"""and from_province_zone_id in ({province_ids}) """
        result_data = {}
        sql_now = f"""
                    SELECT CASE when TIMESTAMPDIFF(SECOND ,create_time,completed_time) <86400 then 0 when TIMESTAMPDIFF(SECOND ,create_time,completed_time)<86400*2 then 1 
                    when TIMESTAMPDIFF(SECOND ,create_time,completed_time)<86400*3 then 2 when TIMESTAMPDIFF(SECOND ,create_time,completed_time)<86400*4 then 3
                    when TIMESTAMPDIFF(SECOND ,create_time,completed_time)<86400*4 then 4 when TIMESTAMPDIFF(SECOND ,create_time,completed_time)<86400*5 then 5  
                     when TIMESTAMPDIFF(SECOND ,create_time,completed_time)<86400*6 then 6   when TIMESTAMPDIFF(SECOND ,create_time,completed_time)<86400*7 then 7  
                    else 8 end
                    as day_round,sum(received_quantity) as received_quantity
                    FROM hmcdata.bi_dist_model_transfer_base where status ='已收货' and completed_time is not null
                     and  create_time >='{date_start} 00:00:00' and create_time <='{date_end} 23:59:59' and
                        business_unit_id =:business_unit_id  {assign_sql} {province_ids_sql} 
                    GROUP BY day_round
                        """
        ret_in = Scope['vertica'].execute(sql_now, {'business_unit_id': business_unit_id})
        columns = ret_in.keys()
        data = []
        for rank, val in enumerate(ret_in):
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
        result_data['now'] = data

        sql_last = f"""
                            SELECT CASE when TIMESTAMPDIFF(SECOND ,create_time,completed_time) <86400 then 0 when TIMESTAMPDIFF(SECOND ,create_time,completed_time)<86400*2 then 1 
                            when TIMESTAMPDIFF(SECOND ,create_time,completed_time)<86400*3 then 2 when TIMESTAMPDIFF(SECOND ,create_time,completed_time)<86400*4 then 3
                            when TIMESTAMPDIFF(SECOND ,create_time,completed_time)<86400*4 then 4 when TIMESTAMPDIFF(SECOND ,create_time,completed_time)<86400*5 then 5  
                             when TIMESTAMPDIFF(SECOND ,create_time,completed_time)<86400*6 then 6   when TIMESTAMPDIFF(SECOND ,create_time,completed_time)<86400*7 then 7  
                            else 8 end
                            as day_round,sum(received_quantity) as received_quantity
                            FROM hmcdata.bi_dist_model_transfer_base where status ='已收货' and completed_time is not null
                            and  create_time >='{date_s_last} 00:00:00' and create_time <='{date_e_last} 23:59:59' and
                                business_unit_id =:business_unit_id  {assign_sql} {province_ids_sql} 
                            GROUP BY day_round
                                """
        ret_last = Scope['vertica'].execute(sql_last, {'business_unit_id': business_unit_id})
        columns = ret_last.keys()
        last_data = []
        for rank, val in enumerate(ret_last):
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
            last_data.append(data_dict)
        result_data['last'] = last_data
        context['data'] = result_data
        return jsonify(context)
    finally:
        Scope['vertica'].remove()






@good.route('/get_transfer_info_order_info_pie', methods=['GET'], endpoint='get_transfer_info_order_info_pie')
@__token_wrapper
def get_transfer_info_order_info_pie(context):
    '''
        获取调拨单地理分析和收货天数分析
        :param context:
        :return:
        '''
    try:
        business_unit_id = context['data']['businessUnitId']
        date_start = request.args.get('date_start', type=str)
        date_end = request.args.get('date_end', type=str)
        if date_start is None or date_end is None:
            return jsonify(build_context(context, '时间不能为空', False, -1))
        result_data = {}
        sql_distance = f"""
                    SELECT distance_type as name ,count(DISTINCT code) as value FROM hmcdata.bi_dist_model_transfer_order_info
                where status in('待发货','待收货','已收货') and create_time >='{date_start} 00:00:00' and create_time <='{date_end} 23:59:59' and business_unit_id=:business_unit_id
                group by distance_type 
                order by distance_type
                        """
        ret_distance = Scope['vertica'].execute(sql_distance, {'business_unit_id': business_unit_id})
        columns = ret_distance.keys()
        data = []
        for rank, val in enumerate(ret_distance):
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
        result_data['distance'] = data

        sql_day_round = f"""
                            SELECT day_round as name,count(DISTINCT code) as value FROM hmcdata.bi_dist_model_transfer_order_info
                        where status ='已收货' and create_time >='{date_start} 00:00:00' and create_time <='{date_end} 23:59:59'
                        and business_unit_id=:business_unit_id
                        group by day_round 
                        order by day_round
                                """
        ret_day_round = Scope['vertica'].execute(sql_day_round, {'business_unit_id': business_unit_id})
        columns = ret_day_round.keys()
        last_data = []
        for rank, val in enumerate(ret_day_round):
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
            last_data.append(data_dict)
        result_data['day_round'] = last_data
        context['data'] = result_data
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_transfer_info_order_info_table', methods=['GET'], endpoint='get_transfer_info_order_info_table')
@__token_wrapper
def get_transfer_info_order_info_table(context):
    '''
        获取调拨单订单数据
        :param context:
        :return:
        '''
    try:
        business_unit_id = context['data']['businessUnitId']
        date_start = request.args.get('date_start', type=str)
        date_end = request.args.get('date_end', type=str)
        page = request.args.get('page', type=int)
        page_size = request.args.get('pageSize')
        sorter = request.args.get('sorter')
        order = request.args.get('order')
        status = request.args.get('status', type=str)
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
        if status is None or status=='':
            status_sql=''
        else:
            status_sql = f""" and status='{status}' """
        if page_size is None:
            page_size = 10
        offset = f''' LIMIT {int(page_size)} OFFSET {(int(page) - 1) * int(page_size)} '''
        sql = f"""
                    select code,from_shop_name ,to_shop_name ,assign_type,dist_type ,apply_quantity,service_fee,status,create_time ,day_round,distance 
                FROM hmcdata.bi_dist_model_transfer_order_info 
                where create_time >='{date_start} 00:00:00' and create_time <='{date_end} 23:59:59' and business_unit_id=:business_unit_id {status_sql}
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
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)
        total_sql = f"""
                   select count(*)
                FROM hmcdata.bi_dist_model_transfer_order_info 
                where create_time >='{date_start} 00:00:00' and create_time <='{date_end} 23:59:59' and business_unit_id=:business_unit_id {status_sql}
                """
        ret_total = Scope['vertica'].execute(total_sql, {'business_unit_id': business_unit_id})
        total = ret_total.fetchone()[0]
        context['total'] = total
        context['data'] = data
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_transfer_info_shop_detail_table_in', methods=['GET'], endpoint='get_transfer_info_shop_detail_table_in')
@__token_wrapper
def get_transfer_info_shop_detail_table_in(context):
    '''
        获取调拨单店铺调入数据
        :param context:
        :return:
        '''
    try:
        business_unit_id = context['data']['businessUnitId']
        date_start = request.args.get('date_start', type=str)
        date_end = request.args.get('date_end', type=str)
        page = request.args.get('page', type=int)
        page_size = request.args.get('pageSize')
        sorter = request.args.get('sorter')
        order = request.args.get('order')
        province_ids = request.args.get('provinceIds', type=str)
        if province_ids is None or province_ids == '':
            province_ids_sql = """"""
        else:
            province_ids_sql = f"""and from_province_zone_id in ({province_ids}) """
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
        sql = f"""
                SELECT a.from_level_name,a.from_shop_code,a.from_shop_name,a.zone_name,a.province_name,a.apply_quantity,a.received_quantity,
            a.apply_quantity+a.received_quantity as total_transfer,a.cancel_quantity,
            ROUND(b.avg_day_round,2) AS avg_day_round from (
            SELECT from_dc_shop_id ,from_level_name,from_shop_code,from_shop_name,bz.zone_name,z.province_name ,
            sum(case when status='待发货' then apply_quantity when status ='待收货' then delivery_quantity else 0 end) as apply_quantity 
            ,sum(case when status='已收货' then received_quantity else 0 end) as received_quantity 
            ,sum(case when status='已取消' then apply_quantity else 0 end) as cancel_quantity
            FROM hmcdata.bi_dist_model_transfer_base b
            left join hmcdata.bi_province_zone z on b.from_province_zone_id =z.id
            left join hmcdata.bi_big_zone bz on z.zone_id =bz.id
            where create_time >='{date_start} 00:00:00' and create_time <='{date_end} 23:59:59' and b.business_unit_id=:business_unit_id {province_ids_sql}
            GROUP BY from_level_name,from_shop_code,from_shop_name,bz.zone_name,z.province_name,from_dc_shop_id
            )a
            left join (
            SELECT from_dc_shop_id ,avg(TIMESTAMPDIFF(SECOND ,create_time,completed_time))/86400 as avg_day_round FROM hmcdata.bi_dist_model_transfer_base 
            where status ='已收货' and completed_time is not null AND create_time >='{date_start} 00:00:00' and create_time <='{date_end} 23:59:59' and business_unit_id=:business_unit_id
            GROUP BY from_dc_shop_id
            )b on a.from_dc_shop_id =b.from_dc_shop_id 
            {sorter_sql} {order_sql} {offset};
             """
        ret = Scope['vertica'].execute(sql, {'business_unit_id': business_unit_id})
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
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)
        total_sql = f"""
                    SELECT COUNT(*) from(
                   SELECT 1 FROM hmcdata.bi_dist_model_transfer_base
                     where create_time >='{date_start} 00:00:00' and create_time <='{date_end} 23:59:59' and business_unit_id=:business_unit_id
                    GROUP BY from_dc_shop_id
                    )a
                """
        ret_total = Scope['vertica'].execute(total_sql, {'business_unit_id': business_unit_id})
        total = ret_total.fetchone()[0]
        context['total'] = total
        context['data'] = data
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@good.route('/get_transfer_info_shop_detail_table_out', methods=['GET'], endpoint='get_transfer_info_shop_detail_table_out')
@__token_wrapper
def get_transfer_info_shop_detail_table_out(context):
    '''
        获取调拨单店铺调出数据
        :param context:
        :return:
        '''
    try:
        business_unit_id = context['data']['businessUnitId']
        date_start = request.args.get('date_start', type=str)
        date_end = request.args.get('date_end', type=str)
        page = request.args.get('page', type=int)
        page_size = request.args.get('pageSize')
        sorter = request.args.get('sorter')
        order = request.args.get('order')
        province_ids = request.args.get('provinceIds', type=str)
        if province_ids is None or province_ids == '':
            province_ids_sql = """"""
            other_province_ids_sql=''
        else:
            province_ids_sql = f"""and from_province_zone_id in ({province_ids}) """
            other_province_ids_sql=f""" and z.id in ({province_ids}) """
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
        sql = f"""
            select a.to_level_name,a.to_shop_code,a.province_name,a.zone_name,a.to_shop_name,a.grabbing_orders,a.pool_orders,a.hq_orders,a.grabbing_orders+a.pool_orders+a.hq_orders as total_send,
            a.cancel_quantity,ifnull(b.qty_send,0) as qty_send FROM (
            select to_level_name,to_dc_shop_id,to_shop_code,to_shop_name,z.province_name,bz.zone_name, SUM(CASE WHEN assign_type=1 then CASE when status='待发货' THEN apply_quantity when status='待收货' THEN delivery_quantity when status='已收货' 
            then received_quantity else 0 end else 0 end) as grabbing_orders,
            SUM(CASE WHEN assign_type=0 then CASE when status='待发货' THEN apply_quantity when status='待收货' THEN delivery_quantity when status='已收货' 
            then received_quantity else 0 end else 0 end) as pool_orders,
            SUM(CASE WHEN assign_type=2 then CASE when status='待发货' THEN apply_quantity when status='待收货' THEN delivery_quantity when status='已收货' 
            then received_quantity else 0 end else 0 end) as hq_orders,
            sum(case when status='已取消' then apply_quantity else 0 end) as cancel_quantity
            FROM hmcdata.bi_dist_model_transfer_base b
            left join hmcdata.bi_province_zone z on b.to_province_zone_id =z.id
            left join hmcdata.bi_big_zone bz on z.zone_id=bz.id 
            where create_time >='{date_start} 00:00:00' and create_time <='{date_end} 23:59:59' and b.business_unit_id=:business_unit_id {province_ids_sql}
            group by to_level_name,to_dc_shop_id,to_shop_code,to_shop_name ,z.province_name,bz.zone_name
            )a left join(
            SELECT dc_shop_id ,SUM(qty_send) as qty_send FROM hmcdata.bi_channel_order_info i
            left join hmcdata.dc_shop ds on i.dc_shop_id=ds.id and ds.is_online=0
            left join hmcdata.bi_province_zone z on ds.bi_province_id=z.id
            where order_status <>3 and total_day>='{date_start} 00:00:00' and total_day <='{date_end} 23:59:59'  and ds.business_unit_id=:business_unit_id {other_province_ids_sql}
            GROUP BY dc_shop_id
            ) b on a.to_dc_shop_id=b.dc_shop_id
            {sorter_sql} {order_sql} {offset};
             """
        ret = Scope['vertica'].execute(sql, {'business_unit_id': business_unit_id})
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
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)
        total_sql = f"""
                    SELECT COUNT(*) from(
                   SELECT 1 FROM hmcdata.bi_dist_model_transfer_base
                     where create_time >='{date_start} 00:00:00' and create_time <='{date_end} 23:59:59' and business_unit_id=:business_unit_id
                    GROUP BY to_dc_shop_id
                    )a
                """
        ret_total = Scope['vertica'].execute(total_sql, {'business_unit_id': business_unit_id})
        total = ret_total.fetchone()[0]
        context['total'] = total
        context['data'] = data
        return jsonify(context)
    finally:
        Scope['vertica'].remove()



@good.route('/get_transfer_info_line', methods=['GET'], endpoint='get_transfer_info_line')
@__token_wrapper
def get_transfer_info_line(context):
    '''
        获取调拨单数据天数据
        :param context:
        :return:
        '''
    try:
        business_unit_id = context['data']['businessUnitId']
        date_start = request.args.get('date_start', type=str)
        date_end = request.args.get('date_end', type=str)
        assign_type = request.args.get('assignType', type=int)
        province_ids = request.args.get('provinceIds', type=str)
        if date_start is None or date_end is None:
            return jsonify(build_context(context, '时间不能为空', False, -1))
        if assign_type is None or assign_type == '' or (isinstance(assign_type, int) and int(assign_type) >= 3):
            assign_sql = """"""
        else:
            assign_sql = f"""and assign_type ={assign_type} """
        if province_ids is None or province_ids == '':
            province_ids_sql = """"""
        else:
            province_ids_sql = f"""and from_province_zone_id in ({province_ids}) """

        sql = f"""
                        SELECT COUNT(DISTINCT code) as 'order_count',SUM(actual_quantity) as actual_quantity,date(create_time) as total_day
                        FROM hmcdata.bi_dist_model_transfer_base where status in('待收货',
                        '已收货',
                        '待发货') and create_time >='{date_start} 00:00:00' and create_time <='{date_end} 23:59:59' 
                            and business_unit_id =:business_unit_id 
                        {assign_sql} {province_ids_sql}
                        group by total_day
                        order by total_day
                        """
        ret = Scope['vertica'].execute(sql, {'business_unit_id': business_unit_id})
        columns = ret.keys()
        data=[]
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