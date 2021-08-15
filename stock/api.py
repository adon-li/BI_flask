from flask import jsonify, Blueprint, request
from bi_flask.__token import __token_wrapper,__token_download
import logging
from bi_flask._sessions import sessions, sessions_scopes
from bi_flask.utils import *
from . import stock
logger = logging.getLogger('bi')
Scope = sessions_scopes(sessions)



@stock.route('/get_brand', methods=['GET'], endpoint='get_brand')
@__token_wrapper
def get_brand(context):
    try:
        business_unit_id = context['data']['businessUnitId']
        clazz_ids = context['data']['bi_business_clazz_id_list']
        brand_list = []
        clazz_ids = ','.join(clazz_ids)
        sql = f"""select distinct categoryClass from bi_business_brand_new where business_id in({clazz_ids}) and status=1 order by sort desc;"""
        ret = Scope['bi_saas'].execute(sql)
        # columns = ret.keys()
        data = []
        for categoryClass in ret:
            data.append(categoryClass[0])

        context['data'] = data
        return jsonify(context)

    finally:
        Scope['bi_saas'].remove()


@stock.route('/get_cat', methods=['GET'], endpoint='get_cat')
@__token_wrapper
def get_cat(context):
    try:

        sql = f"""select distinct ProductCategory2 from iom_scm_product_list where IsGift='否';"""
        ret = Scope['bi_saas'].execute(sql)
        # columns = ret.keys()
        data = []
        for cat in ret:
            data.append(cat[0])
        context['data'] = data
        return jsonify(context)

    finally:
        Scope['bi_saas'].remove()


@stock.route('/get_brand_stock_info', methods=['GET'], endpoint='get_brand_stock_info')
@__token_wrapper
def get_brand_stock_info(context):
    '''
    获取库存
    :param context:
    :return:
    '''
    try:
        brand_name = request.args.get('brand_name')
        #         sql=f"""SELECT s.doc_year as goods_year,SUM(s.plan_total) as planorder_nums,SUM(s.planorder_nums_On) as planorder_nums_On,
        # SUM(s.planorder_nums_Off) as planorder_nums_Off,sum(s.Produced_nums_On) as Produced_nums_On,
        # sum(s.Produced_nums_Off) as Produced_nums_Off,SUM(s.production_onway_nums) as production_onway_nums,
        # sum(s.production_onway_nums*ifnull(c.costamount,s.cost)) as production_onway_cost,sum(b.InStock_nums-b.InStock_numsO2O-b.InStock_nums_back) as InStock_nums_On,sum((b.InStock_nums-b.InStock_numsO2O-b.InStock_nums_O2Oback)*ifnull(c.costamount,s.cost)) as InStock_nums_On_cost
        # ,sum(b.InStock_nums_back-b.InStock_nums_O2Oback) as InStock_nums_back,sum((b.InStock_nums_back-b.InStock_nums_O2Oback)*ifnull(c.costamount,s.cost)) as InStock_nums_back_cost,
        # sum(b.offline_nums) as offline_nums,sum(b.offline_nums*ifnull(c.costamount,s.cost)) as offline_cost,
        # sum(b.onway_nums) as onway_nums,sum(b.onway_nums*ifnull(c.costamount,s.cost)) as onway_cost,sum(b.InStock_numsO2O) as InStock_numsO2O,sum(b.InStock_numsO2O*ifnull(c.costamount,s.cost)) as InStock_numsO2O_cost,sum(b.salesnums_all_On) as salesnums_all_On,sum(b.salesnums_all_On*ifnull(c.costamount,s.cost)) as salesnums_all_On_cost,sum(b.salesnums_all_Off) as salesnums_all_Off,sum(b.salesnums_all_Off*ifnull(c.costamount,s.cost)) as salesnums_all_Off_cost,sum(b.InStock_nums+b.offline_nums+b.onway_nums) as total_in_stock,
        # sum((InStock_nums+offline_nums+onway_nums)*ifnull(c.costamount,s.cost)) as total_in_stock_cost,sum(b.InStock_nums_O2Oback) as InStock_nums_O2Oback,
        # sum(b.InStock_nums_O2Oback*ifnull(c.costamount,s.cost)) as InStock_nums_O2Oback_cost,sum(s.production_onway_nums_on) as production_onway_nums_on,sum(s.production_onway_nums_on*ifnull(c.costamount,s.cost)) as production_onway_on_cost
        # ,sum(s.production_onway_nums_off) as production_onway_nums_off,sum(s.production_onway_nums_off*ifnull(c.costamount,s.cost)) as production_onway_off_cost
        # FROM hmcdata.bi_goods_stock_sub  s
        # LEFT JOIN hmcdata.bi_goods_stocks b ON s.product_sn=b.goods_bn AND s.doc_season=b.doc_season AND s.doc_year=b.doc_year AND s.color_name=b.goods_color
        # LEFT JOIN bi_goods_cost c
        # on s.product_sn=c.goods_bn
        # WHERE
        # s.category_class='{brand_name}' and s.is_gift='否'
        # GROUP BY s.doc_year,s.brand
        # ORDER BY s.doc_year desc"""
        sql = f"""SELECT doc_year as goods_year,SUM(Instock_nums_O2O) as InStock_numsO2O,SUM(Instock_nums_O2O*cost) as InStock_numsO2O_cost,
            SUM(InStock_nums_O2Oback) as InStock_nums_O2Oback,SUM(InStock_nums_O2Oback*cost) as InStock_nums_O2Oback_cost,
            SUM(Instock_nums_on) as InStock_nums_On,SUM(Instock_nums_on*cost) as InStock_nums_On_cost,
            SUM(InStock_nums_back) as InStock_nums_back,SUM(InStock_nums_back*cost) as InStock_nums_back_cost,
            SUM(Produced_nums_On) as Produced_nums_On,SUM(Produced_nums_Off) as Produced_nums_Off,
            SUM(Instock_nums_off) as offline_nums,SUM(Instock_nums_off*cost) as offline_cost,
            SUM(onway_nums) as onway_nums,SUM(onway_nums*cost) as onway_cost,
            SUM(plan_total) as planorder_nums,
            SUM(production_onway_nums_on) as production_onway_nums_on,SUM(production_onway_nums_on*cost) as production_onway_on_cost,
            SUM(production_onway_nums_off) as production_onway_nums_off,SUM(production_onway_nums_off*cost) as production_onway_off_cost,
            SUM(total_in_stock) as total_in_stock,SUM(total_in_stock*cost) as total_in_stock_cost,
            SUM(plan_total) as plan_total,SUM(planorder_nums_On) as planorder_nums_On,SUM(planorder_nums_Off) as planorder_nums_Off
            ,SUM(production_onway_nums) as production_onway_nums ,SUM(production_onway_nums*cost) as production_onway_cost
            ,SUM(online_stock) as online_stock,SUM(offline_stock) as offline_stock,SUM(production_onway_nums*cost) as production_onway_cost
            FROM bi_goods_stock_sub_new
            WHERE is_gift='否' 
            AND category_class='{brand_name}'
            GROUP BY doc_year
            ORDER BY doc_year desc"""

        ret = Scope['bi_saas'].execute(sql)
        columns = ret.keys()
        data = []
        for rank, val in enumerate(ret):
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
    finally:
        Scope['bi_saas'].remove()


@stock.route('/get_brand_stock_info_season', methods=['GET'], endpoint='get_brand_stock_info_season')
@__token_wrapper
def get_brand_stock_info_season(context):
    try:
        brand_name = request.args.get('brand_name')
        goods_year = request.args.get('goods_year')
        #         sql = f"""SELECT CASE s.doc_season WHEN 68 THEN '春季'
        #         WHEN 69 THEN '夏季' WHEN 70 THEN '秋季' WHEN 71 THEN '冬季' WHEN 72 THEN '全季' ELSE '其他' END AS  goods_season,s.doc_season as goods_season_id,SUM(s.plan_total) as planorder_nums,SUM(s.planorder_nums_On) as planorder_nums_On,
        #         SUM(s.planorder_nums_Off) as planorder_nums_Off,sum(s.Produced_nums_On) as Produced_nums_On,
        #         sum(s.Produced_nums_Off) as Produced_nums_Off,SUM(s.production_onway_nums) as production_onway_nums,
        #         sum(b.production_onway_nums*ifnull(c.costamount,s.cost)) as production_onway_cost,sum(b.InStock_nums-b.InStock_numsO2O-b.InStock_nums_back) as InStock_nums_On,sum((b.InStock_nums-b.InStock_numsO2O-b.InStock_nums_O2Oback)*ifnull(c.costamount,s.cost)) as InStock_nums_On_cost
        #         ,sum(b.InStock_nums_back-b.InStock_nums_O2Oback) as InStock_nums_back,sum((b.InStock_nums_back-b.InStock_nums_O2Oback)*ifnull(c.costamount,s.cost)) as InStock_nums_back_cost,
        #         sum(b.offline_nums) as offline_nums,sum(b.offline_nums*ifnull(c.costamount,s.cost)) as offline_cost,
        #         sum(b.onway_nums) as onway_nums,sum(b.onway_nums*ifnull(c.costamount,s.cost)) as onway_cost,sum(b.InStock_numsO2O) as InStock_numsO2O,sum(b.InStock_numsO2O*ifnull(c.costamount,s.cost)) as InStock_numsO2O_cost,sum(b.salesnums_all_On) as salesnums_all_On,sum(b.salesnums_all_On*ifnull(c.costamount,s.cost)) as salesnums_all_On_cost,sum(b.salesnums_all_Off) as salesnums_all_Off,sum(b.salesnums_all_Off*ifnull(c.costamount,s.cost)) as salesnums_all_Off_cost,sum(b.InStock_nums+b.offline_nums+b.onway_nums) as total_in_stock,
        #         sum((InStock_nums+offline_nums+onway_nums)*ifnull(c.costamount,s.cost)) as total_in_stock_cost,sum(b.InStock_nums_O2Oback) as InStock_nums_O2Oback,
        #         sum(b.InStock_nums_O2Oback*ifnull(c.costamount,s.cost)) as InStock_nums_O2Oback_cost,sum(s.production_onway_nums_on) as production_onway_nums_on,sum(s.production_onway_nums_on*ifnull(c.costamount,s.cost)) as production_onway_on_cost
        # ,sum(s.production_onway_nums_off) as production_onway_nums_off,sum(s.production_onway_nums_off*ifnull(c.costamount,s.cost)) as production_onway_off_cost
        #         FROM hmcdata.bi_goods_stock_sub  s
        #         LEFT JOIN hmcdata.bi_goods_stocks b ON s.product_sn=b.goods_bn AND s.doc_season=b.doc_season AND s.doc_year=b.doc_year AND s.color_name=b.goods_color
        #         LEFT JOIN bi_goods_cost c
        #         on s.product_sn=c.goods_bn
        #         WHERE
        #         s.doc_year={goods_year}  and s.is_gift='否'
        #         AND s.category_class='{brand_name}'
        #         GROUP BY s.doc_season,s.brand
        #         ORDER BY planorder_nums desc"""
        sql = f"""SELECT doc_season as goods_season,SUM(Instock_nums_O2O) as InStock_numsO2O,SUM(Instock_nums_O2O*cost) as InStock_numsO2O_cost,
SUM(InStock_nums_O2Oback) as InStock_nums_O2Oback,SUM(InStock_nums_O2Oback*cost) as InStock_nums_O2Oback_cost,
SUM(Instock_nums_on) as InStock_nums_On,SUM(Instock_nums_on*cost) as InStock_nums_On_cost,
SUM(InStock_nums_back) as InStock_nums_back,SUM(InStock_nums_back*cost) as InStock_nums_back_cost,
SUM(Produced_nums_On) as Produced_nums_On,SUM(Produced_nums_Off) as Produced_nums_Off,
SUM(Instock_nums_off) as offline_nums,SUM(Instock_nums_off*cost) as offline_cost,
SUM(onway_nums) as onway_nums,SUM(onway_nums*cost) as onway_cost,
SUM(plan_total) as planorder_nums,
SUM(production_onway_nums_on) as production_onway_nums_on,SUM(production_onway_nums_on*cost) as production_onway_on_cost,
SUM(production_onway_nums_off) as production_onway_nums_off,SUM(production_onway_nums_off*cost) as production_onway_off_cost,
SUM(total_in_stock) as total_in_stock,SUM(total_in_stock*cost) as total_in_stock_cost,
SUM(plan_total) as plan_total,SUM(planorder_nums_On) as planorder_nums_On,SUM(planorder_nums_Off) as planorder_nums_Off,
SUM(production_onway_nums) as production_onway_nums,SUM(online_stock) as online_stock,SUM(offline_stock) as offline_stock,SUM(production_onway_nums*cost) as production_onway_cost
FROM bi_goods_stock_sub_new
WHERE is_gift='否' 
AND category_class='{brand_name}' AND doc_year={goods_year}
GROUP BY doc_season
ORDER BY season_sorter asc"""

        ret = Scope['bi_saas'].execute(sql)
        columns = ret.keys()
        data = []
        for rank, val in enumerate(ret):
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
    finally:
        Scope['bi_saas'].remove()


@stock.route('/get_brand_goods_year', methods=['GET'], endpoint='get_brand_goods_year')
@__token_wrapper
def get_brand_goods_year(context):
    try:
        brand_name = request.args.get('brand_name')

        #         sql=f"""SELECT DISTINCT goods_year from bi_goods_stocks where goods_brand_name='{brand_name}' and goods_year is not null
        # ORDER BY goods_year desc"""
        sql = f"""SELECT DISTINCT doc_year from bi_goods_stock_sub where category_class='{brand_name}' and doc_year is not null
        ORDER BY doc_year desc"""
        ret = Scope['bi_saas'].execute(sql)
        columns = ret.keys()
        data = []
        for rank, val in enumerate(ret):
            for i, column in enumerate(columns):
                if isinstance(val[i], decimal.Decimal):
                    data.append(format_(val[i]))
                elif isinstance(val[i], datetime.datetime):
                    data.append(datetime_format(val[i]))
                elif isinstance(val[i], float):
                    data.append(format_(val[i]))
                elif isinstance(val[i], datetime.date):
                    data.append(datetime_format(val[i]))
                elif val[i] is None:
                    data.append(0)
                else:
                    data.append(val[i])

        context['data'] = data
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()


@stock.route('/get_brand_stock_info_cat', methods=['GET'], endpoint='get_brand_stock_info_cat')
@__token_wrapper
def get_brand_stock_info_cat(context):
    try:
        brand_name = request.args.get('brand_name')
        goods_year = request.args.get('goods_year')
        goods_season = request.args.get('goods_season')
        sql = f"""SELECT s.goods_catname_L2,SUM(s.plan_total) as planorder_nums,SUM(s.planorder_nums_On) as planorder_nums_On,
        SUM(s.planorder_nums_Off) as planorder_nums_Off,sum(s.Produced_nums_On) as Produced_nums_On,
        sum(s.Produced_nums_Off) as Produced_nums_Off,SUM(s.production_onway_nums) as production_onway_nums,
        sum(b.production_onway_nums*ifnull(c.costamount,s.cost)) as production_onway_cost,sum(b.InStock_nums-b.InStock_numsO2O-b.InStock_nums_back) as InStock_nums_On,sum((b.InStock_nums-b.InStock_numsO2O-b.InStock_nums_back)*ifnull(c.costamount,s.cost)) as InStock_nums_On_cost
        ,sum(b.InStock_nums_back-b.InStock_nums_O2Oback) as InStock_nums_back,sum((b.InStock_nums_back-b.InStock_nums_O2Oback)*ifnull(c.costamount,s.cost)) as InStock_nums_back_cost,
        sum(b.offline_nums) as offline_nums,sum(b.offline_nums*ifnull(c.costamount,s.cost)) as offline_cost,
        sum(b.onway_nums) as onway_nums,sum(b.onway_nums*ifnull(c.costamount,s.cost)) as onway_cost,sum(b.InStock_numsO2O) as InStock_numsO2O,sum(b.InStock_numsO2O*ifnull(c.costamount,s.cost)) as InStock_numsO2O_cost,sum(b.salesnums_all_On) as salesnums_all_On,sum(b.salesnums_all_On*ifnull(c.costamount,s.cost)) as salesnums_all_On_cost,sum(b.salesnums_all_Off) as salesnums_all_Off,sum(b.salesnums_all_Off*ifnull(c.costamount,s.cost)) as salesnums_all_Off_cost,sum(b.InStock_nums+b.offline_nums+b.onway_nums) as total_in_stock,
        sum((InStock_nums+offline_nums+onway_nums)*ifnull(c.costamount,s.cost)) as total_in_stock_cost,sum(b.InStock_nums_O2Oback) as InStock_nums_O2Oback,
sum(b.InStock_nums_O2Oback*ifnull(c.costamount,s.cost)) as InStock_nums_O2Oback_cost ,sum(s.production_onway_nums_on) as production_onway_nums_on,sum(s.production_onway_nums_on*ifnull(c.costamount,s.cost)) as production_onway_on_cost
,sum(s.production_onway_nums_off) as production_onway_nums_off,sum(s.production_onway_nums_off*ifnull(c.costamount,s.cost)) as production_onway_off_cost
        FROM hmcdata.bi_goods_stock_sub  s
        LEFT JOIN hmcdata.bi_goods_stocks b ON s.product_sn=b.goods_bn AND s.doc_season=b.doc_season AND s.doc_year=b.doc_year AND s.color_name=b.goods_color
        LEFT JOIN bi_goods_cost c
        on s.product_sn=c.goods_bn
        WHERE -- s.brand='茵曼' and 
        s.doc_year={goods_year}  and s.is_gift='否'
        AND s.category_class='{brand_name}' and s.doc_season='{goods_season}'
        GROUP BY s.brand,s.goods_catname_L2
        ORDER BY planorder_nums desc"""

        sql = f"""SELECT goods_catname_L2,SUM(Instock_nums_O2O) as InStock_numsO2O,SUM(Instock_nums_O2O*cost) as InStock_numsO2O_cost,
SUM(InStock_nums_O2Oback) as InStock_nums_O2Oback,SUM(InStock_nums_O2Oback*cost) as InStock_nums_O2Oback_cost,
SUM(Instock_nums_on) as InStock_nums_On,SUM(Instock_nums_on*cost) as InStock_nums_On_cost,
SUM(InStock_nums_back) as InStock_nums_back,SUM(InStock_nums_back*cost) as InStock_nums_back_cost,
SUM(Produced_nums_On) as Produced_nums_On,SUM(Produced_nums_Off) as Produced_nums_Off,
SUM(Instock_nums_off) as offline_nums,SUM(Instock_nums_off*cost) as offline_cost,
SUM(onway_nums) as onway_nums,SUM(onway_nums*cost) as onway_cost,
SUM(plan_total) as planorder_nums,
SUM(production_onway_nums_on) as production_onway_nums_on,SUM(production_onway_nums_on*cost) as production_onway_on_cost,
SUM(production_onway_nums_off) as production_onway_nums_off,SUM(production_onway_nums_off*cost) as production_onway_off_cost,
SUM(total_in_stock) as total_in_stock,SUM(total_in_stock*cost) as total_in_stock_cost,
SUM(plan_total) as plan_total,SUM(planorder_nums_On) as planorder_nums_On,SUM(planorder_nums_Off) as planorder_nums_Off,SUM(production_onway_nums) as production_onway_nums,SUM(production_onway_nums*cost) as production_onway_cost,SUM(online_stock) as online_stock,SUM(offline_stock) as offline_stock
FROM bi_goods_stock_sub_new
WHERE is_gift='否' 
AND category_class='{brand_name}' AND doc_year={goods_year} AND doc_season='{goods_season}'
GROUP BY doc_season,goods_catname_L2
ORDER BY total_in_stock desc"""

        ret = Scope['bi_saas'].execute(sql)
        columns = ret.keys()
        data = []
        for rank, val in enumerate(ret):
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
    finally:
        Scope['bi_saas'].remove()


@stock.route('/get_brand_goods_year_season', methods=['GET'], endpoint='get_brand_goods_year_season')
@__token_wrapper
def get_brand_goods_year_season(context):
    try:
        business_id = request.args.get('business_id')
        # online = request.args.get('online')
        is_offline_to_online = request.args.get('is_offline_to_online')
        sql = f"""SELECT CategoryClass,max(is_online) as is_online FROM  bi_business_brand_new WHERE business_id={business_id}"""
        ret = Scope['bi_saas'].execute(sql)
        online = ret.fetchone()[1]
        if is_offline_to_online is None or is_offline_to_online == "all":
            sql_sub = ""
        else:
            sql_sub = f""" and is_offline_to_online={int(is_offline_to_online)} """

        if str(online) == '1':
            sql = f"""SELECT doc_year as goods_year,doc_season as goods_season,SUM(online_stock) as stock,(SUM(produced_nums_On)-SUM(online_stock)+sum(out_of_stock))/(SUM(produced_nums_On)+SUM(production_onway_nums_on)) as sold_out_quantity,
            (SUM(produced_nums_On*cost)-SUM(online_stock*cost)+sum(out_of_stock*cost))/(SUM(produced_nums_On*cost)+SUM(production_onway_nums_on*cost)) as sold_out_cost,
            SUM(produced_nums_On) as produced_nums, sum(production_onway_nums_on) as production_onway_nums,SUM(online_stock*cost) as stock_cost,
            SUM(produced_nums_On*cost) as produced_cost,SUM(production_onway_nums_on*cost) as production_onway_nums_cost
            ,SUM(planorder_nums_On) as planorder_nums,SUM(planorder_nums_On*cost) as planorder_nums_cost,sum(out_of_stock) as out_of_stock
            FROM bi_goods_stock_sub_new
            WHERE  is_gift='否' AND division_online='线上' {sql_sub}
            AND category_class IN(
            SELECT CategoryClass FROM  bi_business_brand_new WHERE business_id={business_id}
            )
            GROUP BY doc_year,doc_season
            HAVING SUM(produced_nums_On)>0 OR SUM(production_onway_nums_on)>0
            ORDER BY sorter ASC"""
        else:
            sql = f"""SELECT doc_year as goods_year,doc_season as goods_season,SUM(offline_stock) as stock,(SUM(produced_nums_Off)-SUM(offline_stock))/(SUM(produced_nums_Off)+SUM(production_onway_nums_off)) as sold_out_quantity,
            (SUM(produced_nums_Off*cost)-SUM(offline_stock*cost)+sum(out_of_stock*cost))/(SUM(produced_nums_Off*cost)+SUM(production_onway_nums_off*cost)) as sold_out_cost,
            SUM(produced_nums_Off) as produced_nums, sum(production_onway_nums_off) as production_onway_nums,SUM(offline_stock*cost) as stock_cost,
            SUM(produced_nums_Off*cost) as produced_cost,SUM(production_onway_nums_off*cost) as production_onway_nums_cost
            ,SUM(planorder_nums_Off) as planorder_nums,SUM(planorder_nums_Off*cost) as planorder_nums_cost,sum(out_of_stock) as out_of_stock
              FROM bi_goods_stock_sub_new
            WHERE  is_gift='否' AND division_offline='线下'
            AND category_class IN(
            SELECT CategoryClass FROM  bi_business_brand_new WHERE business_id={business_id}
            )
            GROUP BY doc_year,doc_season
            HAVING SUM(produced_nums_Off)>0 OR SUM(production_onway_nums_off)>0
            ORDER BY sorter ASC"""

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


@stock.route('/get_brand_goods_year_season_detail', methods=['GET'], endpoint='get_brand_goods_year_season_detail')
@__token_wrapper
def get_brand_goods_year_season_detail(context):
    try:
        # brand_name = request.args.get('brand_name')
        business_id = request.args.get('business_id')
        # online = request.args.get('online')
        year = request.args.get('year')
        season = request.args.get('season')
        is_offline_to_online = request.args.get('is_offline_to_online')
        sql = f"""SELECT CategoryClass,max(is_online) as is_online FROM  bi_business_brand_new WHERE business_id={business_id}"""
        ret = Scope['bi_saas'].execute(sql)
        online = ret.fetchone()[1]
        if is_offline_to_online is None or is_offline_to_online == "all":
            sql_sub = ""
        else:
            sql_sub = f""" and is_offline_to_online={int(is_offline_to_online)}  """
        if str(online) == '1':
            sql = f"""SELECT COUNT(DISTINCT product_sn) as quantity,SUM(online_stock) as InStock_nums,SUM(online_stock*cost) as InStock_nums_cost,
            sum(planorder_nums_On) as planorder_nums,sum(Produced_nums_On) as Produced_nums,sum(production_onway_nums_on) as onway_nums,
            sum(planorder_nums_On*cost) as planorder_nums_cost,sum(Produced_nums_On*cost) as Produced_nums_cost,sum(production_onway_nums) as production_onway_nums,
            sum(production_onway_nums_on*cost) as production_onway_nums_cost,sum(out_of_stock) as out_of_stock
            FROM bi_goods_stock_sub_new
            WHERE doc_year={year} AND doc_season='{season}' AND is_gift='否' AND division_online='线上' {sql_sub}
            AND category_class IN(
            SELECT CategoryClass FROM  bi_business_brand_new WHERE business_id={business_id}
            )
            GROUP BY doc_year,doc_season"""
        else:
            sql = f"""SELECT COUNT(DISTINCT product_sn) as quantity,SUM(offline_stock) as InStock_nums,SUM(offline_stock*cost) as InStock_nums_cost,
            sum(planorder_nums_Off) as planorder_nums,sum(Produced_nums_Off) as Produced_nums,
            sum(planorder_nums_Off*cost) as planorder_nums_cost,sum(Produced_nums_Off*cost) as Produced_nums_cost,sum(production_onway_nums) as production_onway_nums,
            sum(production_onway_nums_off*cost) as production_onway_nums_cost,sum(out_of_stock) as out_of_stock
            FROM bi_goods_stock_sub_new
            WHERE doc_year={year} AND doc_season='{season}' AND is_gift='否' AND division_offline='线下'
            AND category_class IN(
            SELECT CategoryClass FROM  bi_business_brand_new WHERE business_id={business_id}
            )
            GROUP BY doc_year,doc_season"""

        ret = Scope['bi_saas'].execute(sql)
        columns = ret.keys()
        data = []
        for rank, val in enumerate(ret):
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
    finally:
        Scope['bi_saas'].remove()


# 供应链查看
@stock.route('/get_produce_info', methods=['GET'], endpoint='get_produce_info')
@__token_wrapper
def get_produce_info(context):
    try:
        brand_name = request.args.get('brand_name')
        doc_year = int(request.args.get('doc_year'))
        doc_season = request.args.get('doc_season')
        sql = f"""SELECT a.time,a.order_count,a.product_counts,a.PlanTotal,a.TranTotal,b.NotArriveQty FROM(
            SELECT 'now' as time,COUNT(DISTINCT ProduceOrderId) as order_count,count(DISTINCT product_sn) as product_counts,
            SUM(PlanTotal) as PlanTotal,SUM(TranTotal) as TranTotal,SUM(NotArriveQty) as NotArriveQty,CategoryShopType FROM hmcdata.iom_scm_produce_schedule_info WHERE 
            CategoryShopType='{brand_name}'
            AND doc_season='{doc_season}' AND doc_year='{doc_year}' AND Deleted=0 AND Status<>2
            GROUP BY CategoryShopType
            )a
            LEFT JOIN (
            SELECT  SUM(NotArriveQty) as NotArriveQty,CategoryShopType FROM hmcdata.iom_scm_produce_schedule_info WHERE CategoryShopType='{brand_name}'
            AND doc_season='{doc_season}' AND doc_year='{doc_year}' AND Deleted=0 AND Status=0
            GROUP BY CategoryShopType
            )b ON a.CategoryShopType=b.CategoryShopType
            
            UNION ALL

            SELECT a.time,a.order_count,a.product_counts,a.PlanTotal,a.TranTotal,b.NotArriveQty FROM(
            SELECT 'last' as time,COUNT(DISTINCT ProduceOrderId) as order_count,count(DISTINCT product_sn) as product_counts,
            SUM(PlanTotal) as PlanTotal,SUM(TranTotal) as TranTotal,SUM(NotArriveQty) as NotArriveQty,CategoryShopType FROM hmcdata.iom_scm_produce_schedule_info WHERE 
            CategoryShopType='{brand_name}'
            AND doc_season='{doc_season}' AND doc_year='{doc_year - 1}' AND Deleted=0 AND Status<>2
            GROUP BY CategoryShopType
            )a
            LEFT JOIN (
            SELECT  SUM(NotArriveQty) as NotArriveQty,CategoryShopType FROM hmcdata.iom_scm_produce_schedule_info WHERE CategoryShopType='{brand_name}'
            AND doc_season='{doc_season}' AND doc_year='{doc_year - 1}' AND Deleted=0 AND Status=0
            GROUP BY CategoryShopType
            )b ON a.CategoryShopType=b.CategoryShopType
            """

        # sql=f"""  SELECT 'now' as time,COUNT(DISTINCT ProduceOrderId) as order_count,count(DISTINCT product_sn) as product_counts,
        #     SUM(PlanTotal) as PlanTotal,SUM(TranTotal) as TranTotal,SUM(NotArriveQty) as NotArriveQty FROM hmcdata.iom_scm_produce_schedule_info WHERE
        #     CategoryShopType='{brand_name}'
        #     AND doc_season='{doc_season}' AND doc_year='{doc_year}' AND Deleted=0 AND Status<>2
        #     UNION ALL
        #     SELECT 'last' as time,COUNT(DISTINCT ProduceOrderId) as order_count,count(DISTINCT product_sn) as product_counts,
        #     SUM(PlanTotal) as PlanTotal,SUM(TranTotal) as TranTotal,SUM(NotArriveQty) as NotArriveQty FROM hmcdata.iom_scm_produce_schedule_info WHERE
        #     CategoryShopType='{brand_name}'
        #     AND doc_season='{doc_season}' AND doc_year='{doc_year-1}' AND Deleted=0 AND Status<>2
        #     """

        ret = Scope['vertica'].execute(sql)
        data = {'now': {}, 'last': {}}

        columns = ret.keys()
        # data = []
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
            if data_dict['time'] == 'now':
                data['now'] = data_dict
            else:
                data['last'] = data_dict
        context['data'] = data
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@stock.route('/get_iom_isOntime', methods=['GET'], endpoint='get_iom_isOntime')
@__token_wrapper
def get_iom_isOntime(context):
    try:
        brand_name = request.args.get('brand_name')
        doc_year = int(request.args.get('doc_year'))
        doc_season = request.args.get('doc_season')
        sql = f"""  SELECT IsOnTime,COUNT(DISTINCT ProduceOrderId) as order_counts FROM hmcdata.iom_scm_produce_schedule_info WHERE 
            CategoryShopType='{brand_name}'
            AND doc_season='{doc_season}' AND doc_year='{doc_year}' AND Deleted=0 AND Status<>2
            GROUP BY IsOnTime;
            """

        ret = Scope['vertica'].execute(sql)
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
    finally:
        Scope['vertica'].remove()


@stock.route('/get_iom_orderType', methods=['GET'], endpoint='get_iom_orderType')
@__token_wrapper
def get_iom_orderType(context):
    try:
        brand_name = request.args.get('brand_name')
        doc_year = int(request.args.get('doc_year'))
        doc_season = request.args.get('doc_season')
        sql = f"""SELECT CASE OrderType WHEN '首单' THEN '首单' ELSE '返单' END as order_type,COUNT(DISTINCT ProduceOrderId) as order_counts FROM hmcdata.iom_scm_produce_schedule_info WHERE 
            CategoryShopType='{brand_name}'
            AND doc_season='{doc_season}' AND doc_year='{doc_year}' AND Deleted=0 AND Status<>2
            GROUP BY order_type;
            """

        ret = Scope['vertica'].execute(sql)
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
    finally:
        Scope['vertica'].remove()


@stock.route('/get_material_info', methods=['GET'], endpoint='get_material_info')
@__token_wrapper
def get_material_info(context):
    try:
        doc_year = request.args.get('doc_year')
        doc_season = request.args.get('doc_season')
        page = request.args.get('page')
        page_size = request.args.get('pageSize')
        filter = request.args.get('filter')
        if page_size is None:
            page_size = 10

        offset = f''' LIMIT {page_size} offset {(int(page) - 1) * int(page_size)} '''
        if filter is None:
            doc_year = int(doc_year)
            sql = f"""
                SELECT m.ItemCode,SUM(IFNULL(HaveGreyClothQuantity,0)+ifnull(NotHaveGreyClothQuantity,0)) as stock,total.tran_total as tran_total,m.product_count as product_count
                ,SUM(m.tran_on) as tran_on,SUM(m.tran_off) as tran_off,total.sales_all as sales
                FROM hmcdata.bi_fabric_report_main m
                left JOIN hmcdata.bi_fabric_total total ON m.ItemCode=total.ItemCode
                WHERE OrderSeason='{doc_season}' AND OrderYear={doc_year} GROUP BY OrderSeason,OrderYear,m.ItemCode,total.tran_total,m.product_count,total.sales_all
                ORDER BY m.product_count DESC
                {offset}
                """
        else:
            sql = f"""
                SELECT m.ItemCode,SUM(IFNULL(HaveGreyClothQuantity,0)+ifnull(NotHaveGreyClothQuantity,0)) as stock,total.tran_total as tran_total,m.product_count as product_count
                ,SUM(m.tran_on) as tran_on,SUM(m.tran_off) as tran_off,total.sales_all as sales
                FROM hmcdata.bi_fabric_report_main m
                left JOIN hmcdata.bi_fabric_total total ON m.ItemCode=total.ItemCode
                WHERE upper(m.ItemCode) like upper('%{filter}%') GROUP BY OrderSeason,OrderYear,m.ItemCode,total.tran_total,m.product_count,total.sales_all
                ORDER BY m.product_count DESC
                {offset}
                """

        ret = Scope['vertica'].execute(sql)
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
    finally:
        Scope['vertica'].remove()


@stock.route('/getTotal', methods=['GET'], endpoint='getTotal')
@__token_wrapper
def getTotal(context):
    '''
    获取具体评论
    :param context:
    :return:
    '''
    try:
        doc_year = request.args.get('doc_year')
        doc_season = request.args.get('doc_season')
        page_size = request.args.get('pageSize')
        filter = request.args.get('filter')

        if page_size is None:
            page_size = 10

        if filter is None:
            doc_year = int(doc_year)
            sql_total = f'''SELECT COUNT(*) FROM(
            SELECT m.ItemCode,SUM(IFNULL(HaveGreyClothQuantity,0)+ifnull(NotHaveGreyClothQuantity,0)) as stock,total.tran_total as tran_total,m.product_count as product_count
            ,SUM(m.tran_on) as tran_on,SUM(m.tran_off) as tran_off,total.sales_all as sales
            FROM hmcdata.bi_fabric_report_main m
            left JOIN hmcdata.bi_fabric_total total ON m.ItemCode=total.ItemCode
            WHERE OrderSeason='{doc_season}' AND OrderYear={doc_year} GROUP BY OrderSeason,OrderYear,m.ItemCode,total.tran_total,m.product_count,total.sales_all
            ORDER BY total.tran_total DESC
            )a'''
        else:
            sql_total = f'''SELECT COUNT(*) FROM(
            SELECT m.ItemCode,SUM(IFNULL(HaveGreyClothQuantity,0)+ifnull(NotHaveGreyClothQuantity,0)) as stock,total.tran_total as tran_total,m.product_count as product_count
            ,SUM(m.tran_on) as tran_on,SUM(m.tran_off) as tran_off,total.sales_all as sales
            FROM hmcdata.bi_fabric_report_main m
            left JOIN hmcdata.bi_fabric_total total ON m.ItemCode=total.ItemCode
            WHERE upper(m.ItemCode) like upper('%{filter}%') GROUP BY OrderSeason,OrderYear,m.ItemCode,total.tran_total,m.product_count,total.sales_all
            ORDER BY total.tran_total DESC
            )a'''

        # print(sql_total)
        ret = Scope['vertica'].execute(sql_total)
        total_rows = ret.fetchone()[0]

        context['data'] = {'total_rows': total_rows}
        return jsonify(context)
    finally:
        # Scope['bi_saas'].remove()
        Scope['vertica'].remove()


@stock.route('/get_material_detail', methods=['GET'], endpoint='get_material_detail')
@__token_wrapper
def get_material_detail(context):
    try:
        doc_year = int(request.args.get('doc_year'))
        doc_season = request.args.get('doc_season')
        item_code = request.args.get('ItemCode')

        # sql=f"""
        #     SELECT ProductSN,ProductCategory2,SUM(tran_total) as tran_total,UsePosition,AVG(fabric_consumption) as fabric_consumption,Unit,max(i.img_src) as img_src FROM hmcdata.bi_fabric_report r
        #     LEFT JOIN hmcdata.bi_product_img i ON r.ProductSN=i.product_sn WHERE
        #     ItemCode='{item_code}' AND OrderSeason='{doc_season}' AND OrderYear={doc_year}
        #     GROUP BY ProductSN,ProductCategory2,UsePosition,Unit
        #     """
        sql = f""" SELECT a.ProductSN,a.ProductCategory2,a.tran_total,a.UsePosition,a.fabric_consumption,a.img_src,b.sales_num FROM (
            SELECT ProductSN,ProductCategory2,SUM(tran_total) as tran_total,UsePosition,AVG(fabric_consumption) as fabric_consumption,Unit,max(i.img_src) as img_src FROM hmcdata.bi_fabric_report r
            LEFT JOIN hmcdata.bi_product_img i ON r.ProductSN=i.product_sn
            WHERE
            ItemCode='{item_code}'  AND OrderSeason='{doc_season}' AND OrderYear={doc_year}
            GROUP BY ProductSN,ProductCategory2,UsePosition,Unit
            )a
            LEFT JOIN (SELECT SUM(sales_num) as sales_num,product_sn FROM hmcdata.e3_goods_sales GROUP BY product_sn
            )b ON a.ProductSN=b.product_sn"""

        ret = Scope['vertica'].execute(sql)
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
    finally:
        Scope['vertica'].remove()


@stock.route('/get_material_detail_on_off', methods=['GET'], endpoint='get_material_detail_on_off')
@__token_wrapper
def get_material_detail_on_off(context):
    try:
        doc_year = int(request.args.get('doc_year'))
        doc_season = request.args.get('doc_season')
        item_code = request.args.get('ItemCode')
        result = {'on': [], 'off': []}
        # sql=f"""
        #     SELECT ProductSN,ProductCategory2,SUM(tran_total) as tran_total,UsePosition,AVG(fabric_consumption) as fabric_consumption,Unit,max(i.img_src) as img_src FROM hmcdata.bi_fabric_report r
        #     LEFT JOIN hmcdata.bi_product_img i ON r.ProductSN=i.product_sn WHERE
        #     ItemCode='{item_code}' AND OrderSeason='{doc_season}' AND OrderYear={doc_year}
        #     GROUP BY ProductSN,ProductCategory2,UsePosition,Unit
        #     """
        sql_on = f""" SELECT a.ProductSN,a.ProductCategory2,a.tran_total,a.UsePosition,a.fabric_consumption,a.img_src,b.sales_num FROM (
            SELECT ProductSN,ProductCategory2,SUM(tran_on) as tran_total,UsePosition,AVG(fabric_consumption) as fabric_consumption,Unit,max(i.img_src) as img_src FROM hmcdata.bi_fabric_report r
            LEFT JOIN hmcdata.bi_product_img i ON r.ProductSN=i.product_sn
            WHERE
            ItemCode='{item_code}' AND OrderSeason='{doc_season}' AND OrderYear={doc_year}  AND tran_on>0
            GROUP BY ProductSN,ProductCategory2,UsePosition,Unit
            )a
            LEFT JOIN (SELECT SUM(sales_num) as sales_num,product_sn FROM hmcdata.e3_goods_sales GROUP BY product_sn
            )b ON a.ProductSN=b.product_sn"""
        ret = Scope['vertica'].execute(sql_on)
        columns = ret.keys()
        # data = []
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
            result["on"].append(data_dict)
        sql_off = f""" SELECT a.ProductSN,a.ProductCategory2,a.tran_total,a.UsePosition,a.fabric_consumption,a.img_src,b.sales_num FROM (
                   SELECT ProductSN,ProductCategory2,SUM(tran_off) as tran_total,UsePosition,AVG(fabric_consumption) as fabric_consumption,Unit,max(i.img_src) as img_src FROM hmcdata.bi_fabric_report r
                   LEFT JOIN hmcdata.bi_product_img i ON r.ProductSN=i.product_sn
                   WHERE
                   ItemCode='{item_code}' AND OrderSeason='{doc_season}' AND OrderYear={doc_year} AND tran_off>0
                   GROUP BY ProductSN,ProductCategory2,UsePosition,Unit
                   )a
                   LEFT JOIN (SELECT SUM(sales_num) as sales_num,product_sn FROM hmcdata.e3_goods_sales GROUP BY product_sn
                   )b ON a.ProductSN=b.product_sn"""
        ret = Scope['vertica'].execute(sql_off)
        columns = ret.keys()
        # data = []
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
            result["off"].append(data_dict)

        context['data'] = result
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@stock.route('/get_dateofdelivery', methods=['GET'], endpoint='get_dateofdelivery')
@__token_wrapper
def get_dateofdelivery(context):
    try:
        brand_name = request.args.get('brand_name')
        doc_year = int(request.args.get('doc_year'))
        doc_season = request.args.get('doc_season')
        channel = request.args.get('channel')
        # print(doc_season)
        if doc_season == '':
            context['data'] = []
            return jsonify(context)
        if channel == "all":
            channel_sql = ""
        elif channel == '线上':
            channel_sql = f"AND (Channel='线上' OR Channel is NULL) "
        else:
            channel_sql = f"AND Channel='{channel}'"

        # sql=f""" SELECT a.OrderType,a.ProduceDocNum,a.product_nums,a.nums as total_nums,b.nums as ontime_nums,c.nums as after_ontime_nums FROM(
        # SELECT OrderType,COUNT(*)as nums,COUNT(DISTINCT ProduceDocNum) AS ProduceDocNum,COUNT(DISTINCT product_sn) as product_nums FROM hmcdata.iom_scm_produce_schedule_info
        # WHERE doc_year={doc_year} AND doc_season IN({doc_season}) AND CategoryShopType='{brand_name}' AND Status<>2
        # {channel_sql}
        # GROUP BY OrderType
        # )a
        # LEFT JOIN
        # (
        # SELECT OrderType,COUNT(*) as nums FROM hmcdata.iom_scm_produce_schedule_info
        # WHERE doc_year={doc_year} AND doc_season IN({doc_season}) AND CategoryShopType='{brand_name}' AND Status<>2 AND IFNULL(IFNULL(DATE(Arrival90PercentDate),DATE(PurchaseOrderFinishDate)),DATE(NOW()))<=DATE(ContractArrivalScheduleDate)
        # {channel_sql}
        # GROUP BY OrderType
        # )b ON a.OrderType=b.OrderType
        # LEFT JOIN
        # (
        # SELECT OrderType,COUNT(*)as nums FROM hmcdata.iom_scm_produce_schedule_info
        # WHERE doc_year={doc_year} AND doc_season IN({doc_season}) AND CategoryShopType='{brand_name}' AND Status<>2 AND IFNULL(IFNULL(DATE(Arrival90PercentDate),DATE(PurchaseOrderFinishDate)),DATE(NOW()))<=DATE(ArrivalScheduleDate)
        # {channel_sql}
        # GROUP BY OrderType
        # )c ON a.OrderType=c.OrderType
        # ORDER BY a.nums DESC"""
        sql = f"""SELECT a.OrderType,a.ProduceDocNum,a.product_nums,a.nums as total_nums,b.nums as ontime_nums,c.nums as after_ontime_nums,a.PlanTotal,
        d.nums as last_total_nums,e.nums as last_ontime_nums,f.nums as last_after_ontime_nums,d.PlanTotal as last_PlanTotal
        FROM(
        SELECT OrderType,COUNT(*)as nums,COUNT(DISTINCT ProduceDocNum) AS ProduceDocNum,COUNT(DISTINCT product_sn) as product_nums,sum(PlanTotal) as PlanTotal FROM hmcdata.iom_scm_produce_schedule_info 
        WHERE doc_year={doc_year} AND doc_season IN({doc_season}) AND CategoryShopType='{brand_name}' AND Status<>2
        {channel_sql}
        GROUP BY OrderType
        )a 
        LEFT JOIN
        (
        SELECT OrderType,COUNT(*) as nums FROM hmcdata.iom_scm_produce_schedule_info 
        WHERE doc_year={doc_year} AND doc_season IN({doc_season}) AND CategoryShopType='{brand_name}' AND Status<>2 AND IFNULL(IFNULL(DATE(Arrival90PercentDate),DATE(PurchaseOrderFinishDate)),DATE(NOW()))<=DATE(ContractArrivalScheduleDate)
        {channel_sql}
        GROUP BY OrderType
        )b ON a.OrderType=b.OrderType
        LEFT JOIN 
        (
        SELECT OrderType,COUNT(*)as nums FROM hmcdata.iom_scm_produce_schedule_info 
        WHERE doc_year={doc_year} AND doc_season IN({doc_season}) AND CategoryShopType='{brand_name}' AND Status<>2 AND IFNULL(IFNULL(DATE(Arrival90PercentDate),DATE(PurchaseOrderFinishDate)),DATE(NOW()))<=DATE(ArrivalScheduleDate)
        {channel_sql}
        GROUP BY OrderType
        )c ON a.OrderType=c.OrderType        
        LEFT JOIN(
        SELECT OrderType,COUNT(*)as nums,COUNT(DISTINCT ProduceDocNum) AS ProduceDocNum,COUNT(DISTINCT product_sn) as product_nums,sum(PlanTotal) as PlanTotal FROM hmcdata.iom_scm_produce_schedule_info 
        WHERE doc_year={doc_year - 1} AND doc_season IN({doc_season}) AND CategoryShopType='{brand_name}' AND Status<>2
        {channel_sql}
        GROUP BY OrderType
        )d ON a.OrderType=d.OrderType
        LEFT JOIN
        (
        SELECT OrderType,COUNT(*) as nums FROM hmcdata.iom_scm_produce_schedule_info 
        WHERE doc_year={doc_year - 1} AND doc_season IN({doc_season}) AND CategoryShopType='{brand_name}' AND Status<>2 AND IFNULL(IFNULL(DATE(Arrival90PercentDate),DATE(PurchaseOrderFinishDate)),DATE(NOW()))<=DATE(ContractArrivalScheduleDate)
        {channel_sql}
        GROUP BY OrderType
        )e ON e.OrderType=a.OrderType
        LEFT JOIN 
        (
        SELECT OrderType,COUNT(*)as nums FROM hmcdata.iom_scm_produce_schedule_info 
        WHERE doc_year={doc_year - 1} AND doc_season IN({doc_season}) AND CategoryShopType='{brand_name}' AND Status<>2 AND IFNULL(IFNULL(DATE(Arrival90PercentDate),DATE(PurchaseOrderFinishDate)),DATE(NOW()))<=DATE(ArrivalScheduleDate)
        {channel_sql}
        GROUP BY OrderType
        )f ON a.OrderType=f.OrderType
        ORDER BY a.nums DESC"""

        ret = Scope['vertica'].execute(sql)
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
    finally:
        Scope['vertica'].remove()


@stock.route('/get_dateofdelivery_detail', methods=['GET'], endpoint='get_dateofdelivery_detail')
@__token_wrapper
def get_dateofdelivery_detail(context):
    try:
        brand_name = request.args.get('brand_name')
        doc_year = int(request.args.get('doc_year'))
        doc_season = request.args.get('doc_season')
        channel = request.args.get('channel')
        order_type = request.args.get('order_type')
        # print(doc_season)
        if doc_season == '':
            context['data'] = []
            return jsonify(context)
        if channel == "all":
            channel_sql = ""
        elif channel == '线上':
            channel_sql = f"AND (Channel='线上' OR Channel is NULL) "
        else:
            channel_sql = f"AND Channel='{channel}'"

        sql = f""" SELECT ProduceDocNum,i.img_src,s.product_sn,PlanTotal,TranTotal,ColorName,ArrivalScheduleDate,s.doc_season,
        IFNULL(IFNULL(DATE(Arrival90PercentDate),DATE(PurchaseOrderFinishDate)),DATE(NOW()))-DATE(ArrivalScheduleDate) as DelayDay,
        IFNULL(DATE(Arrival90PercentDate),DATE(PurchaseOrderFinishDate)) as last_day,DATE(CreatedOn) as CreatedOn FROM hmcdata.iom_scm_produce_schedule_info s
        LEFT JOIN hmcdata.bi_product_img i ON s.product_sn=i.product_sn AND i.relate_type=1
        WHERE doc_year={doc_year} AND doc_season IN({doc_season}) AND CategoryShopType='{brand_name}' AND Status<>2 AND OrderType='{order_type}'
        AND IFNULL(IFNULL(DATE(Arrival90PercentDate),DATE(PurchaseOrderFinishDate)),DATE(NOW()))>DATE(ArrivalScheduleDate)
        {channel_sql}
        ORDER BY ArrivalScheduleDate DESC
        """

        ret = Scope['vertica'].execute(sql)
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
                    data_dict[column] = ""
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)
        context['data'] = data
        return jsonify(context)
    finally:
        Scope['vertica'].remove()


@stock.route('/get_brand_goods_year_season_cost', methods=['GET'], endpoint='get_brand_goods_year_season_cost')
@__token_wrapper
def get_brand_goods_year_season_cost(context):
    try:
        business_id = request.args.get('business_id')
        # online = request.args.get('online')
        is_offline_to_online = request.args.get('is_offline_to_online')
        if is_offline_to_online is None or is_offline_to_online == "all":
            sql_sub = ""
        else:
            sql_sub = f""" and is_offline_to_online={int(is_offline_to_online)} """
        sql = f"""SELECT CategoryClass,max(is_online) as is_online FROM  bi_business_brand_new WHERE business_id={business_id}"""
        ret = Scope['bi_saas'].execute(sql)
        online = ret.fetchone()[1]
        if str(online) == '1':
            sql = f"""SELECT doc_year as goods_year,doc_season as goods_season,SUM(online_stock) as stock,SUM(online_stock*cost) as cost FROM bi_goods_stock_sub_new
            WHERE  is_gift='否' AND division_online='线上'
            AND category_class IN(
            SELECT CategoryClass FROM  bi_business_brand_new WHERE business_id={business_id} {sql_sub}
            )
            GROUP BY doc_year,doc_season
            HAVING SUM(produced_nums_On)>0 OR SUM(production_onway_nums_on)>0
            ORDER BY sorter ASC"""
        else:
            sql = f"""SELECT doc_year as goods_year,doc_season as goods_season,SUM(offline_stock) as stock,SUM(offline_stock*cost) as cost   FROM bi_goods_stock_sub_new
            WHERE  is_gift='否' AND division_offline='线下'
            AND category_class IN(
            SELECT CategoryClass FROM  bi_business_brand_new WHERE business_id={business_id}
            )
            GROUP BY doc_year,doc_season
            HAVING SUM(produced_nums_Off)>0 OR SUM(production_onway_nums_off)>0
            ORDER BY sorter ASC"""

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


@stock.route('/get_brand_goods_year_season_cat', methods=['GET'], endpoint='get_brand_goods_year_season_cat')
@__token_wrapper
def get_brand_goods_year_season_cat(context):
    '''
        获取类目库存数
        :param context:
        :return:
    '''
    try:
        business_id = request.args.get('business_id')
        # online = request.args.get('online')
        doc_season = request.args.get('doc_season')
        doc_year = request.args.get('doc_year')
        is_offline_to_online = request.args.get('is_offline_to_online')
        if is_offline_to_online is None or is_offline_to_online == "all":
            sql_sub_offline = ""
        else:
            sql_sub_offline = f""" and is_offline_to_online={int(is_offline_to_online)} """
        if doc_year is not None and doc_season is not None:
            sql_sub = f"""AND doc_year={doc_year} and doc_season='{doc_season}' """
            sql_group = f""",doc_year,doc_season"""
        else:
            sql_sub = ""
            sql_group = ""
        sql = f"""SELECT CategoryClass,max(is_online) as is_online FROM  bi_business_brand_new WHERE business_id={business_id}"""
        ret = Scope['bi_saas'].execute(sql)
        online = ret.fetchone()[1]
        if str(online) == '1':
            sql = f"""SELECT goods_catname_L2,SUM(online_stock) as stock,SUM(online_stock*cost) as cost {sql_group} FROM bi_goods_stock_sub_new
                    WHERE  is_gift='否' AND division_online='线上' {sql_sub} {sql_sub_offline}
                    AND category_class IN(
                    SELECT CategoryClass FROM  bi_business_brand_new WHERE business_id={business_id}
                    )
                    GROUP BY goods_catname_L2 {sql_group}
                    HAVING SUM(produced_nums_On)>0 OR SUM(production_onway_nums_on)>0 
                    ORDER BY SUM(online_stock) DESC"""
        else:
            sql = f"""SELECT goods_catname_L2,SUM(offline_stock) as stock,SUM(offline_stock*cost) as cost {sql_group} FROM bi_goods_stock_sub_new
                    WHERE  is_gift='否' AND division_offline='线下'  {sql_sub}
                    AND category_class IN(
                    SELECT CategoryClass FROM  bi_business_brand_new WHERE business_id={business_id}
                    )
                    GROUP BY goods_catname_L2 {sql_group}
                    HAVING SUM(produced_nums_Off)>0 OR SUM(production_onway_nums_off)>0 
                    ORDER BY SUM(offline_stock) DESC"""

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


@stock.route('/get_brand_goods_year_season_cat_detail', methods=['GET'],
             endpoint='get_brand_goods_year_season_cat_detail')
@__token_wrapper
def get_brand_goods_year_season_cat_detail(context):
    '''
        获取类目售罄率
        :param context:
        :return:
        '''
    try:
        brand_name = request.args.get('brand_name')
        business_id = request.args.get('business_id')
        # online = request.args.get('online')
        doc_season = request.args.get('doc_season')
        doc_year = request.args.get('doc_year')
        sql = f"""SELECT CategoryClass,max(is_online) as is_online FROM  bi_business_brand_new WHERE business_id={business_id}"""
        ret = Scope['bi_saas'].execute(sql)
        online = ret.fetchone()[1]
        if doc_year is not None and doc_season is not None:
            sql_sub = f"""AND doc_year={doc_year} and doc_season='{doc_season}' """
        else:
            sql_sub = ""
        is_offline_to_online = request.args.get('is_offline_to_online')
        if is_offline_to_online is None or is_offline_to_online == "all":
            sql_sub_offline = ""
        else:
            sql_sub_offline = f""" and is_offline_to_online={int(is_offline_to_online)} """

        # print(online==1)
        if str(online) == '1':
            sql = f"""SELECT doc_year,doc_season,goods_catname_L2,SUM(online_stock) as stock,
            (SUM(produced_nums_On)-SUM(online_stock)+sum(out_of_stock))/(SUM(produced_nums_On)+SUM(production_onway_nums_on)) as sold_out_quantity,
            (SUM(produced_nums_On*cost)-SUM(online_stock*cost)+sum(out_of_stock*cost))/(SUM(produced_nums_On*cost)+SUM(production_onway_nums_on*cost)) as sold_out_cost,
            SUM(produced_nums_On) as produced_nums, sum(production_onway_nums_on) as production_onway_nums,SUM(online_stock*cost) as stock_cost,
            SUM(produced_nums_On*cost) as produced_cost,SUM(production_onway_nums_on*cost) as production_onway_nums_cost
            ,SUM(planorder_nums_On) as planorder_nums,SUM(planorder_nums_On*cost) as planorder_nums_cost,sum(out_of_stock) as out_of_stock
            FROM bi_goods_stock_sub_new
                WHERE  is_gift='否' AND division_online='线上'  {sql_sub} {sql_sub_offline}
                AND category_class IN(
                SELECT CategoryClass FROM  bi_business_brand_new WHERE business_id={business_id}
                )
                GROUP BY doc_year,doc_season,goods_catname_L2
                HAVING SUM(produced_nums_On)>0 OR SUM(production_onway_nums_on)>0
                ORDER BY stock DESC"""
        else:
            sql = f"""SELECT doc_year,doc_season,goods_catname_L2,SUM(offline_stock) as stock,
            (SUM(produced_nums_Off)-SUM(offline_stock)+sum(out_of_stock))/(SUM(produced_nums_Off)+SUM(production_onway_nums_off)) as sold_out_quantity,
            (SUM(produced_nums_Off*cost)-SUM(offline_stock*cost)+sum(out_of_stock*cost))/(SUM(produced_nums_Off*cost)+SUM(production_onway_nums_off*cost)) as sold_out_cost,  
            SUM(produced_nums_Off) as produced_nums, sum(production_onway_nums_off) as production_onway_nums,SUM(offline_stock*cost) as stock_cost,
            SUM(produced_nums_Off*cost) as produced_cost,SUM(production_onway_nums_off*cost) as production_onway_nums_cost
            ,SUM(planorder_nums_Off) as planorder_nums,SUM(planorder_nums_Off*cost) as planorder_nums_cost,sum(out_of_stock) as out_of_stock
            FROM bi_goods_stock_sub_new
            WHERE  is_gift='否' AND division_offline='线下' {sql_sub}
            AND category_class IN(
            SELECT CategoryClass FROM  bi_business_brand_new WHERE business_id={business_id}
            )
            GROUP BY doc_year,doc_season,goods_catname_L2
            HAVING SUM(produced_nums_Off)>0 OR SUM(production_onway_nums_off)>0
            ORDER BY stock DESC"""

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


@stock.route('/get_brand_goods_cat_year', methods=['GET'], endpoint='get_brand_goods_cat_year')
@__token_wrapper
def get_brand_goods_cat_year(context):
    '''
        获取类目年份数
        :param context:
        :return:
    '''
    try:
        business_id = request.args.get('business_id')
        # online = request.args.get('online')
        sql = f"""SELECT CategoryClass,max(is_online) as is_online FROM  bi_business_brand_new WHERE business_id={business_id}"""
        ret = Scope['bi_saas'].execute(sql)
        online = ret.fetchone()[1]
        cat = request.args.get('cat')
        is_offline_to_online = request.args.get('is_offline_to_online')
        if is_offline_to_online is None or is_offline_to_online == "all":
            sql_sub_offline = ""
        else:
            sql_sub_offline = f""" and is_offline_to_online={int(is_offline_to_online)} """

        if str(online) == '1':
            sql = f"""SELECT doc_year,SUM(online_stock) as stock,SUM(online_stock*cost) as cost FROM bi_goods_stock_sub_new
                    WHERE  is_gift='否' AND division_online='线上' AND online_stock>0 AND goods_catname_L2='{cat}' {sql_sub_offline}
                    AND category_class IN(
                    SELECT CategoryClass FROM  bi_business_brand_new WHERE business_id={business_id}
                    )
                    GROUP BY doc_year
                    HAVING SUM(produced_nums_On)>0 OR SUM(production_onway_nums_on)>0 
                    ORDER BY doc_year DESC
                    """
        else:
            sql = f"""SELECT doc_year,SUM(offline_stock) as stock,SUM(offline_stock*cost) as cost FROM bi_goods_stock_sub_new
                    WHERE  is_gift='否' AND division_offline='线下' AND offline_stock>0  AND goods_catname_L2='{cat}'
                    AND category_class IN(
                    SELECT CategoryClass FROM  bi_business_brand_new WHERE business_id={business_id}
                    )
                    GROUP BY doc_year
                    HAVING SUM(produced_nums_Off)>0 OR SUM(production_onway_nums_off)>0 
                    ORDER BY doc_year DESC"""

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


@stock.route('/get_brand_goods_sold_out_product_sn', methods=['GET'], endpoint='get_brand_goods_sold_out_product_sn')
@__token_wrapper
def get_brand_goods_sold_out_product_sn(context):
    '''
        获取商品售罄率
        :param context:
        :return:
        '''
    try:
        business_id = request.args.get('business_id')
        # online = request.args.get('online')
        doc_season = request.args.get('doc_season')
        doc_year = request.args.get('doc_year')
        cat = request.args.get('cat')
        is_offline_to_online = request.args.get('is_offline_to_online')
        sql = f"""SELECT CategoryClass,max(is_online) as is_online FROM  bi_business_brand_new WHERE business_id={business_id}"""
        ret = Scope['bi_saas'].execute(sql)
        online = ret.fetchone()[1]
        if is_offline_to_online is None or is_offline_to_online == "all":
            sql_sub = ""
        else:
            sql_sub = f""" and is_offline_to_online={int(is_offline_to_online)} """
        if str(online) == '1':
            sql = f"""SELECT n.product_sn,max(i.img_src) as img_src,SUM(production_onway_nums_on) as onway,
            SUM((IFNULL(online_stock,0)+IFNULL(production_onway_nums_on,0))*cost) as total_cost,SUM(online_stock) as stock,
            (SUM(produced_nums_On)-SUM(online_stock)+sum(out_of_stock))/(SUM(produced_nums_On)+SUM(production_onway_nums_on)) as sold_out_quantity,
            (SUM(produced_nums_On*cost)-SUM(online_stock*cost)+sum(out_of_stock*cost))/(SUM(produced_nums_On*cost)+SUM(production_onway_nums_on*cost)) as sold_out_cost ,
             SUM(produced_nums_On) as produced_nums,SUM(online_stock*cost) as stock_cost,
            SUM(produced_nums_On*cost) as produced_cost,SUM(production_onway_nums_on*cost) as production_onway_nums_cost
            ,SUM(planorder_nums_On) as planorder_nums,SUM(planorder_nums_On*cost) as planorder_nums_cost,sum(out_of_stock) as out_of_stock
             FROM bi_goods_stock_sub_new n
                    LEFT JOIN bi_product_img i ON n.product_sn=i.product_sn
                    WHERE  is_gift='否' AND division_online='线上'  AND doc_year={doc_year} and doc_season='{doc_season}'  {sql_sub}
                    AND goods_catname_L2='{cat}'
                    AND category_class IN(
                    SELECT CategoryClass FROM  bi_business_brand_new WHERE business_id={business_id}
                    )
                    GROUP BY doc_year,doc_season,goods_catname_L2,product_sn
                    HAVING SUM(produced_nums_On)>0 OR SUM(production_onway_nums_on)>0
                    ORDER BY stock DESC"""
        else:
            sql = f"""SELECT n.product_sn,max(i.img_src) as img_src,SUM(production_onway_nums_off) as onway,
            SUM((IFNULL(offline_stock,0)+IFNULL(production_onway_nums_off,0))*cost) as total_cost,SUM(offline_stock) as stock,
            (SUM(produced_nums_Off)-SUM(offline_stock)+sum(out_of_stock))/(SUM(produced_nums_Off)+SUM(production_onway_nums_off)) as sold_out_quantity,
            (SUM(produced_nums_Off*cost)-SUM(offline_stock*cost)+sum(out_of_stock*cost))/(SUM(produced_nums_Off*cost)+SUM(production_onway_nums_off*cost)) as sold_out_cost, 
              SUM(produced_nums_Off) as produced_nums,SUM(offline_stock*cost) as stock_cost,
            SUM(produced_nums_Off*cost) as produced_cost,SUM(production_onway_nums_off*cost) as production_onway_nums_cost
            ,SUM(planorder_nums_Off) as planorder_nums,SUM(planorder_nums_Off*cost) as planorder_nums_cost,sum(out_of_stock) as out_of_stock
             FROM bi_goods_stock_sub_new n
                    LEFT JOIN bi_product_img i ON n.product_sn=i.product_sn
                    WHERE  is_gift='否' AND division_offline='线下' AND doc_year={doc_year} and doc_season='{doc_season}' 
                    AND goods_catname_L2='{cat}'
                    AND category_class IN(
                    SELECT CategoryClass FROM  bi_business_brand_new WHERE business_id={business_id}
                    )
                    GROUP BY doc_year,doc_season,goods_catname_L2,product_sn
                    HAVING SUM(produced_nums_Off)>0 OR SUM(production_onway_nums_off)>0
                    ORDER BY stock DESC"""

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


@stock.route('/export_brand_goods_sold_out', methods=['GET'], endpoint='export_brand_goods_sold_out')
@__token_download
def export_brand_goods_sold_out(context):
    '''
        导出商品售罄率
        :param context:
        :return:
        '''
    try:
        business_id = request.args.get('business_id')
        # online = request.args.get('online')

        sql = f"""SELECT CategoryClass,max(is_online) as is_online FROM  bi_business_brand_new WHERE business_id={business_id}"""
        ret = Scope['bi_saas'].execute(sql)
        online = ret.fetchone()[1]

        if str(online) == '1':
            sql = f"""
            SELECT product_sn,doc_year,doc_season,goods_catname_L2 as '类目',SUM(production_onway_nums_on) as '在途数',
            SUM((IFNULL(online_stock,0)+IFNULL(production_onway_nums_on,0))*cost) as '总成本',SUM(online_stock) as '在库库存',
            (SUM(produced_nums_On)-SUM(online_stock)+sum(out_of_stock))/(SUM(produced_nums_On)+SUM(production_onway_nums_on)) as '售罄率',
            (SUM(produced_nums_On*cost)-SUM(online_stock*cost)+sum(out_of_stock*cost))/(SUM(produced_nums_On*cost)+SUM(production_onway_nums_on*cost)) as '成本售罄率' ,
            SUM(produced_nums_On) as '入库数',SUM(online_stock*cost) as '在库成本',
            SUM(produced_nums_On*cost) as '入库成本',SUM(production_onway_nums_on*cost) as '在途成本'
            ,SUM(planorder_nums_On) as '计划数',SUM(planorder_nums_On*cost) as '计划成本',sum(out_of_stock) as '缺货数'
            FROM bi_goods_stock_sub_new n
            WHERE  is_gift='否' AND division_online='线上' 
            AND category_class IN(
                    SELECT CategoryClass FROM  bi_business_brand_new WHERE business_id={business_id}
            )
            GROUP BY doc_year,doc_season,goods_catname_L2,product_sn
            HAVING SUM(produced_nums_On)>0 OR SUM(production_onway_nums_on)>0
            
           """
        else:
            sql = f"""
            SELECT product_sn,doc_year,doc_season,goods_catname_L2 as '类目',SUM(offline_stock) as '在库库存',
            (SUM(produced_nums_Off)-SUM(offline_stock))/(SUM(produced_nums_Off)+SUM(production_onway_nums_off)) as '售罄率',
            (SUM(produced_nums_Off*cost)-SUM(offline_stock*cost))/(SUM(produced_nums_Off*cost)+SUM(production_onway_nums_off*cost)) as '成本售罄率', 
            SUM(produced_nums_Off) as '入库数', sum(production_onway_nums_off) as '在途数',SUM(offline_stock*cost) as '在库成本',
            SUM(produced_nums_Off*cost) as '入库成本',SUM(production_onway_nums_off*cost) as '在途成本'
            ,SUM(planorder_nums_Off) as '下单数',SUM(planorder_nums_Off*cost) as '下单成本'
            FROM bi_goods_stock_sub_new
            WHERE  is_gift='否' AND division_offline='线下' 
             AND category_class IN(
                    SELECT CategoryClass FROM  bi_business_brand_new WHERE business_id={business_id}
                    )
            GROUP BY doc_year,doc_season,goods_catname_L2,product_sn
            HAVING SUM(produced_nums_Off)>0 OR SUM(production_onway_nums_off)>0
            """

        ret = Scope['bi_saas'].execute(sql)
        columns = ret.keys()
        return export_file(columns, ret, f"""业务线售罄率明细{datetime.datetime.now().strftime('%Y-%m-%d %H_%M_%S')}""")
    finally:
        Scope['bi_saas'].remove()


@stock.route('/getCategory2', methods=['GET'], endpoint='getCategory2')
@__token_wrapper
def getCategory2(context):
    try:
        CategoryClass = request.args.get('CategoryClass')
        sql = f"""select distinct ProductCategory2 from iom_scm_product_list where CategoryClass='{CategoryClass}';"""
        ret = Scope['bi_saas'].execute(sql)
        # columns = ret.keys()
        data = []
        for cat in ret:
            data.append(cat[0])
        context['data'] = data
        return jsonify(context)

    finally:
        Scope['bi_saas'].remove()


@stock.route('/getTotalStock', methods=['GET'], endpoint='getTotalStock')
@__token_wrapper
def getTotalStock(context):
    try:
        product_sn = request.args.get('product_sn')
        CategoryClass = request.args.get('CategoryClass')
        if product_sn == "" or product_sn is None:
            product_sql = ""
        else:
            product_sql = f"""AND product_sn='{product_sn}' """
        sql = f"""SELECT SUM(online_zongcang_stock) as online_zongcang_stock,SUM(online_channel_stock) as online_channel_stock,
            SUM(online_return_stock) as online_return_stock,SUM(production_onway_nums_on) as production_onway_nums_on,
            SUM(o2o_stock_in_shop) as o2o_stock_in_shop,SUM(o2o_stock_toshop_onway) as o2o_stock_toshop_onway,
            SUM(o2o_return_onway) as o2o_return_onway,SUM(production_onway_nums_off) as production_onway_nums_off,
            SUM(o2o_stock_in_warehouse) as o2o_stock_in_warehouse,SUM(o2o_return_stock) as o2o_return_stock
            FROM bi_goods_stock_day_report
            WHERE CategoryClass='{CategoryClass}' AND is_gift='否' {product_sql};"""
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
        context['data'] = data[0]
        return jsonify(context)

    finally:
        Scope['bi_saas'].remove()


@stock.route('/getStockTotalRows', methods=['GET'], endpoint='getStockTotalRows')
@__token_wrapper
def getStockTotalRows(context):
    try:
        CategoryClass = request.args.get('CategoryClass')
        channel = request.args.get('channel')
        season = request.args.get('season')
        cat = request.args.get('cat')
        product_sn = request.args.get('product_sn')
        product_year = request.args.get('product_year')

        if product_sn == "" or product_sn is None:
            product_sql = ""
        else:
            product_sql = f"""AND product_sn='{product_sn}' """

        if channel == "1":
            sql_sub = """online_zongcang_stock"""
        elif channel == "2":
            sql_sub = """online_channel_stock"""
        elif channel == "3":
            sql_sub = """online_return_stock"""
        elif channel == "4":
            sql_sub = """production_onway_nums_on"""
        elif channel == "5":
            sql_sub = """o2o_stock_in_shop"""
        elif channel == "6":
            sql_sub = """o2o_stock_toshop_onway"""
        elif channel == "7":
            sql_sub = """o2o_return_onway"""
        elif channel == "8":
            sql_sub = """production_onway_nums_off"""
        elif channel == "9":
            sql_sub = """o2o_stock_in_warehouse"""
        elif channel == "10":
            sql_sub = """o2o_return_stock"""
        if season == 'all' or season is None:
            sesaon_sql = ""
        else:
            sesaon_sql = f"""AND product_season='{season}' """
        if cat == 'all' or cat is None:
            cat_sql = ""
        else:
            cat_sql = f"""AND product_category2='{cat}' """

        if product_year == 'all' or cat is None:
            product_year_sql = ""
        else:
            product_year_sql = f"""AND product_year='{product_year}' """

        sql = f"""SELECT COUNT(*) as total_rows,SUM({sql_sub}) as total_stock,
        SUM({sql_sub}*IFNULL(cost,iom_cost)) as total_stock_cost FROM bi_goods_stock_day_report 
        WHERE CategoryClass='{CategoryClass}' AND is_gift='否' AND
                {sql_sub}>0 {sesaon_sql} {cat_sql} {product_sql} {product_year_sql};"""
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
        context['data'] = data[0]
        return jsonify(context)

    finally:
        Scope['bi_saas'].remove()


@stock.route('/getStockDetail', methods=['GET'], endpoint='getStockDetail')
@__token_wrapper
def getStockDetail(context):
    try:
        page = request.args.get('page')
        page_size = request.args.get('pageSize')
        season = request.args.get('season')
        cat = request.args.get('cat')
        product_year = request.args.get('product_year')
        product_sn = request.args.get('product_sn')
        if product_sn == "" or product_sn is None:
            product_sql = ""
        else:
            product_sql = f"""AND r.product_sn='{product_sn}' """
        if page_size is None:
            page_size = 10
        offset = f''' LIMIT {int(page_size)} OFFSET {(int(page) - 1) * int(page_size)} '''
        CategoryClass = request.args.get('CategoryClass')
        channel = request.args.get('channel')
        if channel == "1":
            sql_sub = """online_zongcang_stock"""
        elif channel == "2":
            sql_sub = """online_channel_stock"""
        elif channel == "3":
            sql_sub = """online_return_stock"""
        elif channel == "4":
            sql_sub = """production_onway_nums_on"""
        elif channel == "5":
            sql_sub = """o2o_stock_in_shop"""
        elif channel == "6":
            sql_sub = """o2o_stock_toshop_onway"""
        elif channel == "7":
            sql_sub = """o2o_return_onway"""
        elif channel == "8":
            sql_sub = """production_onway_nums_off"""
        elif channel == "9":
            sql_sub = """o2o_stock_in_warehouse"""
        elif channel == "10":
            sql_sub = """o2o_return_stock"""
        if season == 'all' or season is None:
            sesaon_sql = ""
        else:
            sesaon_sql = f"""AND product_season='{season}' """
        if cat == 'all' or cat is None:
            cat_sql = ""
        else:
            cat_sql = f"""AND product_category2='{cat}' """

        if product_year == 'all' or cat is None:
            product_year_sql = ""
        else:
            product_year_sql = f"""AND r.product_year={product_year} """

        sql = f"""SELECT i.img_src,r.product_sn,r.product_category2,r.color,r.product_year,r.product_season,r.dev_prop,r.doc_year,r.doc_season,
        {sql_sub} as stock,{sql_sub}*ifnull(cost,iom_cost) as cost FROM bi_goods_stock_day_report r LEFT JOIN bi_product_img i ON i.product_sn=r.product_sn 
        WHERE CategoryClass='{CategoryClass}' AND is_gift='否' AND
        {sql_sub}>0 {sesaon_sql} {cat_sql} {product_sql}  {product_year_sql}
        ORDER BY {sql_sub} DESC {offset};"""

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


@stock.route('/fallInPrice', methods=['GET'], endpoint='fallInPrice')
@__token_wrapper
def fallInPrice(context):
    try:
        # product_sn = request.args.get('product_sn')
        business_id = request.args.get('business_id')
        sql = f"""SELECT CategoryClass,max(is_online) as is_online FROM  bi_business_brand_new WHERE business_id={business_id}"""
        ret = Scope['bi_saas'].execute(sql)
        online = ret.fetchone()[1]
        if str(online) == '1':
            divsql = f"""AND division='线上' """
        else:
            divsql = f"""AND division='线下' """
        sql = f"""SELECT inventory_age,stock_year,stock_season,down_price_rate,SUM(stock_nums) as stock_nums,
            SUM(stock_nums*cost) as stock_cost,SUM((stock_nums*cost)*down_price_rate) as down_price,first_down_price_day,
            first_down_price_rate,second_down_price_day,second_down_price_rate,third_down_price_day,third_down_price_rate,season_sorter 
            FROM hmcdata.bi_finance_stock_downprice WHERE CategoryClass IN
            (
            SELECT CategoryClass FROM  bi_business_brand_new WHERE business_id={business_id}
            ) {divsql}
            GROUP BY inventory_age,stock_year,stock_season,season_sorter,down_price_rate,first_down_price_day,first_down_price_rate,second_down_price_day,second_down_price_rate,third_down_price_day,third_down_price_rate
            ORDER BY down_price_rate ASC,stock_year DESC,season_sorter ASC;"""

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
                # elif val[i] is None:
                #     data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)
        context['data'] = data
        return jsonify(context)

    finally:
        Scope['bi_saas'].remove()


@stock.route('/get_fallInPrice_cat', methods=['GET'], endpoint='get_fallInPrice_cat')
@__token_wrapper
def get_fallInPrice_cat(context):
    try:
        stock_year = request.args.get('stock_year')
        stock_season = request.args.get('stock_season')
        business_id = request.args.get('business_id')
        sql = f"""SELECT CategoryClass,max(is_online) as is_online FROM  bi_business_brand_new WHERE business_id={business_id}"""
        ret = Scope['bi_saas'].execute(sql)
        online = ret.fetchone()[1]
        if str(online) == '1':
            divsql = f"""AND division='线上' """
        else:
            divsql = f"""AND division='线下' """
        sql = f"""SELECT product_category2,inventory_age,stock_year,stock_season,down_price_rate,SUM(stock_nums) as stock_nums,
            SUM(stock_nums*cost) as stock_cost,SUM((stock_nums*cost)*down_price_rate) as down_price,first_down_price_day,
            first_down_price_rate,second_down_price_day,second_down_price_rate,third_down_price_day,third_down_price_rate,season_sorter 
            FROM hmcdata.bi_finance_stock_downprice WHERE CategoryClass IN
            (
            SELECT CategoryClass FROM  bi_business_brand_new WHERE business_id={business_id}
            ) {divsql} AND stock_year={stock_year} AND stock_season='{stock_season}'
            GROUP BY inventory_age,stock_year,stock_season,season_sorter,down_price_rate,first_down_price_day,first_down_price_rate,second_down_price_day,second_down_price_rate,third_down_price_day,third_down_price_rate,product_category2
			ORDER BY stock_nums DESC"""

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
                # elif val[i] is None:
                #     data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)
        context['data'] = data
        return jsonify(context)

    finally:
        Scope['bi_saas'].remove()


@stock.route('/get_fallInPrice_goods', methods=['GET'], endpoint='get_fallInPrice_goods')
@__token_wrapper
def get_fallInPrice_goods(context):
    try:
        stock_year = request.args.get('stock_year')
        stock_season = request.args.get('stock_season')
        business_id = request.args.get('business_id')
        cat = request.args.get('cat')
        # online = request.args.get('online')
        page = request.args.get('page')
        page_size = request.args.get('pageSize')
        sql = f"""SELECT CategoryClass,max(is_online) as is_online FROM  bi_business_brand_new WHERE business_id={business_id}"""
        ret = Scope['bi_saas'].execute(sql)
        online = ret.fetchone()[1]
        if page_size is None:
            page_size = 10
        if str(online) == '1':
            divsql = f"""AND division='线上' """
        else:
            divsql = f"""AND division='线下' """
        offset = f''' LIMIT {int(page_size)} OFFSET {(int(page) - 1) * int(page_size)} '''
        sql = f"""SELECT i.img_src,d.product_sn,inventory_age,stock_year,stock_season,down_price_rate,SUM(stock_nums) as stock_nums,
            SUM(stock_nums*cost) as stock_cost,SUM((stock_nums*cost)*down_price_rate) as down_price,first_down_price_day,
            first_down_price_rate,second_down_price_day,second_down_price_rate,third_down_price_day,third_down_price_rate,season_sorter 
            FROM hmcdata.bi_finance_stock_downprice  d
			LEFT JOIN bi_product_img i on d.product_sn=i.product_sn WHERE CategoryClass IN
            (
            SELECT CategoryClass FROM  bi_business_brand_new WHERE business_id={business_id}
            ) {divsql} AND stock_year={stock_year} AND stock_season='{stock_season}' AND product_category2='{cat}'
            GROUP BY inventory_age,stock_year,stock_season,season_sorter,down_price_rate,first_down_price_day,
            first_down_price_rate,second_down_price_day,second_down_price_rate,third_down_price_day,third_down_price_rate,product_category2,product_sn
			ORDER BY stock_nums DESC {offset}"""

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
                # elif val[i] is None:
                #     data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)
        context['data'] = data
        return jsonify(context)

    finally:
        Scope['bi_saas'].remove()


@stock.route('/get_fallInPrice_goods_total', methods=['GET'], endpoint='get_fallInPrice_goods_total')
@__token_wrapper
def get_fallInPrice_goods_total(context):
    try:
        stock_year = request.args.get('stock_year')
        stock_season = request.args.get('stock_season')
        business_id = request.args.get('business_id')
        cat = request.args.get('cat')
        sql = f"""SELECT CategoryClass,max(is_online) as is_online FROM  bi_business_brand_new WHERE business_id={business_id}"""
        ret = Scope['bi_saas'].execute(sql)
        online = ret.fetchone()[1]
        if str(online) == '1':
            divsql = f"""AND division='线上' """
        else:
            divsql = f"""AND division='线下' """
        sql = f"""SELECT count(*) as total_rows
            FROM hmcdata.bi_finance_stock_downprice  d
			WHERE CategoryClass IN
            (
            SELECT CategoryClass FROM  bi_business_brand_new WHERE business_id={business_id}
            ) {divsql} AND stock_year={stock_year} AND stock_season='{stock_season}' AND product_category2='{cat}'
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
                # elif val[i] is None:
                #     data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)
        context['data'] = data[0]
        return jsonify(context)

    finally:
        Scope['bi_saas'].remove()


@stock.route('/get_shop_stock_category', methods=['GET'], endpoint='get_shop_stock_category')
@__token_wrapper
def get_shop_stock_category(context):
    try:
        business_unit_id = context['data']['businessUnitId']

        sql = f"""
            SELECT DISTINCT category_name FROM level2_shop_product_year_season_category_wave_stock WHERE  business_unit_id={business_unit_id}
           """

        ret = Scope['bi_saas'].execute(sql)
        data = []
        for val in ret:
            data.append(val[0])

        context['data'] = data
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()


@stock.route('/get_shop_stock_wave', methods=['GET'], endpoint='get_shop_stock_wave')
@__token_wrapper
def get_shop_stock_wave(context):
    try:
        business_unit_id = context['data']['businessUnitId']

        sql = f"""
            SELECT DISTINCT wave_section_name FROM level2_shop_product_year_season_category_wave_stock
            WHERE wave_section_name IS NOT NULL AND business_unit_id={business_unit_id}
            ORDER BY wave_section_id ASC
           """

        ret = Scope['bi_saas'].execute(sql)
        data = []
        for val in ret:
            data.append(val[0])

        context['data'] = data
        return jsonify(context)
    finally:
        Scope['bi_saas'].remove()


@stock.route('/get_shop_stock_info', methods=['GET'], endpoint='get_shop_stock_info')
@__token_wrapper
def get_shop_stock_info(context):
    try:
        business_unit_id = context['data']['businessUnitId']
        product_year = request.args.get('product_year')
        product_season = request.args.get('product_season')
        wave = request.args.get('wave')
        page = request.args.get('page')
        page_size = request.args.get('pageSize')
        category = request.args.get('category')
        shop_id_list = context['data']['offline_shop_id_list']
        region_name = request.args.get('region_name')
        product_sn = request.args.get('product_sn')
        if len(shop_id_list) == 0:
            context['data'] = []
            return jsonify(context)
        shop_ids = ','.join(shop_id_list)
        if page_size is None:
            page_size = 15

        if wave is None or wave == 'all':
            wave_sql = ""
        else:
            wave_sql = f"""AND wave_section_name='{wave}' """
        if category is None or category == 'all':
            category_sql = ""
        else:
            category_sql = f"""AND category_name='{category}' """

        offset = f''' LIMIT {int(page_size)} OFFSET {(int(page) - 1) * int(page_size)} '''

        if product_sn is None or product_sn == '':
            sql = f"""SELECT e.level_name,s.dc_shop_id,shop_code,s.shop_name,s.region_name,SUM(in_model)+sum(sys_model)+SUM(in_transfer) as total_in,SUM(out_sale)+sum(out_transfer) as total_out,SUM(in_model) as in_model,SUM(sys_model) as sys_model,SUM(in_transfer) as in_transfer,
                       SUM(out_sale) as out_sale,SUM(out_transfer) as out_transfer,SUM(out_model) as out_model,
                SUM(out_sale)/(SUM(in_model)+sum(sys_model)+sum(in_transfer)-sum(out_transfer)) as sold_out_rate	FROM level2_shop_product_year_season_category_wave_stock s
                left join bi_offline_shop_ext e on s.dc_shop_id=e.dc_shop_id
                       WHERE s.business_unit_id={business_unit_id} AND year={product_year} AND season='{product_season}' {wave_sql} {category_sql}
                       AND (in_model>0 OR sys_model>0 OR in_transfer>0) AND s.region_name='{region_name}'
                       and s.dc_shop_id IN(
                       {shop_ids}
                       )
                       GROUP BY shop_code,s.shop_name,s.region_name,s.dc_shop_id
                       ORDER BY sold_out_rate DESC
                       {offset}
                      """
        else:
            sql = f"""SELECT e.level_name,s.dc_shop_id,s.shop_code,s.shop_name,s.region_name,SUM(in_model)+sum(sys_model)+SUM(in_transfer) as total_in,SUM(out_sale)+sum(out_transfer) as total_out,SUM(in_model) as in_model,SUM(sys_model) as sys_model,SUM(in_transfer) as in_transfer,
                           SUM(out_sale) as out_sale,SUM(out_transfer) as out_transfer,
SUM(out_model) as out_model,SUM(out_sale)/(SUM(in_model)+sum(sys_model)+sum(in_transfer)-sum(out_transfer)) as sold_out_rate	FROM level2_shop_product_color_type_stock s 
                           left join bi_offline_shop_ext e on s.dc_shop_id=e.dc_shop_id
                           WHERE s.business_unit_id={business_unit_id} AND product_sn='{product_sn}' AND s.region_name='{region_name}'
                           AND (in_model>0 OR sys_model>0 OR in_transfer>0) 
                           and s.dc_shop_id IN(
                           {shop_ids}
                           )
                           GROUP BY shop_code,shop_name,region_name,dc_shop_id
                           ORDER BY sold_out_rate DESC
                           {offset}
                      """
        # print(sql)
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
                # elif val[i] is None:
                #     data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)
        context['data'] = data
        return jsonify(context)

    finally:
        Scope['bi_saas'].remove()


@stock.route('/get_shop_stock_info_total', methods=['GET'], endpoint='get_shop_stock_info_total')
@__token_wrapper
def get_shop_stock_info_total(context):
    try:

        product_year = request.args.get('product_year')
        product_season = request.args.get('product_season')
        business_unit_id = context['data']['businessUnitId']
        wave = request.args.get('wave')
        category = request.args.get('category')
        shop_id_list = context['data']['offline_shop_id_list']
        region_name = request.args.get('region_name')
        product_sn = request.args.get('product_sn')

        if len(shop_id_list) == 0:
            context['data'] = []
            return jsonify(context)
        shop_ids = ','.join(shop_id_list)
        if category is None or category == 'all':
            category_sql = ""
        else:
            category_sql = f"""AND category_name='{category}' """

        if wave is None or wave == 'all':
            wave_sql = ""
        else:
            wave_sql = f"""AND wave_section_name='{wave}' """

        if product_sn is None or product_sn == '':
            sql = f"""
                            SELECT COUNT(*) as total_rows FROM (
                            SELECT 1 FROM level2_shop_product_year_season_category_wave_stock 
                            WHERE business_unit_id={business_unit_id} AND year={product_year} AND season='{product_season}' {wave_sql} {category_sql} 
                            AND (in_model>0 OR sys_model>0 OR in_transfer>0) and dc_shop_id IN(
                            {shop_ids}
                            ) AND region_name='{region_name}'
                            GROUP BY shop_code,shop_name,region_name
                            )a
                               """
        else:
            sql = f"""
                SELECT COUNT(*) as total_rows FROM (
                SELECT 1 FROM level2_shop_product_color_type_stock 
                WHERE business_unit_id={business_unit_id} AND product_sn='{product_sn}'
                AND (in_model>0 OR sys_model>0 OR in_transfer>0) and dc_shop_id IN(
                {shop_ids}
                ) AND region_name='{region_name}'
                GROUP BY shop_code,shop_name,region_name,dc_shop_id
                )a
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
                # elif val[i] is None:
                #     data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)
        context['data'] = data[0]
        return jsonify(context)

    finally:
        Scope['bi_saas'].remove()


@stock.route('/get_shop_stock_info_bigzone', methods=['GET'], endpoint='get_shop_stock_info_bigzone')
@__token_wrapper
def get_shop_stock_info_bigzone(context):
    try:
        business_unit_id = context['data']['businessUnitId']
        product_year = request.args.get('product_year')
        product_season = request.args.get('product_season')
        wave = request.args.get('wave')

        category = request.args.get('category')
        shop_id_list = context['data']['offline_shop_id_list']
        product_sn = request.args.get('product_sn')

        if len(shop_id_list) == 0:
            context['data'] = []
            return jsonify(context)
        shop_ids = ','.join(shop_id_list)

        if wave is None or wave == 'all':
            wave_sql = ""
        else:
            wave_sql = f"""AND wave_section_name='{wave}' """
        if category is None or category == 'all':
            category_sql = ""
        else:
            category_sql = f"""AND category_name='{category}' """
        if product_sn is None or product_sn == '':
            sql = f"""SELECT b_area,SUM(in_model)+sum(sys_model)+SUM(in_transfer) as total_in,SUM(out_sale)+sum(out_transfer) as total_out,SUM(in_model) as in_model,SUM(sys_model) as sys_model,SUM(in_transfer) as in_transfer,
                        SUM(out_sale) as out_sale,SUM(out_transfer) as out_transfer,SUM(out_model) as out_model,SUM(out_sale)/(SUM(in_model)+sum(sys_model)+sum(in_transfer)-sum(out_transfer)) as sold_out_rate	FROM level2_shop_product_year_season_category_wave_stock 
                        WHERE business_unit_id={business_unit_id} AND year={product_year} AND season='{product_season}' {wave_sql} {category_sql} 
                        AND (in_model>0 OR sys_model>0 OR in_transfer>0) and dc_shop_id IN(
                        {shop_ids}
                        )
                        GROUP BY b_area
                        ORDER BY sold_out_rate DESC
                       """
        else:
            sql = f"""SELECT b_area,SUM(in_model)+sum(sys_model)+SUM(in_transfer) as total_in,SUM(out_sale)+sum(out_transfer) as total_out,SUM(in_model) as in_model,SUM(sys_model) as sys_model,SUM(in_transfer) as in_transfer,
                        SUM(out_sale) as out_sale,SUM(out_transfer) as out_transfer,SUM(out_model) as out_model,SUM(out_sale)/(SUM(in_model)+sum(sys_model)+sum(in_transfer)-sum(out_transfer)) as sold_out_rate	FROM level2_shop_product_color_type_stock 
                        WHERE business_unit_id={business_unit_id} AND product_sn='{product_sn}'
                        AND (in_model>0 OR sys_model>0 OR in_transfer>0) and dc_shop_id IN(
                        {shop_ids}
                        )
                        GROUP BY b_area
                        ORDER BY sold_out_rate DESC
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
                # elif val[i] is None:
                #     data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)
        context['data'] = data
        return jsonify(context)

    finally:
        Scope['bi_saas'].remove()


@stock.route('/get_shop_stock_info_zone', methods=['GET'], endpoint='get_shop_stock_info_zone')
@__token_wrapper
def get_shop_stock_info_zone(context):
    try:
        business_unit_id = context['data']['businessUnitId']
        product_year = request.args.get('product_year')
        product_season = request.args.get('product_season')
        wave = request.args.get('wave')
        b_area = request.args.get('b_area')
        category = request.args.get('category')
        shop_id_list = context['data']['offline_shop_id_list']
        product_sn = request.args.get('product_sn')

        if len(shop_id_list) == 0:
            context['data'] = []
            return jsonify(context)
        shop_ids = ','.join(shop_id_list)

        if wave is None or wave == 'all':
            wave_sql = ""
        else:
            wave_sql = f"""AND wave_section_name='{wave}' """
        if category is None or category == 'all':
            category_sql = ""
        else:
            category_sql = f"""AND category_name='{category}' """

        if product_sn is None or product_sn == '':
            sql = f"""SELECT region_name,SUM(in_model)+sum(sys_model)+SUM(in_transfer) as total_in,SUM(out_sale)+sum(out_transfer) as total_out,SUM(in_model) as in_model,SUM(sys_model) as sys_model,SUM(in_transfer) as in_transfer,
                       SUM(out_sale) as out_sale,SUM(out_transfer) as out_transfer,SUM(out_model) as out_model,SUM(out_sale)/(SUM(in_model)+sum(sys_model)+sum(in_transfer)-sum(out_transfer)) as sold_out_rate	FROM level2_shop_product_year_season_category_wave_stock 
                       WHERE business_unit_id={business_unit_id} AND year={product_year} AND season='{product_season}' {wave_sql} {category_sql} 
                    AND (in_model>0 OR sys_model>0 OR in_transfer>0) AND b_area='{b_area}' 
                       and dc_shop_id IN(
                       {shop_ids}
                       )
                       GROUP BY region_name
                       ORDER BY sold_out_rate DESC"""
        else:
            sql = f"""SELECT region_name,SUM(in_model)+sum(sys_model)+SUM(in_transfer) as total_in,SUM(out_sale)+sum(out_transfer) as total_out,SUM(in_model) as in_model,SUM(sys_model) as sys_model,SUM(in_transfer) as in_transfer,
                                   SUM(out_sale) as out_sale,SUM(out_transfer) as out_transfer,SUM(out_model) as out_model,SUM(out_sale)/(SUM(in_model)+sum(sys_model)+sum(in_transfer)-sum(out_transfer)) as sold_out_rate	FROM level2_shop_product_color_type_stock 
                                   WHERE business_unit_id={business_unit_id} 
                                   AND (in_model>0 OR sys_model>0 OR in_transfer>0) AND b_area='{b_area}'   AND product_sn='{product_sn}'
                                   and dc_shop_id IN(
                                   {shop_ids}
                                   )
                                   GROUP BY region_name
                                   ORDER BY sold_out_rate DESC"""

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
                # elif val[i] is None:
                #     data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)
        context['data'] = data
        return jsonify(context)

    finally:
        Scope['bi_saas'].remove()


@stock.route('/get_shop_stock_info_goods', methods=['GET'], endpoint='get_shop_stock_info_goods')
@__token_wrapper
def get_shop_stock_info_goods(context):
    try:
        business_unit_id = context['data']['businessUnitId']
        product_year = request.args.get('product_year')
        product_season = request.args.get('product_season')
        wave = request.args.get('wave')
        b_area = request.args.get('b_area')
        category = request.args.get('category')
        shop_id_list = context['data']['offline_shop_id_list']
        page = request.args.get('page')
        page_size = request.args.get('pageSize')
        region_name = request.args.get('region_name')
        dc_shop_id = request.args.get('dc_shop_id')
        product_sn = request.args.get('product_sn')
        if product_sn is None or product_sn == '':
            product_sn_sql = ""
        else:
            product_sn_sql = f"""AND t.product_sn='{product_sn}' """
        if len(shop_id_list) == 0:
            context['data'] = []
            return jsonify(context)
        shop_ids = ','.join(shop_id_list)
        if dc_shop_id is None or dc_shop_id == "":
            shop_sql = ""
        else:
            shop_sql = f"""AND dc_shop_id='{dc_shop_id}' """
        if b_area is None or b_area == 'all':
            b_area_sql = ""
        else:
            b_area_sql = f"""AND b_area='{b_area}' """
        if region_name is None or region_name == 'all':
            region_name_sql = ""
        else:
            region_name_sql = f"""AND region_name='{region_name}' """
        if wave is None or wave == 'all':
            wave_sql = ""
        else:
            wave_sql = f"""AND wave_section_name='{wave}' """
        if category is None or category == 'all':
            category_sql = ""
        else:
            category_sql = f"""AND category_name='{category}' """
        if page_size is None:
            page_size = 10
        offset = f''' LIMIT {int(page_size)} OFFSET {(int(page) - 1) * int(page_size)} '''
        if product_sn=="" or product_sn is None:
            sql = f"""SELECT max(i.img_src) as img_src,t.product_sn,color_name,max(goods_name) as goods_name,SUM(in_model)+sum(sys_model)+SUM(in_transfer) as total_in,SUM(out_sale)+sum(out_transfer) as total_out,SUM(in_model) as in_model,SUM(sys_model) as sys_model,SUM(in_transfer) as in_transfer,
                    SUM(out_sale) as out_sale,SUM(out_transfer) as out_transfer,SUM(out_model) as out_model,SUM(out_sale)/(SUM(in_model)+sum(sys_model)+sum(in_transfer)-sum(out_transfer)) as sold_out_rate,transfer_pool	FROM level2_shop_product_color_type_stock t
                    LEFT JOIN bi_product_img i ON t.product_sn=i.product_sn
                    WHERE t.business_unit_id={business_unit_id} AND year={product_year} AND season='{product_season}' {wave_sql} {category_sql} {product_sn_sql}
                    AND (in_model>0 OR sys_model>0 OR in_transfer>0) {b_area_sql} {region_name_sql} {shop_sql}
                    and dc_shop_id IN(
                    {shop_ids}
                    )
                    GROUP BY t.product_sn,color_name
                    ORDER BY sold_out_rate DESC
                    {offset}
                    """
        else:
            sql = f"""SELECT max(i.img_src) as img_src,t.product_sn,color_name,max(goods_name) as goods_name,SUM(in_model)+sum(sys_model)+SUM(in_transfer) as total_in,SUM(out_sale)+sum(out_transfer) as total_out,SUM(in_model) as in_model,SUM(sys_model) as sys_model,SUM(in_transfer) as in_transfer,
                    SUM(out_sale) as out_sale,SUM(out_transfer) as out_transfer,SUM(out_model) as out_model,SUM(out_sale)/(SUM(in_model)+sum(sys_model)+sum(in_transfer)-sum(out_transfer)) as sold_out_rate,transfer_pool	FROM level2_shop_product_color_type_stock t
                    LEFT JOIN bi_product_img i ON t.product_sn=i.product_sn
                    WHERE t.business_unit_id={business_unit_id}  {product_sn_sql}
                    AND (in_model>0 OR sys_model>0 OR in_transfer>0) {b_area_sql} {region_name_sql} {shop_sql}
                    and dc_shop_id IN(
                    {shop_ids}
                    )
                    GROUP BY t.product_sn,color_name
                    ORDER BY sold_out_rate DESC
                    {offset}
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
                # elif val[i] is None:
                #     data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)
        context['data'] = data
        return jsonify(context)

    finally:
        Scope['bi_saas'].remove()


@stock.route('/get_shop_stock_info_goods_total', methods=['GET'], endpoint='get_shop_stock_info_goods_total')
@__token_wrapper
def get_shop_stock_info_goods_total(context):
    try:
        business_unit_id = context['data']['businessUnitId']
        product_year = request.args.get('product_year')
        product_season = request.args.get('product_season')
        wave = request.args.get('wave')
        b_area = request.args.get('b_area')
        category = request.args.get('category')
        region_name = request.args.get('region_name')
        dc_shop_id = request.args.get('dc_shop_id')
        shop_id_list = context['data']['offline_shop_id_list']
        product_sn = request.args.get('product_sn')
        if product_sn is None or product_sn == '':
            product_sn_sql = ""
        else:
            product_sn_sql = f"""AND product_sn='{product_sn}' """
        if len(shop_id_list) == 0:
            context['data'] = []
            return jsonify(context)
        shop_ids = ','.join(shop_id_list)
        if dc_shop_id is None or dc_shop_id == "":
            shop_sql = ""
        else:
            shop_sql = f"""AND dc_shop_id='{dc_shop_id}' """
        if b_area is None or b_area == '':
            b_area_sql = ""
        else:
            b_area_sql = f"""AND b_area='{b_area}' """
        if region_name is None or region_name == '':
            region_name_sql = ""
        else:
            region_name_sql = f"""AND region_name='{region_name}' """
        if wave is None or wave == 'all':
            wave_sql = ""
        else:
            wave_sql = f"""AND wave_section_name='{wave}' """
        if category is None or category == 'all':
            category_sql = ""
        else:
            category_sql = f"""AND category_name='{category}' """
        if product_sn == "" or product_sn is None:
            sql = f"""
                    SELECT COUNT(*) as total_rows FROM (
                    SELECT 1 FROM level2_shop_product_color_type_stock t
                    WHERE t.business_unit_id={business_unit_id} AND year={product_year} AND season='{product_season}' {wave_sql} {category_sql} {product_sn_sql}
                    AND (in_model>0 OR sys_model>0 OR in_transfer>0) {b_area_sql} {region_name_sql} {shop_sql}
                    and dc_shop_id IN(
                    {shop_ids}
                    )GROUP BY t.product_sn,color_name
                    )a
                    """
        else:
            sql = f"""
                    SELECT COUNT(*) as total_rows FROM (
                    SELECT 1 FROM level2_shop_product_color_type_stock t
                    WHERE t.business_unit_id={business_unit_id}  {product_sn_sql}
                    AND (in_model>0 OR sys_model>0 OR in_transfer>0) {b_area_sql} {region_name_sql} {shop_sql}
                    and dc_shop_id IN(
                    {shop_ids}
                    )GROUP BY t.product_sn,color_name
                    )a
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
                # elif val[i] is None:
                #     data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)
        context['data'] = data[0]
        return jsonify(context)

    finally:
        Scope['bi_saas'].remove()


@stock.route('/get_category_class_turnover', methods=['GET'], endpoint='get_category_class_turnover')
@__token_wrapper
def get_category_class_turnover(context):
    try:
        business_id = request.args.get('business_id')
        year = request.args.get('year')
        sql = f"""
                SELECT total_year ,total_month ,sum(total_sale_cost-total_return_cost )as total_sale_cost,sum(end_of_cost)  as end_of_cost ,sum(total_sale_cost -total_return_cost )/sum(beginning_cost +end_of_cost)*2*12/total_month as turnover,
                sum(ifnull(sale_cost,0)-ifnull(return_cost,0)) as sale_cost
                FROM hmcdata.bi_turnover_summary s WHERE total_year ={year}
                AND EXISTS(
                            SELECT categoryClass,is_online FROM  bi_business_brand_new n WHERE business_id={business_id} and s.category_class=n.categoryClass and s.online=n.is_online
                )
				GROUP BY total_year ,total_month
                ORDER BY total_month ASC
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
                # elif val[i] is None:
                #     data_dict[column] = 0
                else:
                    data_dict[column] = val[i]
            data.append(data_dict)
        context['data'] = data
        return jsonify(context)

    finally:
        Scope['bi_saas'].remove()



@stock.route('/export_fallInPrice_goods', methods=['GET'], endpoint='export_fallInPrice_goods')
@__token_download
def export_fallInPrice_goods(context):
    try:
        business_id = request.args.get('business_id')
        sql = f"""SELECT CategoryClass,max(is_online) as is_online FROM  bi_business_brand_new WHERE business_id={business_id}"""
        ret = Scope['bi_saas'].execute(sql)
        online = ret.fetchone()[1]
        if str(online) == '1':
            divsql = f"""AND division='线上' """
        else:
            divsql = f"""AND division='线下' """
        sql = f"""SELECT d.product_sn as '款号',inventory_age as '库龄',stock_year as '年份',stock_season as '季节',down_price_rate as '目前跌价比',SUM(stock_nums)  as '库存数',
            SUM(stock_nums*cost)  as '总成本',SUM((stock_nums*cost)*down_price_rate)  as '目前跌价',first_down_price_day as '下次跌价日期',
            first_down_price_rate as '下次跌价比',SUM((stock_nums*cost)*first_down_price_rate)  as '下次跌价金额',second_down_price_day as '第二次跌价日期',second_down_price_rate as '第二次跌价比',SUM((stock_nums*cost)*second_down_price_rate)  as '第二次跌价金额',third_down_price_day as '第三次跌价日期',
            third_down_price_rate as '第三次跌价比' ,SUM((stock_nums*cost)*third_down_price_rate)  as '第三次跌价金额'
            FROM hmcdata.bi_finance_stock_downprice  d
			 WHERE CategoryClass IN
            (
            SELECT CategoryClass FROM  bi_business_brand_new WHERE business_id={business_id}
            ) {divsql}
            GROUP BY inventory_age,stock_year,stock_season,season_sorter,down_price_rate,first_down_price_day,
            first_down_price_rate,second_down_price_day,second_down_price_rate,third_down_price_day,third_down_price_rate,product_category2,product_sn
			ORDER BY stock_nums DESC """

        ret = Scope['bi_saas'].execute(sql)
        columns = ret.keys()
        return export_file(columns, ret, f"""存货跌价明细{datetime.datetime.now().strftime('%Y-%m-%d %H_%M_%S')}""")


    finally:
        Scope['bi_saas'].remove()
