#!/usr/bin/python
# -*- coding:utf-8 -*-
# @Author  : kingsley kwong

from __future__ import unicode_literals

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (Column, Integer, String, DateTime, Sequence, TIMESTAMP, Float, Numeric, CHAR)
import datetime
import decimal

Base = declarative_base()


# Create your models here.


def to_dict(self):
    return {
        c.name: getattr(self, c.name, None) if not isinstance(getattr(self, c.name, None), decimal.Decimal) else float(
            getattr(self, c.name, None)) for c in self.__table__.columns}


Base.to_dict = to_dict


class TbSycmShopAuctionDetail(Base):
    __tablename__ = 'tb_sycm_shop_auction_detail'
    __table_args__ = {'schema': 'hmcdata'}

    terminal = Column(String(128))
    auction_id = Column(String(128))
    auction_title = Column(String(255))
    auction_status = Column(String(255))
    auction_url = Column(String(384))
    pv = Column(Integer)
    uv = Column(Integer)
    avg_page_duration = Column(Float)
    detail_page_bounce_rate = Column(Float)
    gmv_order_cvr = Column(Float)
    gmv_to_alipay_cvr = Column(Float)
    pay_order_cvr = Column(Float)
    gmv_order_amt = Column(Float)
    gmv_auction_cnt = Column(Integer)
    gmv_order_buyer_cnt = Column(Integer)
    alipay_trade_amt = Column(Float)
    alipay_auction_cnt = Column(Integer)
    add_cart_auction_cnt = Column(Integer)
    avg_uv_value = Column(Float)
    click_cnt = Column(Integer)
    click_rate = Column(Float)
    impression_cnt = Column(Integer)
    collect_goods_buyer_cnt = Column(Integer)
    search_guide_alipay_buyer_cnt = Column(Integer)
    avg_amt_per_order = Column(Float)
    search_to_alipay_cvr = Column(Float)
    search_guide_uv_cnt = Column(Integer)
    alipay_order_buyer_cnt = Column(Integer)
    selling_after_sales_succ_refund_amt = Column(Float)
    selling_after_sales_succ_refund_cnt = Column(Integer)
    st_date = Column(DateTime)
    shop_name = Column(String(255))
    shop_id = Column(Integer)
    seller_id = Column(Integer)
    uni_key = Column(String(255))
    pic_path = Column(String)
    outer_goods_id = Column(String)
    id = Column(Integer, primary_key=True)


class TbSycmAuctionSKUDetail(Base):
    __tablename__ = 'tb_sycm_auction_sku_detail'
    __table_args__ = {'schema': 'hmcdata'}

    st_date = Column(DateTime)
    auction_id = Column(Integer)
    auction_title = Column(String(255))
    tb_sku_id = Column(Integer)
    tb_sku = Column(String(255))
    add_cart_auction_cnt = Column(Integer)
    pc_add_cart_auction_cnt = Column(Integer)
    wireless_add_cart_auction_cnt = Column(Integer)
    gmv_auction_cnt = Column(Integer)
    pc_gmv_auction_cnt = Column(Integer)
    wireless_gmv_auction_cnt = Column(Integer)
    gmv_order_amt = Column(Float)
    pc_gmv_order_amt = Column(Float)
    wireless_gmv_order_amt = Column(Float)
    gmv_order_buyer_cnt = Column(Integer)
    pc_gmv_order_buyer_cnt = Column(Integer)
    wireless_gmv_order_buyer_cnt = Column(Integer)
    alipay_auction_cnt = Column(Integer)
    pc_alipay_auction_cnt = Column(Integer)
    wireless_alipay_auction_cnt = Column(Integer)
    alipay_trade_amt = Column(Float)
    pc_alipay_trade_amt = Column(Float)
    wireless_alipay_trade_amt = Column(Float)
    alipay_order_buyer_cnt = Column(Integer)
    pc_alipay_order_buyer_cnt = Column(Integer)
    wireless_alipay_order_buyer_cnt = Column(Integer)
    barcode = Column(String(128))
    goods_sn = Column(String(128))
    goods_color = Column(String(128))
    brand = Column(String(128))
    shop_name = Column(String(255))
    shop_id = Column(Integer)
    seller_id = Column(Integer)
    uni_key = Column(String(255))
    id = Column(Integer, primary_key=True)


class E3TaobaoItems(Base):
    __tablename__ = 'e3_taobao_items'
    __table_args__ = {'schema': 'hmcdata'}

    taobao_items_id = Column(Integer)
    taobao_trade_id = Column(Integer)
    tid = Column(Integer)
    shop_id = Column(Integer)
    total_fee = Column(Float)
    discount_fee = Column(Float)
    adjust_fee = Column(Float)
    payment = Column(Float)
    modified = Column(DateTime)
    item_meal_id = Column(String)
    status = Column(String)
    iid = Column(String)
    sku_id = Column(Integer)
    sku_properties_name = Column(String)
    item_meal_name = Column(String)
    num = Column(Integer)
    title = Column(String)
    price = Column(Float)
    pic_path = Column(String)
    seller_nick = Column(String)
    buyer_nick = Column(String)
    refund_status = Column(String)
    oid = Column(Integer)
    outer_iid = Column(String)
    outer_sku_id = Column(String)
    snapshot_url = Column(String)
    snapshot = Column(String)
    timeout_action_time = Column(DateTime)
    refund_id = Column(String)
    seller_type = Column(String)
    num_iid = Column(String)
    cid = Column(Integer)
    is_oversold = Column(CHAR)
    error_code = Column(Integer)
    error_msg = Column(String)
    is_tran_success = Column(Integer)
    lastchanged = Column(DateTime)
    part_mjz_discount = Column(Float)
    hm_yugou_id = Column(Integer)
    hm_yg_fhrq = Column(Integer)
    hm_is_yugou = Column(Integer)
    hm_yg_is_kc_yfp = Column(Integer)
    hm_is_sbgys = Column(Integer)
    shop_name = Column(String)
    id = Column(Integer, primary_key=True)
    gmt_modified = Column(DateTime)


class RdsShopSubOrderDetail(Base):
    __tablename__ = 'rds_shop_sub_order_detail'
    __table_args__ = {'schema': 'hmcdata'}

    oid = Column(Float)
    status = Column(String)
    date_created = Column(String)
    hour_create = Column(String)
    date_pay = Column(String)
    hour_pay = Column(String)
    pic_path = Column(String)
    title = Column(String)
    sku_properties_name = Column(String)
    trade_from = Column(String)
    is_jhs = Column(String)
    is_wap = Column(String)
    buyer_rate = Column(String)
    is_oversold = Column(String)
    is_www = Column(String)
    is_meal = Column(String)
    logistics_company = Column(String)
    num_iid = Column(Integer)
    refund_status = Column(String)
    seller_rate = Column(String)
    shipping_type = Column(String)
    store_code = Column(String)
    sku_id = Column(Integer)
    outer_iid = Column(String)
    outer_sku_id = Column(String)
    cid = Column(String)
    create = Column(DateTime)
    pay_time = Column(DateTime)
    consign_time = Column(DateTime)
    end_time = Column(DateTime)
    item_meal_id = Column(Integer)
    shop_id = Column(Integer)
    seller_nick = Column(String)
    timeout_action_time = Column(DateTime)
    price = Column(Float)
    num = Column(Integer)
    total_fee = Column(Float)
    payment = Column(Float)
    part_mjz_discount = Column(Float)
    discount_fee = Column(Float)
    divide_order_fee = Column(Float)
    adjust_fee = Column(Float)
    modified = Column(DateTime)
    tid = Column(Float)
    buyer_nick = Column(String)
    receiver_state = Column(String)
    receiver_city = Column(String)
    receiver_district = Column(String)
    seller_memo = Column(String)
    shop_name = Column(String)
    id = Column(Integer, primary_key=True)
    src_business_id = Column(String)


class BiDataAuthority(Base):
    __tablename__ = 'bi_data_authority'
    __table_args__ = {'schema': 'hmcdata'}

    account_role_id = Column(Integer)
    real_name = Column(String)
    email = Column(String)
    shop_ids = Column(String)
    offline_shop_ids = Column(String)
    bi_business_clazz_ids = Column(Integer)
    bi_province_ids = Column(String)
    id = Column(Integer, primary_key=True)


class IomScmProductList(Base):
    __tablename__ = 'iom_scm_product_list'
    __table_args__ = {'schema': 'hmcdata'}

    product_sn = Column(String)
    goods_id = Column(Integer)
    product_category1 = Column(String)
    product_category2 = Column(String)
    product_category3 = Column(String)
    cost = Column(Numeric(12, 4))
    o2o_cost = Column(Numeric(12, 4))
    product_cost = Column(Numeric(12, 4))
    brand = Column(String)
    product_year = Column(Integer)
    product_season = Column(String)
    doc_year = Column(Integer)
    doc_season = Column(String)
    collection = Column(String)
    wave_session = Column(String)
    design_product_sn = Column(String)
    designer_name = Column(String)
    price = Column(Numeric(12, 4))
    main_img_src = Column(String)
    report_created_date = Column(DateTime)
    gmt_src_created = Column(DateTime)
    gmt_created = Column(DateTime)
    gmt_modified = Column(DateTime)
    src_business_id = Column(String)
    SalePrice = Column(Numeric(12, 4))
    CategoryClass = Column(String)
    Component = Column(String)
    ExecStandard = Column(String)
    ProductTitle = Column(String)
    ProductDesc = Column(String)
    ProductDaysSupply = Column(Integer)
    Material = Column(String)
    Technology = Column(String)
    Collar = Column(String)
    Shape = Column(String)
    ClothesLong = Column(String)
    SleeveShape = Column(String)
    SleeveLong = Column(String)
    SkirtLong = Column(String)
    WaistShape = Column(String)
    TrousersShape = Column(String)
    OutSeam = Column(String)
    Peplum = Column(String)
    isGift = Column(String)
    FOBCost = Column(Numeric(12, 4))
    isuploadK3 = Column(Integer)

    id = Column(Integer, primary_key=True)
