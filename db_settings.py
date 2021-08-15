# -*- coding:utf-8 -*-
#@Author: Joshua
#@File : db_settings.py

SSH_SOURCE = {
    'hmc': {
        'ssh_host': "",
        'ssh_port': ,
        'ssh_user': "",
        'ssh_password': ""
    }
}
SSH_O2O_SOURCE = {
    'o2o': {
        'ssh_host': "",
        'ssh_port': ,
        'ssh_user': "",
        'ssh_password': ""
    }
}

REDIS_SOURCE = {
    'local_redis': {
        'host': '',
        'port': ,
        'password': '',
        'db': 5
    },
    'bi_redis': {
        'host': '',
        'port': ,
        'password': '',
        'db':
    }
}

DATA_SOURCE = {
    'vertica': {
        'engine': 'vertica',
        'host': '',
        'port': ,
        'user': 'hmc',
        'passwd': '',
        'db': ''
    },
    'bi_saas': {
        'engine': 'mysql',
        'host': 'rm-uf6xd393y559jo5sh774.mysql.rds.aliyuncs.com',
        'port': ,
        'user': '',
        'passwd': '',
        'db': '',
        'ssh': '',
    },
    'hmc_upms': {
        'engine': 'mysql',
        'host': 'prod-hmcloud-app-ro.rwlb.rds.aliyuncs.com',
        'port': ,
        'user': '',
        'passwd': '',
        'db': '',
        'ssh': '',
    },
    'hmc_scm_product': {
        'engine': 'mysql',
        'host': 'prod-hmcloud-app-ro.rwlb.rds.aliyuncs.com',
        'port': ,
        'user': '',
        'passwd': '',
        'db': '',
        'ssh': 'hmc',
    },
    'hmc_product_center': {
        'engine': 'mysql',
        'host': 'prod-hmcloud-center-ro.rwlb.rds.aliyuncs.com',
        'port': ,
        'user': '',
        'passwd': '',
        'db': '',
        'ssh': '',
    },
    'hmc_member_center': {
        'engine': 'mysql',
        'host': 'prod-hmcloud-center-ro.rwlb.rds.aliyuncs.com',
        'port': ,
        'user': '',
        'passwd': '',
        'db': '',
        'ssh': '',
    },
    'hmc_oms_business': {
        'engine': 'mysql',
        'host': 'prod-hmcloud-app-ro.rwlb.rds.aliyuncs.com',
        'port': ,
        'user': '',
        'passwd': '',
        'db': '',
        'ssh': '',
    },
    'hmc_trade_center_read': {
        'engine': 'mysql',
        'host': 'prod-hmcloud-center-ro.rwlb.rds.aliyuncs.com',
        'port': ,
        'user': 'hmc_bi_ro',
        'passwd': '',
        'db': '',
        'ssh': '',
    },
}
