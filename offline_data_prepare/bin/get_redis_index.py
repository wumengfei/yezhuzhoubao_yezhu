# -*- coding: utf-8 -*-
import sys
sys.path.append('conf')
import conf
import urllib
from datetime import *
from datetime import time
import json
import redis_client
import yzd_redias_api_new

def get_index_from_redis():
    rc = redis_client.Redis_client(conf.redis_conf)
    index_file = open(conf.index_file, 'w')
    # 相似房源,某一聚类,业主端2.0给出
    # 将相似房源以key,val的形式存入index_file文件中
    k_set = rc.get_keys('yzd_cluster_2_houses_')
    for k_item in k_set:
        tmp_cls_info = list(rc.rc.smembers(k_item))
        tmp_dict = {}
        tmp_dict['key'] = k_item
        tmp_dict['value'] = tmp_cls_info
        index_file.write(json.dumps(tmp_dict))
        index_file.write("\n")
    index_file.close()

if __name__ == "__main__":
    get_index_from_redis()

