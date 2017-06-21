#encoding=utf-8
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import io,shutil  
import urllib,time
import getopt,string
from flask import Flask
from flask import request
from redis_client import *
import math
import uuid
import json
sys.path.append("../conf")
import conf
import pdb
import traceback

err_f = open(conf.error_file, 'w')
rc = Redias_client(conf.redis_conf)
# rc: redis client
# r_id: resblock_id
# get neighbourhood resblock
# dist:1

def Get_house_info(rc ,house_code):
    house_code = 'yzd_house_2_cluster_' + house_code
    house_info = rc.rc.hgetall(house_code)
    cls_id = ''
    fea_list = ''
    r_id = ''
    if 'cluster_id' in house_info:
        cls_id = house_info['cluster_id']
    if 'feature_vec' in house_info:
        fea_list = house_info['feature_vec']
    if 'resblock' in house_info:
        r_id = house_info['resblock']
    fea_list_arry = fea_list.split(',')
    if cls_id != '' and len(fea_list_arry) > 0 and r_id != '':
        return (cls_id, fea_list_arry, r_id)
    else:
        return None

def Get_neighbour_res(rc, r_id, dist):
    resblock_id = 'yzd_resblock_neighbor_' + r_id
    neighbour_res = rc.rc.zrangebyscore(resblock_id, 0, dist)
    r_set = set()
    for item in neighbour_res:
        item = str(item)
        cur_rid = item.split('_')[0]
        if cur_rid != r_id:
            r_set.add(cur_rid)
    return r_set

# Return similar house
# rc: redis client
# cls_id: cluster id
# res_set: result set
def Get_recall_houses(rc, cls_id, res_set):
    h_set = set()
    cls_id = int(cls_id)
    try:
        for r_item in res_set:
            if r_item in rc:
                cur_h_set = rc[r_item]
            else:
                continue
            for h_item in cur_h_set:
                cur_h_cls = int(h_item[1])
                if cur_h_cls == cls_id:
                    h_set.add((h_item[0],h_item[2],h_item[3],r_item))
        return h_set
    except:
        return None

def Filter_recall_houses(h_set, type):
    rt_set = set()
    for item in h_set:
        is_deal = int(item[2])
        created_days = int(item[1])
        if type == 0 and is_deal == 1: # dealed house list
            rt_set.add(item[0])
        if type == 1 and is_deal == 0: # onsale house list
            rt_set.add(item[0])
        if type == 2 and is_deal == 0 and created_days <= 5: # recently onsale house list
            rt_set.add(item[0])
    return rt_set


def caculate_same_house(h_fea, h_set):
    house_list = []
    for item in h_set:
        house_code = 'yzd_house_2_cluster_' + item
        house_info = rc.rc.hgetall(house_code)
        if 'feature_vec' in house_info:
            t_fea = house_info['feature_vec'].split(',')
            sim_value = get_cos_sim(h_fea, t_fea)
            house_tem = tem(item, sim_value)
            house_list.append(house_tem)
    if len(house_list) > 0:
        house_list = sorted(house_list,reverse=True)
    return house_list

def load_redis():
    cluster_dict = {}
    file_obj = open(conf.index_file, 'r')
    for line in file_obj:
        line = line.strip()
        tmp_dict = json.loads(line)
        key = tmp_dict['key']
        value = tmp_dict['value']
        for item in value:
            item = eval(item)
            r_id = item[1]
            cls_id = int(key.replace('yzd_cluster_2_houses_', ''))
            if not r_id in cluster_dict:
                cluster_dict[r_id] = []
            cluster_dict[r_id].append((item[0], cls_id, item[2], item[3]))
    return cluster_dict

def get_index_from_redis():
    rc = redis_client.Redias_client(conf.redis_conf)
    index_file = open(conf.index_file, 'w')
    k_set = rc.get_keys('yzd_cluster_2_houses_')
    for k_item in k_set:
        tmp_cls_info = list(rc.rc.smembers(k_item))
        tmp_dict = {}
        tmp_dict['key'] = k_item
        tmp_dict['value'] = tmp_cls_info
        index_file.write(json.dumps(tmp_dict))
        index_file.write("\n")
    index_file.close()

#def get_similar_house(index_dict, house_code):
def get_similar_house(index_dict, house_code, res_self):
    #index_dict = load_redis()
    #house_code = "BJCP90150325"
    try:
        tup = Get_house_info(rc ,house_code)
        if tup is None:
            print house_code, "fail to get house info"
        cls_id = tup[0]
        res_id = tup[2]
        resblock_set = Get_neighbour_res(rc, res_id, 1)
        resblock_set.add(res_self)
        similar_list = Get_recall_houses(index_dict, tup[0], resblock_set)
        house_list = []
        for item in similar_list:
            house_list.append(item[0])
        return house_list
    except Exception, e:
        traceback.print_exc(file = err_f)
        return []

if __name__ == "__main__":
    index_dict = load_redis()
    get_similar_house(index_dict, "BJCP89156755")

