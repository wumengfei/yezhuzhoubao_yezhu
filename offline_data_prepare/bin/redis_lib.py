# -*- coding: utf-8 -*-
import sys
sys.path.append('../conf')
import conf
import urllib
from datetime import *
from datetime import time
import json
import redis_client
import yzd_redias_api_new

def load_redis()
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
            cluster_dict[r_id].append(item[0], cls_id, item[2], item[3])
    return cluster_dict

def get_similar_house_list(house_code):
    rc = Redias_client(conf.redis_conf)
    tup = Get_house_info(rc ,house_code)
    resblock_set = Get_neighbour_res(rc, tup[2])
    Get_recall_houses
    similar_list = Get_recall_houses(cluster_dict, tup[0], resblock_set)
    return similar_list

# 近一周的带看次数
def load_showing_data():
    file_obj = open(conf.showing_file, 'r')
    showing_dict = {}
    for line in file_obj:
        tmp = line.rstrip("\n").split("\t")
        house_code = tmp[0]
        showing = tmp[3]
        day = tmp[4][0:7]
        if house_code not in showing_dict:
            showing_dict[house_code] = {}
            showing_dict[house_code]["showing"] = showing
            showing_dict[house_code]["day"] = day
        else:
            if day > showing_dict[house_code]["day"]:
                showing_dict[house_code]["showing"] = showing
                showing_dict[house_code]["day"] = day

    return showing_dict

# 所有挂牌房源的带看次数,以及基本信息
def list_house_this_week(showing_dict, sold_dict):
    file_obj = open(conf.list_house, 'r')
    list_house_dict = {}
    for line in file_obj:
        tmp = line.rstrip("\n").split("\t")
        house_code = tmp[0]
        if house_code not in list_house_dict:
            list_house_dict[house_code] = {}
        
        build_area = tmp[2]
        total_prices = tmp[1]
        create_time = tmp[4]
        create_time = datetime.strptime(create_time,'%Y-%m-%d %H:%M:%S')
        if build_area == "NULL" or total_prices == "NULL" or float(build_area) == 0:
            continue
        list_house_dict[house_code]["build_area"] = build_area
        list_house_dict[house_code]["total_prices"] = total_prices
        if house_code in sold_dict:
            list_house_dict[house_code]["realmoney"] = sold_dict[house_code]["realmoney"]
            list_house_dict[house_code]["dealdate"] = sold_dict[house_code]["deal_time"]
            time_tmp = list_house_dict[house_code]["dealdate"]
            deal_date = datetime.strptime(time_tmp, "%Y%m%d")
            list_house_dict[house_code]["sold_interval"] = deal_date - create_time
            list_house_dict[house_code]["sold_avg"] = float(list_house_dict[house_code]["realmoney"]) / float(build_area)
        else:
            list_house_dict[house_code]["realmoney"] = "NULL"
            list_house_dict[house_code]["dealdate"] = "NULL"
            list_house_dict[house_code]["sold_interval"] = "NULL"
            list_house_dict[house_code]["sold_avg"] = "NULL"

        today = datetime.now()
        timedelta = today - create_time
        list_house_dict["list_interval"] = timedelta
        list_house_dict["list_avg"] = float(total_prices) / float(build_area)
        if house_code in showing_dict:
            list_house_dict["showing"] = showing_dict[house_code]["showing"]
        else:
            list_house_dict["showing"] = 0
        
    return list_house_dict

# 成交房源的汇总信息
def deal_house_this_week():
    file_obj = open(conf.deal_house, 'r')
    sold_house_dict = {}
    for line in file_obj:
        tmp = line.rstrip("\n").split("\t")
        house_code = tmp[0]
        deal_time = tmp[2]
        realmoney = tmp[3]
        if realmoney == "NULL":
            continue
        if house_code not in sold_house_dict:
            sold_house_dict[house_code] = {}

        sold_house_dict[house_code]["realmoney"] = realmoney
        sold_house_dict[house_code]["deal_time"] = deal_time
    return sold_house_dict

def similar_house_this_week(house_code, house_dict, sold_similar_list, list_similar_list):
    result_dict = {}
    #today = time.strftime("%Y%m%d")
    today = datetime.now().strftime("%Y%m%d")
    key = house_code + today
    result_dict[key] = {}
    result_dict[key]["sold_similar"] = {}
    result_dict[key]["sold_similar"]["deal_num"] = 0
    result_dict[key]["sold_similar"]["avg_showing"] = 0
    result_dict[key]["sold_similar"]["avg_deal_day"] = 0
    result_dict[key]["sold_similar"]["similar_house"] = []


    result_dict[key]["list_similar"] = {}
    result_dict[key]["list_similar"]["deal_num"] = 0
    result_dict[key]["list_similar"]["avg_showing"] = 0
    result_dict[key]["list_similar"]["avg_deal_day"] = 0
    result_dict[key]["list_similar"]["similar_house"] = []
    for house in sold_similar_list:
        if house in sold_house_dict:
            result_dict[key]["sold_similar"]["deal_num"] += 1
            result_dict[key]["sold_similar"]["avg_showing"] += house_dict[house]["showing"]
            result_dict[key]["sold_similar"]["avg_deal_day"] += house_dict[house]["sold_interval"]
            result_dict[key]["sold_similar"].append(house_dict[house])
        result_dict[key]["sold_similar"]["avg_showing"] = result_dict[key]["sold_similar"]["avg_showing"] / result_dict[key]["sold_similar"]["deal_num"]
        result_dict[key]["sold_similar"]["avg_deal_day"] = result_dict[key]["sold_similar"]["avg_deal_day"] / result_dict[key]["sold_similar"]["deal_num"]

    for house in list_similar_list:
        if house not in sold_house_dict:
            result_dict[key]["list_similar"]["deal_num"] += 1
            result_dict[key]["list_similar"]["avg_showing"] += house_dict[house]["showing"]
            result_dict[key]["list_similar"]["avg_deal_day"] += house_dict[house]["list_interval"]
            result_dict[key]["list_similar"].append(house_dict[house])
            result_dict[key]["list_similar"]["avg_showing"] = result_dict[key]["list_similar"]["avg_showing"] / result_dict[key]["list_similar"]["deal_num"]
            result_dict[key]["list_similar"]["avg_deal_day"] = result_dict[key]["list_similar"]["avg_deal_day"] / result_dict[key]["list_similar"]["deal_num"]
    return result_dict

def weekly_report(house_dict, sold_list):
    file_obj = open(conf.list_house, 'r')
    output_obj = open(conf.output, 'w')
    for line in file_obj:
        tmp = line.rstrip("\n").split("\t")
        house_code = tmp[0]
        similar_list = get_similar_house_list(house_code)
        sold_similar_list = []
        list_similar_list = []
        for house in similar_list:
            if house in sold_list:
                sold_similar_list.append(house)
            else:
                list_similar_list.append(house)
                print "can not find this house"
        result_dict = similar_house_this_week(house_code, house_dict, sold_similar_list, list_similar_list)
        output_obj.write(json.dumps(result_dict))

if __name__ == "__main__":
    showing_dict = load_showing_data()
    sold_house_dict = deal_house_this_week()
    house_dict = list_house_this_week(showing_dict, sold_house_dict)
    weekly_report(house_dict, sold_house_dict)


