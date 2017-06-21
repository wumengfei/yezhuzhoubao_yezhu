# -*- coding: utf-8 -*-
import sys
sys.path.append('conf')
import conf
import urllib
from datetime import *
from datetime import time
from datetime import timedelta
import json
import traceback
import pdb
import redis_client
from yzd_redias_api_new import *
from Moniter import MyLog

rc = Redias_client(conf.redis_conf)
err_f = open(conf.error_file,'a')
cf = ConfigParser.ConfigParser()
cf.read(conf.redis_conf)
log_file = cf.get('log_info', 'log_file')
log_name = cf.get('log_info', 'log_name')
log_level = cf.get('log_info', 'log_level')
log_wan_reciever = cf.get('log_info', 'log_wan_reciever')
today = datetime.now() - timedelta(days = conf.time_delta)
map_dict = {}

try:
   my_log = MyLog(log_file, log_name, log_level, log_wan_reciever)
except:
   sys.stderr.write("failed to create MyLog instance!")
   exit(1)

#挂牌房源上周的挂牌价
def list_price_last_week():
    file_obj = open(conf.house_on_sale_last_week, 'r')
    list_price_last_week = {}
    for line in file_obj:
        tmp = line.rstrip("\n").split("\t")
        house_code = tmp[0]
        if house_code in map_dict:
            house_code = map_dict[house_code]
        total_prices = tmp[1]
        if house_code not in list_price_last_week:
            list_price_last_week[house_code] = total_prices
    return list_price_last_week


# 所有房源近一周的带看次数
def load_showing_data():
    file_obj_1 = conf.showing_base
    file_obj_2 = conf.showing_file
    showing_add = conf.showing_add
    showing_dict = {}
    if os.path.isfile(showing_add):
        file_obj = open(showing_add, 'r')
        for line in file_obj:
            tmp = line.rstrip("\n").split("\t")
            house_code = tmp[0]
            if house_code in map_dict:
                house_code = map_dict[house_code]
            showing = float(tmp[1])
            if house_code not in showing_dict:
                showing_dict[house_code] = showing

        return showing_dict

    for line in open(file_obj_1, 'r'):
        tmp = line.rstrip("\n").split("\t")
        house_code = tmp[0]
        if house_code in map_dict:
            house_code = map_dict[house_code]
        showing = float(tmp[1])
        if house_code not in showing_dict:
            showing_dict[house_code] = showing

    for line in open(file_obj_2, 'r'):
        tmp = line.rstrip("\n").split("\t")
        house_code = tmp[0]
        showing = float(tmp[1])
        if house_code in map_dict:
            house_code = map_dict[house_code]
        if house_code in showing_dict:
            showing_dict[house_code] += showing
        else:
            showing_dict[house_code] = showing

    for house in showing_dict:
        str_line = house + "\t" + showing_dict[house] + "\n"
        showing_add.write(str_line)
    return showing_dict

# 所有挂牌房源的带看次数,以及基本信息
def list_house_this_week(showing_dict, sold_dict):
    file_obj = open(conf.list_house, 'r')
    list_house_dict = {}
    my_log.debug("start load list price last week")
    list_price_last_week_dict = list_price_last_week()
    my_log.debug("load list price last week")

    for line in file_obj:
        tmp = line.rstrip("\n").split("\t")
        house_code = tmp[0]
        if house_code in map_dict:
            house_code = map_dict[house_code]
        if house_code not in list_house_dict:
            list_house_dict[house_code] = {}

        build_area = tmp[2]
        total_prices = tmp[1]
        create_time = tmp[4]
        create_time_tmp = datetime.strptime(create_time,'%Y-%m-%d %H:%M:%S')
        if build_area == "NULL" or total_prices == "NULL" or float(build_area) == 0:
            continue
        
        if house_code in list_price_last_week_dict:
            last_list_price = list_price_last_week_dict[house_code]
            list_house_dict[house_code]["list_price_last_week"] = \
                           last_list_price
            if float(total_prices) > float(last_list_price):
                list_house_dict[house_code]["list_price_qushi"] = "rise"
            else:
                list_house_dict[house_code]["list_price_qushi"] = "down"
        else:
            list_house_dict[house_code]["list_price_last_week"] = "NULL"
            list_house_dict[house_code]["list_price_qushi"] = "NULL"

        list_house_dict[house_code]["build_area"] = build_area
        list_house_dict[house_code]["total_prices"] = total_prices
        list_house_dict[house_code]["create_time"] = create_time
        if house_code in sold_dict:
            list_house_dict[house_code]["realmoney"] = sold_dict[house_code]["realmoney"]
            list_house_dict[house_code]["dealdate"] = sold_dict[house_code]["deal_time"]
            time_tmp = list_house_dict[house_code]["dealdate"]
            deal_date = datetime.strptime(time_tmp, "%Y%m%d")
            list_house_dict[house_code]["sold_interval"] = \
                int((deal_date - create_time_tmp).days)
            list_house_dict[house_code]["sold_avg"] = float(list_house_dict[house_code]["realmoney"]) / float(build_area)
            list_house_dict[house_code]["list_interval"] = list_house_dict[house_code]["sold_interval"]
        else:
            # list house
            list_house_dict[house_code]["realmoney"] = "NULL"
            list_house_dict[house_code]["dealdate"] = "NULL"
            list_house_dict[house_code]["sold_interval"] = "NULL"
            list_house_dict[house_code]["sold_avg"] = "NULL"
            today = datetime.now() - timedelta(days = conf.time_delta)
            time_delta = today - create_time_tmp
            list_house_dict[house_code]["list_interval"] = int(time_delta.days)
        list_house_dict[house_code]["list_avg"] = float(total_prices) / float(build_area)
        if house_code in showing_dict:
            list_house_dict[house_code]["showing"] = showing_dict[house_code]
        else:
            list_house_dict[house_code]["showing"] = 0
        
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
        if house_code in map_dict:
            house_code = map_dict[house_code]
        if house_code not in sold_house_dict:
            sold_house_dict[house_code] = {}

        sold_house_dict[house_code]["realmoney"] = realmoney
        sold_house_dict[house_code]["deal_time"] = deal_time
    return sold_house_dict

def similar_house_this_week(house_code, house_dict, sold_similar_list, list_similar_list):
    result_dict = {}
    #today = time.strftime("%Y%m%d")
    time_delta = conf.time_delta + 1
    today = (datetime.now() - timedelta(days = time_delta)).strftime("%Y%m%d")
    key = house_code + "-" + today
    result_dict[key] = {}
    if len(sold_similar_list) > 0:
        result_dict[key]["sold_similar"] = []
        try:
            for house in sold_similar_list:
                build_size = house_dict[house]["build_area"]
                showing = house_dict[house]["showing"]
                deal_interval = house_dict[house]["sold_interval"]
                result_dict[key]["sold_similar"].append((house, \
                   build_size, showing, deal_interval))
        except Exception, e:
            traceback.print_exc(file = err_f)

    rise_tmp = 0
    down_tmp = 0
    if len(list_similar_list) > 0:
        result_dict[key]["list_similar"] = []
        try:
            for house in list_similar_list:
                build_size = house_dict[house]["build_area"]
                list_interval = house_dict[house]["list_interval"]
                list_price_qushi = house_dict[house]["list_price_qushi"]
                showing = house_dict[house]["showing"]
                list_price = house_dict[house]["total_prices"]
                result_dict[key]["list_similar"].append((house, build_size, \
                    list_interval, list_price_qushi, showing, list_price))
        
        except Exception, e:
            traceback.print_exc(file = err_f)

    return result_dict

def weekly_report(house_dict, sold_list):
    file_obj = open(conf.list_house, 'r')
    output_obj = open(conf.output, 'w')
    my_log.debug("start load redis")
    index_dict = load_redis()
    my_log.debug("load redis")
    try:
        for line in file_obj:
            tmp = line.rstrip("\n").split("\t")
            house_code = tmp[0]
            if house_code in map_dict:
                house_code = map_dict[house_code]
            similar_list = get_similar_house(index_dict, house_code)
            my_log.debug("get_similar_list")
            sold_similar_list = []
            list_similar_list = []
            for house in similar_list:
                if house in sold_list:
                    sold_similar_list.append(house)
                elif house in house_dict:
                    list_similar_list.append(house)
                else:
                    continue
            #print "list_similar:", list_similar_list
            #print "sold_similar:", sold_similar_list
            my_log.debug("start to get result")
            result_dict = similar_house_this_week(house_code, house_dict, \
                sold_similar_list, list_similar_list)
            my_log.debug("get result")
            #print result_dict
            output_obj.write(json.dumps(result_dict))
            my_log.debug("write file")
            output_obj.write("\n")
    except Exception, e:
        traceback.print_exc(file = err_f)

def load_map_data():
    map_file = conf.map_data
    map_dict = {}
    for line in open(map_file, 'r'):
        tmp = line.rstrip("\n").split("\t")
        house_code = tmp[0]
        new_code = tmp[1]
        if house_code not in map_dict:
            map_dict[house_code] = new_code
    return map_dict
    
if __name__ == "__main__":
    try:
       my_log.debug("start load showing data")
       map_dict = load_map_data()
       showing_dict = load_showing_data()
       my_log.debug("load showing data")
       my_log.debug("start load sold_dict")
       sold_house_dict = deal_house_this_week()
       my_log.debug("load sold_house_dict")
       my_log.debug("load list_house_dict")
       house_dict = list_house_this_week(showing_dict, sold_house_dict)
       my_log.debug("load house_dict")
       weekly_report(house_dict, sold_house_dict)
    except Exception, e:
       traceback.print_exc(file = err_f)


