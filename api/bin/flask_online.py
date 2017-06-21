from __future__ import division
from flask import Flask
from flask import request
from redis_client import Redias_client
import json
from datetime import datetime
from datetime import timedelta
import sys
sys.path.append('conf')
import conf
import ConfigParser
from Moniter import MyLog
import traceback
from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop

app = Flask(__name__)

rc = Redias_client(conf.redis_conf)
err_f = open(conf.error_file,'w')
cf = ConfigParser.ConfigParser()
cf.read(conf.redis_conf)
log_file = cf.get('log_info', 'log_file')
log_name = cf.get('log_info', 'log_name')
log_level = cf.get('log_info', 'log_level')
log_wan_reciever = cf.get('log_info', 'log_wan_reciever')
#today = datetime.now() - timedelta(days = conf.time_delta - 1)

try:
    my_log = MyLog(log_file, log_name, log_level, log_wan_reciever)
except:
    sys.stderr.write("failed to create MyLog instance!")
    exit(1)

@app.route('/yzd_weekly_report_similar_sold_list')
def yzd_weekly_similar_sold_list():
    args_dict = request.args.to_dict()
    if 'house_date' in args_dict:
        house_date = str(args_dict['house_date'])
    else:
        house_date = ''
    #house_date = '103100245400_20160616'
    info_dict = {}
    info_dict['error_code'] = 0
    info_dict['error_msg'] = ''

    redis_similar_key = "yzd_weekly_report_" + house_date
    try:
        similar_house = json.loads(rc.rc.get(redis_similar_key))
        get_similar_sold_list(info_dict, similar_house[house_date])
    except Exception, e:
        # info_dict['error_code'] = 1
        info_dict['error_msg'] = info_dict['error_msg'].join('can not get similar information;')
        info_dict["data"] = []
        traceback.print_exc(file = err_f)
    return json.dumps(info_dict)

@app.route('/yzd_weekly_report_similar_onsale_list')
def yzd_weekly_similar_onsale_list():
    args_dict = request.args.to_dict()
    if 'house_date' in args_dict:
        house_date = str(args_dict['house_date'])
    else:
        house_date = ''
    #house_date = '103100245400_20160616'

    info_dict = {}
    info_dict['error_code'] = 0
    info_dict['error_info'] = ''
    redis_similar_key = "yzd_weekly_report_" + house_date
    try:
        similar_house = json.loads(rc.rc.get(redis_similar_key))
        get_similar_onsale_list(info_dict, similar_house[house_date])
    except Exception, e:
        #info_dict['error_code'] = 1
        info_dict['error_msg'] = info_dict['error_msg'].join\
             ('can not get similar information;')
        info_dict['data'] = []
        traceback.print_exc(file = err_f)
    return json.dumps(info_dict)

@app.route('/yzd_weekly_report_homepage')
def get_yzd_weekly_report():
    args_dict = request.args.to_dict()
    if 'house_date' in args_dict:
        house_date = str(args_dict['house_date'])
    else:
        house_date = ''
    #house_date = '101092295963_20160627'
    redis_similar_key = "yzd_weekly_report_" + house_date
    house_code = house_date.split("_")[0]
    report_date = house_date.split("_")[1]
   
    similar_house = {}
    hot_dict = {}
    price_dict = {}

    info_dict = {}
    info_dict['error_code'] = 0
    info_dict['error_msg'] = ''
    info_dict['data'] = {}
    
    # get similar sold house and similar on_sale house
    try:
        similar_house = json.loads(rc.rc.get(redis_similar_key))
        get_similar_info_homepage(info_dict, similar_house[house_date])
    except Exception, e:
        # info_dict['error_code'] = 1
        info_dict['error_msg'] += "can not get similar information;"
        info_dict["data"]["similar_house_deal_num "] = 0
        info_dict["data"]["similar_house_deal_avg_showing_time"] = 0
        info_dict["data"]["similar_house_deal_avg_deal_circle"] = 0
        info_dict["data"]["similar_house_deal_list"] = []
        info_dict["data"]["similar_house_list_new_add_num"] = 0
        info_dict["data"]["similar_house_list_all_on_sale"] = 0
        info_dict["data"]["similar_house_list_avg_list_price"] = 0
        info_dict["data"]["similar_house_list_list"] = []
        info_dict["data"]["similar_house_price_up_num"] = 0
        info_dict["data"]["similar_house_price_up_percent"] = 0
        info_dict["data"]["similar_house_price_down_num"] = 0
        info_dict["data"]["similar_house_price_down_percent"] = 0
        traceback.print_exc(file = err_f)
    
    # get avg price 
    redis_price_key =  "yzd_house_price_" + house_code
    try:
        price_dict = json.loads(rc.rc.get(redis_price_key))
        get_price_info(info_dict, price_dict)
    except Exception, e:
        # info_dict['error_code'] = 1
        info_dict['error_msg'] += "can not get avg price information;"
        info_dict["data"]["similar_house_deal_avg"] = 0
        traceback.print_exc(file = err_f)
    
    # get showing and view information
    redis_hot_key = "yzd_house_hot_" + house_code
    try:
        hot_dict = json.loads(rc.rc.get(redis_hot_key))
        get_hot_info(info_dict, report_date, hot_dict)
    except Exception, e:
        # info_dict['error_code'] = 1
        info_dict['error_msg'] += "can not get hot information;"
        info_dict["data"]["total_view_num_of_this_week"] = 0
        info_dict["data"]["view_list_of_this_week"] = []
        info_dict["data"]["total_view_num_of_last_week"] = 0
        info_dict["data"]["view_list_of_last_week"] = []
        info_dict["data"]["total_showing_num_of_this_week"] = 0
        info_dict["data"]["view_percent"] = 0
        info_dict["data"]["showing_list_of_this_week"] = []
        info_dict["data"]["total_showing_num_of_last_week"] = 0
        info_dict["data"]["showing_list_of_last_week"] = []
        info_dict["data"]["showing_percent"] = 0
        traceback.print_exc(file = err_f)
    return json.dumps(info_dict)

def get_price_info(info_dict,  price_dict):
    similar_house_deal_avg = 0
    similar_house_deal_avg = price_dict["avg_onsale_price"]
    money = similar_house_deal_avg[0]
    size = similar_house_deal_avg[1]
    if size != 0:
        info_dict["data"]["similar_house_deal_avg"] = money / size
    else:
        info_dict["data"]["similar_house_deal_avg"] = 0

def get_hot_info(info_dict, report_date, hot_dict):
    tmp_hot_dict = hot_dict["hot_hist"]
    #print tmp_hot_dict.keys()
    report_date_tmp = datetime.strptime(report_date, "%Y%m%d")
    total_view_num_of_this_week = 0
    view_list_of_this_week = [0, 0 ,0, 0, 0, 0, 0]
    total_view_num_of_last_week = 0
    view_list_of_last_week = [0, 0, 0, 0, 0, 0, 0]

    total_showing_num_of_this_week = 0
    view_percent = 0
    showing_list_of_this_week = [0, 0, 0, 0, 0, 0, 0]
    total_showing_num_of_last_week = 0
    showing_list_of_last_week = [0, 0, 0, 0, 0, 0, 0]
    showing_percent = 0
    
    for item in tmp_hot_dict:
        item_tmp = datetime.strptime(item, "%Y%m%d")
        time_interval = (report_date_tmp - item_tmp).days

        if time_interval > 14 or time_interval <= 0:
            continue
        elif time_interval > 7 and time_interval <= 14:
            index = time_interval - 8
            total_view_num_of_last_week += tmp_hot_dict[item][0]
            view_list_of_last_week[index] = tmp_hot_dict[item][0]
            total_showing_num_of_last_week += tmp_hot_dict[item][2]
            showing_list_of_last_week[index] = tmp_hot_dict[item][2]
        else:
            index = time_interval - 1
            total_view_num_of_this_week += tmp_hot_dict[item][0]
            view_list_of_this_week[index] = tmp_hot_dict[item][0]
            total_showing_num_of_this_week += tmp_hot_dict[item][2]
            showing_list_of_this_week[index] = tmp_hot_dict[item][2]
    
    if total_view_num_of_last_week != 0:
        view_percent = (total_view_num_of_this_week - total_view_num_of_last_week)/\
                        total_view_num_of_last_week

    if total_showing_num_of_last_week != 0:
        showing_percent = (total_showing_num_of_this_week - \
            total_showing_num_of_last_week) / total_showing_num_of_last_week
    
    info_dict["data"]["total_view_num_of_this_week"] = total_view_num_of_this_week
    info_dict["data"]["view_list_of_this_week"] = view_list_of_this_week
    info_dict["data"]["total_view_num_of_last_week"] = total_view_num_of_last_week
    info_dict["data"]["view_list_of_last_week"] = view_list_of_last_week
    info_dict["data"]["total_showing_num_of_this_week"] = total_showing_num_of_this_week
    info_dict["data"]["view_percent"] = view_percent
    info_dict["data"]["showing_list_of_this_week"] = showing_list_of_this_week
    info_dict["data"]["total_showing_num_of_last_week"] = total_showing_num_of_last_week
    info_dict["data"]["showing_list_of_last_week"] = showing_list_of_last_week
    info_dict["data"]["showing_percent"] = showing_percent

def get_similar_info_homepage(result_dict, similar_dict):
    similar_house_deal_num = 0
    similar_house_deal_avg_showing_time = 0
    similar_house_deal_avg_deal_circle = 0
    similar_house_deal_list = []
    similar_house_list_new_add_num = 0
    similar_house_list_all_on_sale = 0
    similar_house_list_avg_list_price = 0
    similar_house_list_list = []

    similar_house_price_up_num = 0
    similar_house_price_up_percent = 0
    similar_house_price_down_num = 0
    similar_house_price_down_percent = 0
    
    if similar_dict.has_key("sold_similar"):
        similar_house_deal_list = similar_dict["sold_similar"]
        similar_house_deal_num = len(similar_house_deal_list)
        showing_tmp = 0
        deal_tmp = 0
        for item in similar_dict["sold_similar"]:
            showing_tmp += float(item[2])
            deal_tmp += float(item[3])
        if (similar_house_deal_num > 0):
            similar_house_deal_avg_showing_time = showing_tmp / similar_house_deal_num
            similar_house_deal_avg_deal_circle = deal_tmp / similar_house_deal_num

    if similar_dict.has_key("list_similar"):
        similar_house_list_list = similar_dict["list_similar"]
        similar_house_list_all_on_sale = len(similar_house_list_list)
        for item in similar_house_list_list:
            list_interval = item[2]
            if list_interval <= 7:
                similar_house_list_new_add_num += 1
            build_size = item[1]
            total_price = item[5]
            similar_house_list_avg_list_price += float(total_price) / float(build_size)
            qushi = item[3]
            if qushi == "rise":
                similar_house_price_up_num += 1
            elif qushi == "down":
                similar_house_price_down_num += 1
        
        if similar_house_list_all_on_sale > 0:
            similar_house_list_avg_list_price = similar_house_list_avg_list_price\
                                            / similar_house_list_all_on_sale
            
            similar_house_price_up_percent = similar_house_price_up_num / \
                                         similar_house_list_all_on_sale
            
            similar_house_price_down_percent = similar_house_price_down_num /\
                                         similar_house_list_all_on_sale

    result_dict["data"]["similar_house_deal_num"] = similar_house_deal_num 
    result_dict["data"]["similar_house_deal_avg_showing_time"] = \
        similar_house_deal_avg_showing_time
    result_dict["data"]["similar_house_deal_avg_deal_circle"] = \
        similar_house_deal_avg_deal_circle
    
    if similar_house_deal_num > conf.homepage_show:
        result_dict["data"]["similar_house_deal_list"] = \
            similar_house_deal_list[0:conf.homepage_show]
    else:
        result_dict["data"]["similar_house_deal_list"] = \
            similar_house_deal_list

    result_dict["data"]["similar_house_list_new_add_num"] = similar_house_list_new_add_num
    result_dict["data"]["similar_house_list_all_on_sale"] = similar_house_list_all_on_sale
    result_dict["data"]["similar_house_list_avg_list_price"] = \
        similar_house_list_avg_list_price

    if similar_house_list_all_on_sale > conf.homepage_show:
        result_dict["data"]["similar_house_list_list"] = \
            similar_house_list_list[0:conf.homepage_show]
    else:
        result_dict["data"]["similar_house_list_list"] = \
            similar_house_list_list

    result_dict["data"]["similar_house_price_up_num"]= similar_house_price_up_num
    result_dict["data"]["similar_house_price_up_percent"] = similar_house_price_up_percent
    result_dict["data"]["similar_house_price_down_num"] = similar_house_price_down_num
    result_dict["data"]["similar_house_price_down_percent"] = \
        similar_house_price_down_percent

def get_similar_sold_list(info_dict, similar_dict):
    similar_house_deal_list = []
    if similar_dict.has_key("sold_similar"):
        similar_house_deal_list = similar_dict["sold_similar"]
    info_dict["data"] = similar_house_deal_list

def get_similar_onsale_list(info_dict, similar_dict):
    similar_house_list_list=[]
    if similar_dict.has_key("list_similar"):
        similar_house_list_list = similar_dict["list_similar"]
    info_dict["data"] = similar_house_list_list

if __name__ == '__main__':
    #get_yzd_weekly_report()
    #yzd_weekly_similar_sold_list()
    #yzd_weekly_similar_onsale_list()
    #app.run(host='0.0.0.0', port=8015)
    http_server = HTTPServer(WSGIContainer(app))
    http_server.bind(8015, '0.0.0.0')
    http_server.start(num_processes=0)
    IOLoop.instance().start()
