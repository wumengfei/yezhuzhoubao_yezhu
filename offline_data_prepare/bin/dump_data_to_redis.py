import json
import os
import redis_client
import sys
sys.path.append("conf")
import conf
from datetime import datetime
from datetime import timedelta

rc = redis_client.Redis_client("conf/redias_client_tmp.conf")

def dump_data_to_redis():
    result = "result/" + conf.day + "/output.dat"
    yzd_weekly_report_push_key = 'yzd_weekly_report_msg_queue'
    print result

    for line in open(result, 'r'):
        tmp_dict = json.loads(line)
        key = tmp_dict.keys()
        rc.rc.lpush(yzd_weekly_report_push_key, key[0])
        hhot_redis_key = 'yzd_weekly_report_' + key[0]
        rc.rc.set(hhot_redis_key, json.dumps(tmp_dict))

def delete_data_from_redis():
    key_list = []
    file_name = "result/" + conf.redis_save_window + "/output.dat"
    if os.path.exists(file_name):
        for line in open(file_name, 'r'):
            print "delete data from redis"
            key = json.loads(line).keys()[0]
            print key
            erase_redis_key = 'yzd_weekly_report_' + key
            key_list.append(erase_redis_key)
        rc.erase_kv(key_list)

if __name__ == "__main__":
    delete_data_from_redis()
    dump_data_to_redis()
