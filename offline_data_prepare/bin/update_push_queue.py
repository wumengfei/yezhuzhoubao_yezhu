from yzd_redias_api_new import *
import sys
sys.path.append('conf')
import conf


yzd_weekly_report_push_key = 'yzd_weekly_report_msg_queue'
new_house_push_key = 'yzd_new_house_msg_queue'
file_list = ['20160702', '20160709', '20160716', '20160723']
rc = Redias_client(conf.redis_conf)

for file in file_list:
    result = "result/" + file + "/" + "output.dat"
    for line in open(result, 'r'):
        tmp_dict = json.loads(line)
        key = tmp_dict.keys()[0]
        print key
        rc.rc.lpush(yzd_weekly_report_push_key, key)
        rc.rc.lrem(new_house_push_key, 0, key)
