# coding:utf8

import os
import sys
from datetime import datetime
from datetime import timedelta

delta = int(sys.argv[1])
print delta
# last sunday
time_delta = 1 + 7 * delta
time_interval = 8 + 7 * delta
now = datetime.now()
day = datetime.strftime((now - timedelta(days=time_delta)), "%Y%m%d")
print day
last_week = datetime.strftime((now - timedelta(days=time_interval)), "%Y%m%d")

# base on today, minus two weeks, means save two week data
redis_save_window = datetime.strftime((now - timedelta(days=time_delta + 28)), "%Y%m%d")
deal_house = "data/" + day + "/" + "house_deal.dat"
list_house = "data/" + day + "/" + "house_onsale.dat"
showing_file = "data/" + day + "/" + "house_hot.dat"  # 带看量
showing_base = "data/" + last_week + "/showing_add.dat"
showing_add = "data/" + day + "/" + "showing_add.dat"  # 和showing_file区别在哪?

house_on_sale_last_week = "data/" + day + "/" + "house_onsale_last_week.dat"
output_dir = "result/" + day
if not os.path.exists(output_dir):
    os.mkdir(output_dir)
output = output_dir + "/" + "output.dat"
redis_conf = "conf/redis_client.conf"
index_file = "data/redis_index.dat"
error_file = "log/error_file.txt"
base_hot = "data/house_hot_base.dat"

