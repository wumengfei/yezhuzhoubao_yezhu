./get_hive_data.sh 0 0

#only run for one time
python bin/get_redis_index.py 0
python bin/deal_data.py 0
python bin/dump_data_to_redis.py 0
