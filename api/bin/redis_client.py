#!/usr/bin/env python
#-*- coding: gb18030 -*-
"""
/**
 * @file redias_client.py
 * @author zhangkun03(@lianjia.com)
 * @date 2016/04/26 15:15:20
 * @brief
 *
 **/
 """
import os
import sys
import urllib2
import re
import time
import hashlib
import json
import ConfigParser
from Moniter import MyLog
import redis

class Redias_client(object):
    """
    redias client
    """
    def __init__(self, conf_path):
        cf = ConfigParser.ConfigParser()
        cf.read(conf_path)
        log_file = cf.get('log_info', 'log_file')
        log_name = cf.get('log_info', 'log_name')
        log_level = cf.get('log_info', 'log_level')
        log_wan_reciever = cf.get('log_info', 'log_wan_reciever')
        try:
            self.my_log = MyLog(log_file, log_name, log_level, log_wan_reciever)
        except:
            sys.stderr.write("failed to create MyLog instance!")
            exit(1)

        r_host = cf.get('redias_info', 'redias_host')
        r_port = int(cf.get('redias_info', 'redias_port'))
        r_pwd = cf.get('redias_info', 'redias_pwd')
        r_db = cf.get('redias_info', 'redias_db')
        r_con_timeout = int(cf.get('redias_info', 'redias_con_timeout'))
        r_trans_timeout = int(cf.get('redias_info', 'rediad_trans_timeout'))
        try:
            pool = redis.ConnectionPool(\
            host = r_host, \
            port = r_port, \
            db = r_db, \
            password = r_pwd, \
            socket_timeout = r_trans_timeout, \
            socket_connect_timeout = r_con_timeout)
            self.rc = redis.StrictRedis(connection_pool=pool)
        except:
            self.my_log.error("failed to connect redias server!")
            exit(1)
        self.pipe = self.rc.pipeline()
        if self.pipe == None:
            self.my_log.error("failed to get redis client pipeline!")
            exit(1)

    def __del__(self):
        pass

    def get_keys(self, key_pre):
        keys = self.rc.keys()
        rt_keys = set()
        for item in keys:
            if item.find(key_pre) != -1:
                self.my_log.debug("get_cur_keys: add house key %s" % item)
                rt_keys.add(item)
        return rt_keys

    def erase_kv(self, keys):
        if len(keys) == 0:
            return
        for key in keys:
            self.my_log.debug("erase_kv_from_redias: delete key %s" % key)
            self.rc.config_set('stop-writes-on-bgsave-error', 'no')
            self.rc.delete(key)

    def Zincrby(self, key, key_pre, subkey, amount=1):
        if len(key) == 0 or len(key_pre) == 0:
            self.my_log.debug("invalid keys: [key: %s, key_pre: %s ]" % (key, key_pre))
        else:
            real_key = key_pre + key
            self.rc.config_set('stop-writes-on-bgsave-error', 'no')
            self.rc.zincrby(real_key, subkey, amount)
            self.my_log.debug("increase the key-value score [ key: %s, value: %s score: %d]" % (real_key, subkey, amount))

def usage():
    """
    usage info
    """
    print "python redias_client.py conf_file_path"
