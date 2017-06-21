#!/usr/bin/env python
#-*- coding: gb18030 -*-
"""
/**
 * @file Moniter.py
 * @author zhangkun03(@lianjia.com)
 * @date 2016/03/15 15:56:10
 * @brief 
 *  
 **/
"""

import os
import sys
import time
import logging

class MyLog(object):
    """
    自定义log类
    """
    def __init__(self, log_file, log_name, level, reciever):
        fh = logging.FileHandler(log_file)
        formatter =  logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        
        my_level = logging.DEBUG
        if level == "debug":
            my_level = logging.DEBUG
        if level == "info":
            my_level = logging.INFO
        if level == "warning":
            my_level = logging.WARNING
        if level == "error":
            my_level = logging.ERROR

        logger = logging.getLogger(log_name)
        logger.setLevel(my_level)
        logger.addHandler(fh)
        self.logger = logger
        self.reciever = reciever

    def debug(self, line):
        """
        debug 模式
        """
        self.logger.debug(line)

    def info(self, line):
        """
        info 模式
        """
        self.logger.info(line)

    def warning(self, line, subject, content):
        """
        warning 模式
        """
        self.logger.warning(line)
        self.send_mail(subject, content, self.reciever)

    def error(self, line):
        """
        error 模式
        """
        self.logger.error(line)

    def send_mail(self, subject, content, reciever):
        """
        send mail
        """
        if '' in (subject, content, reciever):
            self.error("send mail with bad param")
        else:
            send_mail_cmd = "echo -e \"%s\" | mail -s \"%s\" \"%s\"" % (content, subject, reciever)
            ret = os.system(send_mail_cmd)
            if ret != 0:
                self.logger.error("send mail failed")
