# coding:utf-8
# __author__ = 'user'

import configparser

if __name__=='__main__':
    cf = configparser.ConfigParser()
    cf.add_section("addd")
    cf.set("addd","addd1","addd1的值")
    cf.set("addd","addd2","addd2的值")
    cf.write(open("settings.conf","w"))