#/usr/bin/python
# coding:utf-8
#author=zy
#datetime=2018-04-07
#将集群每日运行的spark任务数量写入mysql

import time
import datetime
import requests
import traceback
import MySQLdb
#from datetime import datetime

clusterid='spark'
now_time = int(time.time())
day_time = now_time - now_time % 86400 + time.timezone

now = time.time()
yesterday_timestamp = datetime.fromtimestamp(now-86400.00).strftime('%Y-%m-%d %H:%M:%S')

today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)


def test():

    # 存满足条件的记录
    data = []
    # 进度显示,修理条数
    no = 0

    # 爬虫获取数据
    response = requests.get("http://192.168.3.149:8088/cluster/apps/FINISHED")
    f = response.text.encode().split("\n")

    start = False
    for line in f:
        if "var appsTableData" in line.strip():
            start = True
            continue

        if "]" == line.strip():
            start = False
            continue

        if start:
            no += 1
            print no

            temp = line.strip().split("\"")
            temp_ddd = []
            for i in range(len(temp)):
                if i % 2 != 0:
                    temp_ddd.append(temp[i])

            print no
            if  "SPARK" in temp_ddd[3] and day_time * 1000 - 86400*1000<int(temp_ddd[6])< day_time * 1000 :
                data.append(temp_ddd)


    return  data

def mysql_insert():

        conn=MySQLdb.connect(
            host='192.168.35.102',
            port = 3306,
            user='www',
            passwd='mysql',
            db ='bigdata'
            )
        cur=conn.cursor()
        cur.execute("insert into spark_finished(time,clusterid,nums) VALUES (%s,,%s,%d)",[yesterday_timestamp,clusterid,int(len(mm))])
        #cur.execute("insert into jobsubmit(day,jobsubmitted) VALUES (%s,%s)",[day,sum(jobsubmitted)])
        cur.close()
        conn.commit()
        conn.close()

mm = test()
mysql_insert()

#print "%s spark job num  is %d" %(today - datetime.timedelta(days=1),len(mm))
