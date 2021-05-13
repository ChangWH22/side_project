from queue import Queue
import threading
import datetime
import pyodbc
import requests as rq
import pandas as pd
from bs4 import BeautifulSoup
import crawler.tool as crawler_tool
import time
import random
import os
import json
import re
from crawler.ctee import ctee_GET_NEWS_time_threading
from crawler.anue import anue_GET_NEWS_time_threading
from crawler.rti import rti_GET_NEWS_time_threading
from crawler.money_udn import moneyudn_GET_NEWS_time_threading
from crawler.China_Time import chinatime_GET_NEWS_time_threading
from crawler.setn import setn_GET_NEWS_time_threading
from crawler.cna import cna_GET_NEWS_time_threading
from crawler.tvbs import tvbs_GET_NEWS_time_threading
from crawler.moneyDJ import moneyDJ_GET_NEWS_time_threading


'''
def GET_DATA(decide_time_begin,decide_time_end):
    q1 = Queue()
    q2 = Queue()
    q3 = Queue()
    p1 = threading.Thread(target=ctee_GET_NEWS_time_threading, args=(decide_time_begin, decide_time_end, q1))
    p2 = threading.Thread(target=moneyudn_GET_NEWS_time_threading, args=(decide_time_begin, decide_time_end, q2))
    p3 = threading.Thread(target=chinatime_GET_NEWS_time_threading, args=(decide_time_begin, decide_time_end, q3))
    p1.start()
    p2.start()
    p3.start()
    p1.join()
    p2.join()
    p3.join()
    df1 = q1.get()
    df2 = q2.get()


    df3 = pd.concat([df1, df2])
    return df3
'''



def GET_DATA(decide_time_begin,decide_time_end):
    begin_time = datetime.datetime.today()
    q = Queue()
    threads = []

    for kind in [ctee_GET_NEWS_time_threading,moneyudn_GET_NEWS_time_threading, chinatime_GET_NEWS_time_threading,
                 setn_GET_NEWS_time_threading, anue_GET_NEWS_time_threading, rti_GET_NEWS_time_threading,
                 cna_GET_NEWS_time_threading,tvbs_GET_NEWS_time_threading, moneyDJ_GET_NEWS_time_threading]:
        t = threading.Thread(target=kind, args=(decide_time_begin, decide_time_end, q))
        t.start()
        threads.append(t)

    for thread in threads:
        thread.join()

    result = []
    for _q in range(len(threads)):
        result.append(q.get())

    df = pd.concat([result[i] for i in range(len(result))], ignore_index=True)
    df = df.drop_duplicates(subset=["Title"]).sort_values("Time")
    file_name = "D:/User/Desktop/corpus/news/_concat/" + decide_time_begin + "_" + decide_time_end + "_concat.csv"
    df.to_csv(file_name, encoding="utf-8")
    print("processing time:", datetime.datetime.today() - begin_time)
    df = pd.read_csv(file_name, encoding="utf-8")
    return df



if __name__ == "__main__":
    dt = datetime.datetime.today()
    if len(str(dt.month))<2:
        month = "0" + str(dt.month)
    else:
        month = str(dt.month)

    if dt.hour >= 14:  # 盤中資訊
        if dt.day < 10:
            decide_time_begin = str(dt.year)+month+"0"+str(dt.day) + "1330"
            decide_time_end = str(dt.year)+month+"0"+str(dt.day) + "0830"
        else:
            decide_time_begin = str(dt.year) + month + str(dt.day) + "1330"
            decide_time_end = str(dt.year) + month + str(dt.day) + "0830"
    elif dt.hour >= 8 and dt.hour < 14: #盤外資訊
        delta = datetime.timedelta(days=1)
        yesterday = dt-delta
        if len(str(yesterday.month)) < 2:
            y_month = "0" + str(yesterday.month)
        else:
            y_month = str(yesterday.month)

        if dt.day < 10 and yesterday.day < 10:
            decide_time_begin = str(dt.year)+month+"0"+str(dt.day) + "0830"
            decide_time_end = str(yesterday.year)+y_month+"0"+str(yesterday.day) + "1330"
        elif dt.day < 10 and yesterday.day > 10:
            decide_time_begin = str(dt.year) + month + "0" + str(dt.day) + "0830"
            decide_time_end = str(yesterday.year) + y_month + str(yesterday.day) + "1330"
        elif dt.day >= 10 and yesterday.day < 10:
            decide_time_begin = str(dt.year) + month + str(dt.day) + "0830"
            decide_time_end = str(yesterday.year) + y_month + "0" + str(yesterday.day) + "1330"
        else:
            decide_time_begin = str(dt.year) + month + str(dt.day) + "0830"
            decide_time_end = str(yesterday.year) + y_month + str(yesterday.day) + "1330"

    print("begin time:", decide_time_begin)
    print("end time:  ", decide_time_end)

    df = GET_DATA(decide_time_begin, decide_time_end)
    conn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-OKF0JOA;DATABASE=test')
    cursor = conn.cursor()
    for i in range(0, len(df)):
        try:
            if pd.isnull(df["Body"][i]):
                print("null")
            else:
                cursor.execute("insert into NEWS (Title,Time,Section,Source,Body) values(?,?,?,?,?)",
                               df["Title"][i], df["Time"][i], df["Section"][i],
                               df["Source"][i], df["Body"][i])
        except pyodbc.DataError:
            print(i)

    cursor.commit()

