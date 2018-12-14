# -*- coding: utf-8 -*-
import pymysql.cursors
from time import sleep,time
import sys
import signal
from multiprocessing import Pool, Array, Manager
import configparser
import judge

def init():
    global cfg,connection,cursor,pool
    signal.signal(signal.SIGINT, terminate)
    cfg=configparser.ConfigParser()
    cfg.read('./config.ini', 'UTF-8')

    connection = pymysql.connect(
        host    =cfg.get('database', 'host'),
        user    =cfg.get('database', 'user'),
        password=cfg.get('database', 'password'),
        db      =cfg.get('database', 'database'),
        charset =cfg.get('database', 'charset'),
        cursorclass=pymysql.cursors.DictCursor)
    connection.autocommit(True)
    cursor=connection.cursor()
    pool = Pool(processes=cfg.getint('limit', 'thread'))

def main():
    global pool,cursor
    with Manager() as manager:
        judging=manager.list()
        while True: # Main loop
            sql = 'select id from `submissions` WHERE status in ("WJ","WR")'
            cursor.execute(sql)
            for row in cursor.fetchall():
                i=row['id']
                if i not in judging:
                    print('start:',i)
                    judging.append(i)
                    pool.apply_async(judge.judge, args=(i,judging))
            sleep(1)

def terminate(signal, frame):
    global connection,cursor
    print('stopping')
    pool.close()
    pool.join()

    cursor.close()
    connection.close()
    sys.exit(0)

if __name__ == "__main__":
    init()
    main()
