# -*- coding: utf-8 -*-
import pymysql.cursors
from time import sleep,time
import sys
import signal
from multiprocessing import Pool, Array, Manager
import configparser
import judge

def main():
    global connection,cursor,pool
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
    
    multi_enabled=(cfg.getint('limit', 'thread')!=-1)

    if multi_enabled:
        pool = Pool(processes=cfg.getint('limit', 'thread'))

    with Manager() as manager:
        if multi_enabled:
            judging=manager.list()
        while True: # Main loop
            sql = 'select id from `submissions` WHERE status in ("WJ","WR")'
            cursor.execute(sql)
            for row in cursor.fetchall():
                i=row['id']
                if multi_enabled:
                    if i not in judging:
                        judging.append(i)
                        pool.apply_async(judge.judge, args=(i,judging))
                else:
                    judge.judge(i,None)
            sleep(1)

def terminate(signal, frame):
    global connection,cursor,pool
    print('stopping')
    pool.close()
    pool.join()

    cursor.close()
    connection.close()
    sys.exit(0)

if __name__ == "__main__":
    main()
