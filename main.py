# -*- coding: utf-8 -*-
import pymysql.cursors
from time import sleep,time
import sys
import signal
import multiprocessing
import configparser
import judge

def main():
    global connection,cursor,jobs
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
    
    job_limit=cfg.getint('limit', 'thread')
    multi_enabled=(job_limit!=-1)

    if multi_enabled:
        jobs = {}
    

    while True: # Main loop
        sql = 'select id from `submissions` WHERE status in ("WJ","WR")'
        cursor.execute(sql)
        for row in cursor.fetchall():
            i=row['id']
            if multi_enabled:
                if i not in jobs and (job_limit==0 or len(jobs)<job_limit):
                    jobs[i]=multiprocessing.Process(target=judge.judge, args=(i,))
                    jobs[i].start()
            else:
                judge.judge(i)
        
        if multi_enabled:
            for i in jobs.copy():
                if not jobs[i].is_alive():
                    del jobs[i]

        sleep(1)

def terminate(signal, frame):
    global connection,cursor,jobs
    print('stopping')
    for job in jobs:
        jobs[job].join()
    
    cursor.close()
    connection.close()
    sys.exit(0)

if __name__ == "__main__":
    main()
