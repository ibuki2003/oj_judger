from submission import submission
import configparser
import signal
import pymysql
import os
import sys

def judge(subid):
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    print('Start #', subid, flush=True)
    cfg=configparser.ConfigParser()
    cfg.read('./config.ini', 'UTF-8')

    if cfg.getboolean('sandbox', 'enabled'):
        if os.getgid() != 0:
            cfg.set('sandbox', 'enabled', 'false')
            sys.stderr.write('using sandox requires root privileges. set disabled automatically.')
            sys.stderr.flush()
            print(cfg.getboolean('sandbox', 'enabled'))

    connection = pymysql.connect(
        host    =cfg.get('database', 'host'),
        user    =cfg.get('database', 'user'),
        password=cfg.get('database', 'password'),
        db      =cfg.get('database', 'database'),
        charset =cfg.get('database', 'charset'),
        cursorclass=pymysql.cursors.DictCursor)
    connection.autocommit(True)
    cursor=connection.cursor()

    with connection, cursor:
        s=submission(subid, cfg, cursor)
        result=s.judge()
        cursor.execute('update submissions set status=%s,point=%s,exec_time=%s where id=%s', result + (subid, ))
    print('Done  #', subid, ':', result[0], flush=True)
    return
