# -*- coding: utf-8 -*-
#
# Can Use Special judge.
# Indev.
#
# Made by ibuki2003.
import pymysql.cursors
from time import sleep,time

from pathlib import Path
import subprocess
import traceback
import sys
import signal

import json

import configparser
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

datadir = Path(cfg.get('oj', 'datadir'))

def terminate(signal, frame):
    global connection,cursor
    print('stopping')
    connection.commit()
    cursor.close()
    connection.close()
    sys.exit(0)
signal.signal(signal.SIGINT, terminate)

class submission:
    def __init__(self, datas):
        global cursor
        self.id=datas['id']
        self.problem=datas['problem']
        self.time=datas['time']

        self.submission_path = datadir/'submissions'/str(self.id)
        self.problem_path = datadir/'problems'/str(self.problem)

        cursor.execute('SELECT * FROM `langs` WHERE id=%s',(datas['lang'],))
        langinfo=cursor.fetchone()
        self.execcmd=langinfo['exec'].replace('{path}',str(self.submission_path)).split()

        self.compile_required=langinfo['compile'] is not None
        if(self.compile_required):
            self.compilecmd=langinfo['compile'].replace('{path}',str(self.submission_path)).split()
    
    def compile(self):
        if not self.compile_required: # no compile
            return True
        p = subprocess.run(self.compilecmd, stderr=subprocess.PIPE)
        if(p.returncode!=0): # Compilation failed
            logfile=open('judge_log.txt','wb')
            logfile.write(p.stderr)
            logfile.close()
            return False
        return True
    
    def judge_one(self, testcase):
        with open(str(testcase), 'r') as input_file:
            try:
                starttime=time()
                p = subprocess.Popen(self.execcmd, stdin=input_file, stdout=subprocess.PIPE)
                out = p.communicate(timeout=cfg.getint('limit', 'time'))[0]
                exectime=int((time()-starttime)*1000)
            except subprocess.TimeoutExpired:
                p.kill()
                return ("TLE",None)
            except subprocess.CalledProcessError:
                return ("RE",None)
            
            if len(out)>cfg.getint('limit', 'output')*1048576:
                return ("OLE",None)
            else: # Execute OK
                with open(str(testcase.parents[1]/'out'/testcase.name), 'r') as ansfile:
                    anslist=ansfile.read().split()
                outlist=out.decode('utf-8').split()
                
                if len(outlist)!=len(anslist):
                    return ("WA",exectime)
                for i in range(len(outlist)):
                    if(outlist[i]!=anslist[i]):
                        return ("WA",exectime)
            return ("AC",exectime)

    def judge(self):
        if self.compile()==False:
            self.save('CE',0) # 0点
            return

        try: # Judge Start!
            point=0

            if (self.problem_path/'tcsets.json').exists():
                with open(str(self.problem_path/'tcsets.json'), 'r') as tcsetsfile:
                    tcsets = json.load(tcsetsfile)
                testcaselist=[]
                for tcset in tcsets:
                    testcaselist.extend(tcset['problems'])
                testcaselist=[(self.problem_path/'in'/filename) for filename in set(testcaselist)] # remove duplication, make Path Obj
            else:
                testcaselist = list((self.problem_path/'in').glob('*'))
                testcasenamelist = [file.name for file in testcaselist]
                testcasenamelist.sort()
                
                tcsets=[
                    {
                        "name":"all",
                        "point":100,
                        "problems":testcasenamelist
                    },
                ]
            

            stats={
                'RE': False,
                'TLE': False,
                'OLE': False,
                'WA': False,
            }
            result_data={
                'result':[],
                'tcsets':[]
            }
            problem_results={}

            for testcase in testcaselist: # judge All
                ret, exectime=self.judge_one(testcase)
                if ret in ['RE','OLE','TLE']:
                    stats[ret]=True
                    problem_results[testcase.name]={
                        'status': ret
                    }
                elif ret=='WA':
                    stats[ret]=True
                    problem_results[testcase.name]={
                        'status': ret,
                        'time': exectime
                    }
                else:
                    problem_results[testcase.name]={
                        'status': ret,
                        'time': exectime
                    }
                result_data['result'].append({
                    'name': testcase.name,
                    'status': ret,
                    'time': exectime,
                })
                result_data['result'].sort(key=lambda x: x['name'])

            for tcset in tcsets: # calculate point
                AllAC=True
                for testcase in tcset['problems']:
                    if problem_results[testcase]['status']!='AC':
                        AllAC=False
                        break
                if AllAC:
                    result_data['tcsets'].append({
                        'name': tcset['name'],
                        'problems': tcset['problems'],
                        'perfect': tcset['point'],
                        'got': True
                    })
                    point+=tcset["point"]
                else:
                    result_data['tcsets'].append({
                        'name': tcset['name'],
                        'problems': tcset['problems'],
                        'perfect': tcset['point'],
                        'got': False
                    })

        except:
            print(traceback.format_exc())
            self.save('IE',point)
            return
        else:
            if stats['RE']:
                self.save('RE',point)
            elif stats['OLE']:
                self.save('OLE',point)
            elif stats['TLE']:
                self.save('TLE',point)
            elif stats['WA']:
                self.save('WA',point)
            else:
                self.save('AC',point)

            with open(str(self.submission_path/'judge_log.json'),'w') as logfile:
                logfile.write(json.dumps(result_data))

    def save(self,stat,point):
        global cursor
        sql = 'update submissions set status=%s,point=%s where id=%s'
        cursor.execute(sql, (stat,point,self.id))

def main():
    while True: # Main loop
        sql = 'select * from `submissions` WHERE status in ("WJ","WR")'
        cursor.execute(sql)
        for row in cursor.fetchall():
            print("judging:#",row['id'])
            submission(row).judge()
            connection.commit()
        sleep(1)

if __name__ == "__main__":
    main()
