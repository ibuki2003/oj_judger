import pymysql.cursors
import configparser
from pathlib import Path
import subprocess
import traceback
from time import time
import json

def judge(subid,judging_list):
    print('Start #', subid, flush=True)
    cfg=configparser.ConfigParser()
    cfg.read('./config.ini', 'UTF-8')

    tl = cfg.getint('limit', 'time')
    ol =  cfg.getint('limit', 'output')
    
    
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

        # get Submission Data
        cursor.execute('SELECT * FROM `submissions` WHERE id=%s',(subid,))
        row=cursor.fetchone()

        # get Lang Data
        cursor.execute('SELECT * FROM `langs` WHERE id=%s',(row['lang'],))
        langinfo=cursor.fetchone()

        datadir = Path(cfg.get('oj', 'datadir'))


        s=submission(row, langinfo, datadir)
        result=s.judge(tl,ol)
        cursor.execute('update submissions set status=%s,point=%s where id=%s', result)
    judging_list.remove(subid)
    print('Done  #', subid, ':', result[0], flush=True)
    if len(judging_list)==0:
        print('Queue Empty.')
    return


class submission:
    def __init__(self, datas, langinfo, datadir):
        self.id=datas['id']
        self.problem=datas['problem']
        self.time=datas['time']

        self.submission_path = datadir/'submissions'/str(self.id)
        self.problem_path = datadir/'problems'/str(self.problem)

        self.execcmd=langinfo['exec'].replace('{path}',str(self.submission_path)).split()

        self.compile_required=langinfo['compile'] is not None
        if(self.compile_required):
            self.compilecmd=langinfo['compile'].replace('{path}',str(self.submission_path)).split()
    
    def compile(self):
        if not self.compile_required: # no compile
            return True
        p = subprocess.run(self.compilecmd, stderr=subprocess.PIPE)
        if(p.returncode!=0): # Compilation failed
            logfile=open(str(self.submission_path/'judge_log.txt'),'wb')
            logfile.write(p.stderr)
            logfile.close()
            return False
        return True
    
    def judge_one(self, testcase, timelimit, outputlimit):
        with open(str(testcase), 'r') as input_file:
            try:
                starttime=time()
                p = subprocess.Popen(self.execcmd, stdin=input_file, stdout=subprocess.PIPE)
                out = p.communicate(timeout=timelimit)[0]
                exectime=int((time()-starttime)*1000)
            except subprocess.TimeoutExpired:
                p.kill()
                return ("TLE",None)
            except subprocess.CalledProcessError:
                return ("RE",None)
            
            if len(out)>outputlimit*1048576:
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

    def judge(self, timelimit, outputlimit):
        if self.compile()==False:
            return ('CE',0,self.id)

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
                ret, exectime=self.judge_one(testcase, timelimit, outputlimit)
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
            return ('IE',point,self.id)
        else:
            with open(str(self.submission_path/'judge_log.json'),'w') as logfile:
                logfile.write(json.dumps(result_data))
            if stats['RE']:
                return ('RE',point,self.id)
            elif stats['OLE']:
                return ('OLE',point,self.id)
            elif stats['TLE']:
                return ('TLE',point,self.id)
            elif stats['WA']:
                return ('WA',point,self.id)
            else:
                return ('AC',point,self.id)


if __name__ == "__main__":
    alist=[]
    judge(1,alist)
