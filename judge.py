import sys
import pymysql.cursors
import configparser
from pathlib import Path
import subprocess
import traceback
from time import time
import json
import signal

def judge(subid):
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    print('Start #', subid, flush=True)
    cfg=configparser.ConfigParser()
    cfg.read('./config.ini', 'UTF-8')

    tl = cfg.getint('limit', 'time')
    ol = cfg.getint('limit', 'output')
    compile_tl = cfg.getint('limit', 'compile_time')
    compile_ol = cfg.getint('limit', 'compile_output')
    
    connection = pymysql.connect(
        host    =cfg.get('database', 'host'),
        user    =cfg.get('database', 'user'),
        password=cfg.get('database', 'password'),
        db      =cfg.get('database', 'database'),
        charset =cfg.get('database', 'charset'),
        cursorclass=pymysql.cursors.DictCursor)
    connection.autocommit(True)
    cursor=connection.cursor()

    if cfg.getboolean('sandbox', 'enabled'):
        import sandbox
        sb=sandbox.SandBox(
            cfg.get('sandbox', 'base_dir'),
            cfg.get('sandbox', 'user'),
            cfg.get('sandbox', 'addition_path').split(' '))
        sb.mount()
        timeout_command = cfg.get('sandbox', 'timeout_command').split(' ')
    else:
        sb=None
        timeout_command = None

    with connection, cursor:

        # get Submission Data
        cursor.execute('SELECT * FROM `submissions` WHERE id=%s',(subid,))
        row=cursor.fetchone()

        # get Lang Data
        cursor.execute('SELECT * FROM `langs` WHERE id=%s',(row['lang_id'],))
        langinfo=cursor.fetchone()

        datadir = Path(cfg.get('oj', 'datadir'))


        s=submission(sb, row, langinfo, datadir, timeout_command)
        result=s.judge(tl,ol,compile_tl,compile_ol)
        cursor.execute('update submissions set status=%s,point=%s,exec_time=%s where id=%s', result)
    if sb is not None:
        sb.umount()
    print('Done  #', subid, ':', result[0], flush=True)
    return


class submission:
    def __init__(self, sandbox, datas, langinfo, datadir, timeout_command):
        self.id=datas['id']
        self.problem=datas['problem_id']
        self.time=datas['time']

        self.submission_path = datadir/'submissions'/str(self.id)
        self.problem_path = datadir/'problems'/str(self.problem)
        self.timeout_command = timeout_command

        self.sandbox_enabled = (sandbox is not None)
        if sandbox is not None:
            self.sandbox=sandbox
            with open(str(self.submission_path/('source.'+langinfo['extension'])),'rb') as source_file:
                sandbox.put_file('/source.'+langinfo['extension'], source_file.read())
            path = './'
        else:
            self.sandbox=subprocess
            path = str(self.submission_path)
        

        self.execcmd=langinfo['exec'].replace('{path}',path).split()

        self.compile_required=langinfo['compile'] is not None
        if(self.compile_required):
            self.compilecmd=langinfo['compile'].replace('{path}',path).split()
    
    def compile(self, timelimit, outputlimit):
        if not self.compile_required: # no compile
            return True
        
        logfile=open(str(self.submission_path/'judge_log.txt'),'wb')
        
        try:
            # disable Ctrl-C for subprocess
            if sys.platform.startswith('win'):
                # https://msdn.microsoft.com/en-us/library/windows/desktop/ms684863(v=vs.85).aspx
                p = self.sandbox.Popen(self.compilecmd, stderr=subprocess.PIPE, creationflags=0x00000200)
            else:
                p = self.sandbox.Popen(self.compilecmd, stderr=subprocess.PIPE, start_new_session=True)
            
            compile_err = p.communicate(timeout=timelimit)[1]
        except subprocess.TimeoutExpired:
            logfile.write(b"Compile time limit exceeded")
            logfile.close()
            return False
        
        if len(compile_err) > outputlimit*1024:
            logfile.write(b"Compile output limit exceeded")
            logfile.close()
            return False
        
        logfile.write(compile_err)
        logfile.close()
            
        return (p.returncode == 0)
    
    def judge_one(self, testcase, timelimit, outputlimit):
        with open(str(testcase), 'r') as input_file:
            try:
                starttime=time()
                additional_command = []
                if self.sandbox_enabled :
                    additional_command += self.timeout_command
                    additional_command.append(str(timelimit+1));
                
                # disable Ctrl-C for subprocess
                if sys.platform.startswith('win'):
                    # https://msdn.microsoft.com/en-us/library/windows/desktop/ms684863(v=vs.85).aspx
                    p = self.sandbox.Popen(additional_command+self.execcmd, stdin=input_file, stdout=subprocess.PIPE,
                        creationflags=0x00000200)
                else:
                    p = self.sandbox.Popen(additional_command+self.execcmd, stdin=input_file, stdout=subprocess.PIPE,
                        start_new_session=True)
                out = p.communicate(timeout=timelimit)[0]
                exectime=int((time()-starttime)*1000)
                if p.returncode!=0:
                    return ("RE", None)
            except subprocess.TimeoutExpired:
                p.kill()
                return ("TLE",None)
            
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

    def judge(self, timelimit, outputlimit, compile_timelimit, compile_outputlimit):
        # delete before files
        for file in [
                self.submission_path/'judge_log.txt',
                self.submission_path/'judge_log.json']:
            if file.exists():
                file.unlink()
        if self.compile(compile_timelimit, compile_outputlimit)==False:
            return ('CE',0,None,self.id)

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

            exectime_max = 0
            for testcase in testcaselist: # judge All
                ret, exectime=self.judge_one(testcase, timelimit, outputlimit)
                if exectime is not None :
                    exectime_max = max(exectime_max, exectime)
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
            return ('IE',point,None,self.id)
        else:
            with open(str(self.submission_path/'judge_log.json'),'w') as logfile:
                logfile.write(json.dumps(result_data))
            if stats['RE']:
                return ('RE',point,None,self.id)
            elif stats['OLE']:
                return ('OLE',point,None,self.id)
            elif stats['TLE']:
                return ('TLE',point,None,self.id)
            elif stats['WA']:
                return ('WA',point,exectime_max,self.id)
            else:
                return ('AC',point,exectime_max,self.id)

