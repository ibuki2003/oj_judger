import sys
import os
import pymysql.cursors
import configparser
from pathlib import Path
import subprocess
import traceback
from time import time
import json
import signal

def kill_child_processes(process):
    if sys.platform.startswith('win'):
        # p.kill() doesn't seem to kill the child processes on Windows
        subprocess.run(['TASKKILL', '/F', '/T', '/PID', str(process.pid)], stdout=subprocess.DEVNULL)
    else:
        process.kill()

def compile_multiple_judger(cmd, sandbox, timelimit, outputlimit):
    try:
        # disable Ctrl-C for subprocess
        if sys.platform.startswith('win'):
            # https://msdn.microsoft.com/en-us/library/windows/desktop/ms684863(v=vs.85).aspx
            p = sandbox.Popen(cmd, stderr=subprocess.PIPE, creationflags=0x00000200)
        else:
            p = sandbox.Popen(cmd, stderr=subprocess.PIPE, start_new_session=True)
        
        compile_err = p.communicate(timeout=timelimit)[1]
    except subprocess.TimeoutExpired:
        kill_child_processes(p)
        return False
    
    if p.returncode != 0:
        return False
    
    if len(compile_err) > outputlimit*1024:
        return False
        
    return True
    
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

    with connection, cursor:
        # get Submission Data
        cursor.execute('SELECT * FROM `submissions` WHERE id=%s',(subid,))
        row=cursor.fetchone()

        # get Lang Data
        cursor.execute('SELECT * FROM `langs` WHERE id=%s',(row['lang_id'],))
        langinfo=cursor.fetchone()

        datadir = Path(cfg.get('oj', 'datadir'))
        
        s=submission(row, langinfo, datadir)
        result=s.judge(tl,ol,compile_tl,compile_ol)
        cursor.execute('update submissions set status=%s,point=%s,exec_time=%s where id=%s', result)
    if s.sandbox_enabled:
        s.sandbox_submitted.umount()
        s.sandbox_judger.umount()
    print('Done  #', subid, ':', result[0], flush=True)
    return


class submission:
    def __init__(self, datas, langinfo, datadir):
        self.id=datas['id']
        self.problem=datas['problem_id']
        self.time=datas['time']

        self.submission_path = datadir/'submissions'/str(self.id)
        self.problem_path = datadir/'problems'/str(self.problem)
        
        cfg=configparser.ConfigParser()
        cfg.read('./config.ini', 'UTF-8')
        
        if os.path.isfile(cfg.get('multiple_judge', 'source_path').replace('{path}', str(self.problem_path))):
            # use custom judger
            self.custom_judger_enabled = True
        else:
            self.custom_judger_enabled = False
        
        self.sandbox_enabled = cfg.getboolean('sandbox', 'enabled')
        if self.sandbox_enabled:
            import sandbox
            addition_paths = cfg.get('sandbox', 'addition_path').split(' ')
            addition_paths.remove('')
            self.sandbox_submitted=sandbox.SandBox(
                cfg.get('sandbox', 'base_dir'),
                cfg.get('sandbox', 'user'),
                addition_paths)
            self.sandbox_judger=sandbox.SandBox(
                cfg.get('sandbox', 'base_dir'),
                cfg.get('sandbox', 'user'),
                addition_paths+[str(self.problem_path)])
            self.sandbox_submitted.mount()
            self.sandbox_judger.mount()
            self.timeout_command = cfg.get('sandbox', 'timeout_command').split(' ')
            
            path = './'
            with open(str(self.submission_path/('source.'+langinfo['extension'])),'rb') as source_file:
                self.sandbox_submitted.put_file('/source.'+langinfo['extension'], source_file.read())
            if self.custom_judger_enabled:
                # copy source file of the custom judger into the sandbox
                judger_source = cfg.get('multiple_judge', 'source_path')
                with open(judger_source.replace('{path}', str(self.problem_path)),'rb') as source_file:
                    self.sandbox_judger.put_file(judger_source.replace('{path}', '/'), source_file.read())
                
                self.judger_exec = cfg.get('multiple_judge', 'exec_path').replace('{path}', path)
                self.judger_compile_cmd = cfg.get('multiple_judge', 'compile_cmd').replace('{path}', path).split(' ')
        else:
            self.sandbox_submitted=subprocess
            self.sandbox_judger=subprocess
            path = str(self.submission_path)
            if self.custom_judger_enabled:
                self.judger_exec = cfg.get('multiple_judge', 'exec_path').replace('{path}', str(self.problem_path))
                self.judger_compile_cmd = cfg.get('multiple_judge', 'compile_cmd').replace('{path}', str(self.problem_path)).split(' ')

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
                p = self.sandbox_submitted.Popen(self.compilecmd, stderr=subprocess.PIPE, creationflags=0x00000200)
            else:
                p = self.sandbox_submitted.Popen(self.compilecmd, stderr=subprocess.PIPE, start_new_session=True)
            
            compile_err = p.communicate(timeout=timelimit)[1]
        except subprocess.TimeoutExpired:
            kill_child_processes(p)
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

    def judge_one_with_custom_judger(self, testcase, timelimit, outputlimit):
        try:
            # start the judger first
            testcase_out_path = str(testcase.parents[1]/'out'/testcase.name)
            # disable Ctrl-C for subprocess
            if sys.platform.startswith('win'):
                # https://msdn.microsoft.com/en-us/library/windows/desktop/ms684863(v=vs.85).aspx
                judger = self.sandbox_judger.Popen([self.judger_exec, str(testcase), testcase_out_path],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, creationflags=0x00000200)
            else:
                judger = self.sandbox_judger.Popen([self.judger_exec, str(testcase), testcase_out_path],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
            
            # run submitted one
            additional_command = []
            if self.sandbox_enabled :
                additional_command += self.timeout_command
                additional_command.append(str(timelimit+1))
            # disable Ctrl-C for subprocess
            if sys.platform.startswith('win'):
                # https://msdn.microsoft.com/en-us/library/windows/desktop/ms684863(v=vs.85).aspx
                submitted = self.sandbox_submitted.Popen(additional_command+self.execcmd, stdin=judger.stdout, stdout=judger.stdin,
                    creationflags=0x00000200)
            else:
                submitted = self.sandbox_submitted.Popen(additional_command+self.execcmd, stdin=judger.stdout, stdout=judger.stdin,
                    start_new_session=True)
            
            starttime=time()
            submitted.communicate(timeout=timelimit)
            exectime=int((time()-starttime)*1000)
            if submitted.returncode!=0:
                return ("RE",None)
        except subprocess.TimeoutExpired:
            kill_child_processes(submitted)
            kill_child_processes(judger)
            return ("TLE",None)
        
        # send EOF to the judger
        judger.stdin.close()
        
        try: # wait for judger for (timelimit) secs
            result = judger.communicate(timeout=timelimit)[1]
        except subprocess.TimeoutExpired:
            kill_child_processes(judger)
            return ("IE",None) # judger TLE
        
        if judger.returncode!=0:
            return ("IE", None)
        if not result.startswith(b"AC"):
            return ("WA", exectime)
        return ("AC", exectime)
    
    def judge_one(self, testcase, timelimit, outputlimit):
        with open(str(testcase), 'r') as input_file:
            try:
                additional_command = []
                if self.sandbox_enabled :
                    additional_command += self.timeout_command
                    additional_command.append(str(timelimit+1))
                
                # disable Ctrl-C for subprocess
                if sys.platform.startswith('win'):
                    # https://msdn.microsoft.com/en-us/library/windows/desktop/ms684863(v=vs.85).aspx
                    p = self.sandbox_submitted.Popen(additional_command+self.execcmd, stdin=input_file, stdout=subprocess.PIPE,
                        creationflags=0x00000200)
                else:
                    p = self.sandbox_submitted.Popen(additional_command+self.execcmd, stdin=input_file, stdout=subprocess.PIPE,
                        start_new_session=True)
                starttime=time()
                out = p.communicate(timeout=timelimit)[0]
                exectime=int((time()-starttime)*1000)
                if p.returncode!=0:
                    return ("RE", None)
            except subprocess.TimeoutExpired:
                kill_child_processes(p)
                return ("TLE",None)
            
            if len(out)>outputlimit*1048576:
                return ("OLE",None)
            
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
            
        if self.custom_judger_enabled:
            if not compile_multiple_judger(self.judger_compile_cmd, self.sandbox_judger,
                compile_timelimit, compile_outputlimit):
                return ('IE',0,None,self.id)

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
                if self.custom_judger_enabled:
                    ret, exectime=self.judge_one_with_custom_judger(testcase, timelimit, outputlimit)
                else:
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
                elif ret=='IE':
                    return ('IE',0,None,self.id)
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

