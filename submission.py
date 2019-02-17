from problem import problem
from pathlib import Path
import utils
import subprocess
from pipe import pipe
from time import time
import traceback
import json

class submission:
    def __init__(self, sub_id, cfg, cursor):
        self.cfg=cfg
        self.cursor=cursor
        self.id=sub_id
        self.cursor.execute('SELECT * FROM `submissions` WHERE id=%s', (self.id,))
        datas=self.cursor.fetchone()
        self.problem=problem(datas['problem_id'], self.cfg)

        datadir=Path(self.cfg.get('oj', 'datadir'))

        self.path = datadir/'submissions'/str(self.id)
        
        self.cursor.execute('SELECT * FROM `langs` WHERE id=%s', (datas['lang_id'],))
        self.lang=self.cursor.fetchone()
        
        self.sandbox_enabled = self.cfg.getboolean('sandbox', 'enabled')

        self.compile_required=self.lang['compile'] is not None
        source_dir = '/' if self.sandbox_enabled else str(self.path)
        if(self.compile_required):
            self.compilecmd=self.lang['compile'].replace('{path}', source_dir).split()
        
    
    def compile(self, sandbox):
        if not self.compile_required: # no compile
            return True
        
        with open(str(self.path/'judge_log.txt'),'wb') as logfile:
            
            try:
                # disable Ctrl-C for subprocess
                p = utils.Popen(sandbox, self.compilecmd, stderr=subprocess.PIPE, start_new_session=True)
                
                compile_err = p.communicate(timeout=self.cfg.getint('limit', 'compile_time'))[1]
            except subprocess.TimeoutExpired:
                utils.kill_child_processes(p)
                logfile.write(b"Compile time limit exceeded")
                return False
            
            if len(compile_err) > self.cfg.getint('limit', 'compile_output')*1024:
                logfile.write(b"Compile output limit exceeded")
                return False
            logfile.write(compile_err)
            
        return (p.returncode == 0)
    
    def judge(self):
        try:
            for file in [
                    self.path/'judge_log.txt',
                    self.path/'judge_log.json']:
                if file.exists():
                    file.unlink()
            if self.problem.compile_judge(self.cfg)==False:
                return ('IE', 0, None)

            if self.sandbox_enabled:
                sandbox_submission=utils.newSandbox(self.cfg)
                timeout_command = self.cfg.get('sandbox', 'timeout_command').split(' ')
                with open(str(self.path/('source.'+self.lang['extension'])),'rb') as source_file:
                    sandbox_submission.put_file('/source.'+self.lang['extension'], source_file.read())
                
                if self.problem.judge_type=='special':
                    sandbox_judge=utils.newSandbox(self.cfg, [str(self.problem.path)])
                    with open(str(self.problem.judger),'rb') as source_file:
                        sandbox_judge.put_file('/judge', source_file.read(), 0o755)
                            
                path = './'
                judger_path = './judge'
            else:
                sandbox_submission = subprocess
                sandbox_judge = subprocess
                timeout_command = None
                path = str(self.path)
                judger_path = str(self.problem.path) + '/judge'

            if self.compile(sandbox_submission)==False:
                return ('CE', 0, None)


            
            
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

            for testcase in self.problem.testcases:
                if self.problem.judge_type == 'batch':
                    ret=self.__judge_batch(
                        testcase=testcase, 
                        timeout_command=timeout_command,
                        path=path,
                        sandbox=sandbox_submission)
                elif self.problem.judge_type == 'special':
                    ret=self.__judge_special(
                        testcase=testcase, 
                        timeout_command=timeout_command,
                        path=path,
                        sandbox_submission=sandbox_submission,
                        judger_path=judger_path,
                        sandbox_judge=sandbox_judge)
                
                stat, exectime = ret
                if exectime is not None :
                    exectime_max = max(exectime_max, exectime)
                if stat in ['RE','OLE','TLE']:
                    stats[stat]=True
                    problem_results[testcase.name]={
                        'status': stat
                    }
                elif stat=='WA':
                    stats[stat]=True
                    problem_results[testcase.name]={
                        'status': stat,
                        'time': exectime
                    }
                elif stat=='IE':
                    return ('IE',0,None)
                else:
                    problem_results[testcase.name]={
                        'status': stat,
                        'time': exectime
                    }
                result_data['result'].append({
                    'name': testcase.name,
                    'status': stat,
                    'time': exectime,
                })
            result_data['result'].sort(key=lambda x: x['name'])

            if self.sandbox_enabled:
                sandbox_submission.umount()
                if self.problem.judge_type == 'special':
                    sandbox_judge.umount()
            
            point = 0
            for tcset in self.problem.tcsets: # calculate point
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
            return ('IE',0,None)
        else:
            with open(str(self.path/'judge_log.json'),'w') as logfile:
                logfile.write(json.dumps(result_data))
            if stats['RE']:
                return ('RE',point,None)
            elif stats['OLE']:
                return ('OLE',point,None)
            elif stats['TLE']:
                return ('TLE',point,None)
            elif stats['WA']:
                return ('WA',point,exectime_max)
            else:
                return ('AC',point,exectime_max)
            
    def __judge_batch(self, testcase, timeout_command, path, sandbox):
        testcase_in=str(testcase)
        testcase_out=str(testcase.parents[1]/'out'/testcase.name)
        with open(testcase_in, 'r') as input_file:
            try:
                additional_command = []
                if self.sandbox_enabled :
                    additional_command += timeout_command
                    additional_command.append(str(self.cfg.getint('limit', 'time')+1))
                
                starttime=time()
                # disable Ctrl-C for subprocess
                p = utils.Popen(sandbox, additional_command + self.lang['exec'].replace('{path}',path).split(), stdin=input_file, stdout=subprocess.PIPE,
                    start_new_session=True)
                
                out = p.communicate(timeout=self.cfg.getint('limit', 'time'))[0]
                exectime=int((time()-starttime)*1000)
                if p.returncode!=0:
                    return ("RE", None)
            except subprocess.TimeoutExpired:
                utils.kill_child_processes(p)
                return ("TLE",None)
            
            if len(out)>self.cfg.getint('limit', 'output')*1048576:
                return ("OLE",None)
            
            with open(testcase_out, 'r') as ansfile:
                anslist=ansfile.read().split()
            outlist=out.decode('utf-8').split()
            
            if len(outlist)!=len(anslist):
                return ("WA",exectime)
            for i in range(len(outlist)):
                if(outlist[i]!=anslist[i]):
                    return ("WA",exectime)
            return ("AC",exectime)
    def __judge_special(self, testcase, timeout_command, path, sandbox_submission,
            judger_path, sandbox_judge):
        testcase_in=str(testcase)
        testcase_out=str(testcase.parents[1]/'out'/testcase.name)
        # make pipe to communicate
        with pipe() as inpp, pipe() as outp:
            try:
                # start the judger first
                # disable Ctrl-C for subprocess
                judger = utils.Popen(sandbox_judge, [judger_path, testcase_in, testcase_out],
                    stdin=outp.r, stdout=inpp.w, stderr=subprocess.PIPE, start_new_session=True)
                
                # run submitted one
                additional_command = []
                if self.sandbox_enabled :
                    additional_command += timeout_command
                    additional_command.append(str(self.cfg.getint('limit', 'time')+1))
                
                starttime=time()
                # disable Ctrl-C for subprocess
                submitted = utils.Popen(sandbox_submission, additional_command + self.lang['exec'].replace('{path}',path).split(), stdin=inpp.r, stdout=outp.w,
                    start_new_session=True)
                
                # submitted.communicate(timeout=timelimit)
                submitted.wait(timeout=self.cfg.getint('limit', 'time'))
                exectime=int((time()-starttime)*1000)
                if submitted.returncode!=0:
                    utils.kill_child_processes(judger)
                    return ("RE",None)
            except subprocess.TimeoutExpired:
                utils.kill_child_processes(submitted)
                utils.kill_child_processes(judger)
                return ("TLE",None)
            
        try: # wait for judger for (timelimit) secs
            result = judger.communicate(timeout=self.cfg.getint('limit', 'output'))[1]
        except subprocess.TimeoutExpired:
            utils.kill_child_processes(judger)
            return ("IE",None) # judger TLE
        
        if judger.returncode!=0:
            return ("IE", None)
        if result.startswith(b"AC"):
            return ("AC", exectime)
        return ("WA", exectime)
