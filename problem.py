from pathlib import Path
import subprocess
import utils
import json

class problem:
    def __init__(self, prb_id, cfg):
        datadir=Path(cfg.get('oj', 'datadir'))
        self.path = datadir/'problems'/str(prb_id)
        
        if (self.path/'tcsets.json').exists():
            with open(str(self.path/'tcsets.json'), 'r') as tcsetsfile:
                tcsets = json.load(tcsetsfile)
            testcaselist=[]
            for tcset in tcsets:
                testcaselist.extend(tcset['problems'])
            testcaselist=[(self.path/'in'/filename) for filename in set(testcaselist)] # remove duplication, make Path Obj
        else:
            testcaselist = list((self.path/'in').glob('*'))
            testcasenamelist = [file.name for file in testcaselist]
            testcasenamelist.sort()
            
            tcsets=[
                {
                    "name":"all",
                    "point":100,
                    "problems":testcasenamelist
                },
            ]
        self.testcases=testcaselist
        self.tcsets=tcsets

        self.judge_type = 'batch'
        if (self.path/'judge.cpp').exists():
            self.judge_type = 'special'
            self.judger=self.path/'judge'

    def compile_judge(self, cfg):
        if self.judge_type!='special':
            return True
        if self.judger.exists() or Path(str(self.judger)+'.exe').exists():
            return True
        try:
            # disable Ctrl-C for subprocess
            cmd=cfg.get('multiple_judge', 'compile_cmd').split(' ') + [str(self.path/'judge.cpp'), '-o', str(self.judger)]
            p = utils.Popen(subprocess, cmd, stderr=subprocess.PIPE, start_new_session=True)
            
            compile_err = p.communicate(timeout=cfg.getint('limit', 'compile_time'))[1]
        except subprocess.TimeoutExpired:
            utils.kill_child_processes(p)
            return False
        
        if p.returncode != 0:
            print(str(compile_err))
            return False
        
        if len(compile_err) > cfg.getint('limit', 'compile_output')*1024:
            return False
            
        return True
