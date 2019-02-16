# -*- coding: UTF-8 -*-
# Sandbox class
# https://github.com/hiromu/arrow-judge/blob/master/src/sandbox.py

import os
import re
import pwd
import json
import stat
import time
import shutil
import signal
import subprocess
import uuid
import configparser

AVAILABLE_DEVICES = ['full', 'null', 'random', 'stderr', 'stdin', 'stdout', 'urandom', 'zero']
CGROUP_SUBSETS = ['cpuacct', 'memory']
AVAILABLE_PATHS = [
    '/bin', '/etc', '/lib', '/lib64', '/proc', '/sbin',
    '/usr',
    '/var/lib']
SYSCTL_PARAMS = ['kernel.sem=0 0 0 0', 'kernel.shmall=0', 'kernel.shmmax=0', 'kernel.shmmni=0', 'kernel.msgmax=0', 'kernel.msgmnb=0', 'kernel.msgmni=0', 'fs.mqueue.queues_max=0']

def execCommand(command):
    return subprocess.call(command, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

class SandBox():
    def __init__(self, directory, user, addition_path=[]):
        self.base_dir = os.path.join(directory, str(uuid.uuid4()))
        self.sandbox_user = user
        
        cfg=configparser.ConfigParser()
        cfg.read('./config.ini', 'UTF-8')
        self.addition_path=addition_path

    def __enter__(self):
        self.mount()
        return self
    
    def __exit__(self, ex_type, ex_value, trace):
        self.umount()
        pass

    def mount(self):
        if not os.path.exists(self.base_dir):
            os.mkdir(self.base_dir)
        
        # Change permission
        uid = pwd.getpwnam(self.sandbox_user)
        os.chown(self.base_dir, uid.pw_uid, uid.pw_gid)

        # Mount /dev filesystem
        if not os.path.exists(self.base_dir + '/dev'):
            os.mkdir(self.base_dir + '/dev')
        for i in AVAILABLE_DEVICES:
            path = self.base_dir + '/dev/' + i

            if not os.path.exists(path):
                open(path, 'a').close()
            if os.path.isdir(path):
                os.removedirs(path)
                open(path, 'a').close()

            execCommand('mount -n --bind /dev/%s %s' % (i, path))

        # Mount allowed directory
        for i in AVAILABLE_PATHS + self.addition_path:
            path = self.base_dir + i

            if not os.path.exists(path):
                os.makedirs(path)
            if not os.path.isdir(path):
                os.remove(path)
                os.makedirs(path)

            execCommand('mount -n --bind -o ro %s %s' % (i, path))

        # Mount tmp directory
        path = self.base_dir + '/tmp'
        if not os.path.exists(path):
            os.makedirs(path)
        if not os.path.isdir(path):
            os.remove(path)
            os.makedirs(path)
        os.chmod(path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)

    def umount(self):
        # Unmount tmp directory
        path = self.base_dir + '/tmp'
        if os.path.exists(path):
            shutil.rmtree(path)

        # Unmount allowed directory
        for i in AVAILABLE_PATHS + self.addition_path:
            path = self.base_dir + i

            while True:
                while execCommand('umount -l %s' % (path)):
                    pass
                try:
                    delete_path = i
                    while delete_path != '/':
                        if os.listdir(self.base_dir + delete_path):
                            break
                        os.rmdir(self.base_dir + delete_path)
                        delete_path = os.path.dirname(delete_path)
                except OSError as e:
                    if re.match(r'\[Errno 16\] Device or resource busy', str(e)):
                        continue
                break

        for i in AVAILABLE_DEVICES:
            path = self.base_dir + '/dev/' + i

            while True:
                while execCommand('umount -l %s' % (path)):
                    pass
                try:
                    os.remove(path)
                except OSError as e:
                    if re.match(r'\[Errno 16\] Device or resource busy', str(e)):
                        continue
                break
                
        os.rmdir(self.base_dir + '/dev')

        shutil.rmtree(self.base_dir)

    def clean(self):
        self.umount()
        self.mount()

    def Popen(self, args, *, as_user=False, **kwargs):
        cmd_args='unshare -finpu chroot {dir}'.format(dir=self.base_dir).split(' ')
        if as_user:
            cmd_args.extend('sudo -u {user}'.format(user=self.sandbox_user).split(' '))
        # 
        cmd_args.extend(args)

        return subprocess.Popen(cmd_args, shell=False, **kwargs)

    def put_file(self, filepath, content):
        with open(self.base_dir+filepath, 'wb') as f:
            f.write(content)
