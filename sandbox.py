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
        self.mounted=[] # list of real paths

    def __enter__(self):
        self.mount()
        return self
    
    def __exit__(self, ex_type, ex_value, trace):
        self.umount()
        pass

    def __mount_dir(self, path):
        if path in self.mounted: # already mounted
            return
        self.mounted.append(path)
        virtual_path = self.base_dir + path

        if (not path.startswith('/dev')) and os.path.islink(path): # device should be mounted anytime
            shutil.copy(path, virtual_path, follow_symlinks=False)
        elif os.path.isdir(path):
            virtual_path=self.base_dir + path
            if not os.path.exists(virtual_path):
                os.makedirs(virtual_path)
            if not os.path.isdir(virtual_path):
                os.remove(virtual_path)
                os.makedirs(virtual_path)

            execCommand('mount -n --bind -o ro %s %s' % (path, virtual_path))
        else: # file or device
            virtual_path=self.base_dir + path
            if not os.path.exists(virtual_path):
                open(virtual_path, 'a').close()
            if os.path.isdir(virtual_path):
                os.removedirs(virtual_path)
                open(virtual_path, 'a').close()
            execCommand('mount -n --bind %s %s' % (path, virtual_path))

    def mount(self):
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)
        
        # Change permission
        uid = pwd.getpwnam(self.sandbox_user)
        os.chown(self.base_dir, uid.pw_uid, uid.pw_gid)

        # Mount /dev filesystem
        if not os.path.exists(self.base_dir + '/dev'):
            os.mkdir(self.base_dir + '/dev')
        for i in AVAILABLE_DEVICES:
            self.__mount_dir('/dev/' + i)

        # Mount allowed directory
        for i in AVAILABLE_PATHS + self.addition_path:
            self.__mount_dir(i)

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

        for i in self.mounted:
            path = self.base_dir + i
            while True:
                if not os.path.islink(path):
                    while execCommand('umount -l %s' % (path)):
                        pass
                try:
                    if os.path.islink(path) or os.path.isfile(path):
                        os.unlink(path)
                    else:
                        delete_path = i
                        while delete_path != '/':
                            if os.listdir(self.base_dir + delete_path):
                                break
                            os.rmdir(self.base_dir + delete_path)
                            delete_path = os.path.dirname(delete_path)
                except OSError as e:
                    if re.match(r'\[Errno 16\] Device or resource busy', str(e)):
                        sleep(1)
                        continue
                break

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

    def put_file(self, filepath, content, permission=0o644):
        with open(self.base_dir+filepath, 'wb') as f:
            f.write(content)
        os.chmod(self.base_dir+filepath, permission)

