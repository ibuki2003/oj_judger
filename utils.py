import sys
import configparser
import subprocess

def kill_child_processes(process):
    if sys.platform.startswith('win'):
        # p.kill() doesn't seem to kill the child processes on Windows
        subprocess.run(['TASKKILL', '/F', '/T', '/PID', str(process.pid)], stdout=subprocess.DEVNULL)
    else:
        process.kill()

def Popen(sandbox, *args, **kwargs):
    if 'start_new_session' in kwargs and kwargs['start_new_session']:
        # disable Ctrl-C
        if sys.platform.startswith('win'):
            kwargs.pop('start_new_session')
            # https://msdn.microsoft.com/en-us/library/windows/desktop/ms684863(v=vs.85).aspx
            return sandbox.Popen(*args, creationflags=0x00000200, **kwargs)
        else:
            return sandbox.Popen(*args, **kwargs)
    else:
        return sandbox.Popen(*args, **kwargs)

def newSandbox(cfg, addition_paths=[]):
    if cfg.getboolean('sandbox', 'enabled'):
        import sandbox
        addition_paths += cfg.get('sandbox', 'addition_path').split(' ')
        while '' in addition_paths:
            addition_paths.remove('')
        sb=sandbox.SandBox(
            cfg.get('sandbox', 'base_dir'),
            cfg.get('sandbox', 'user'),
            addition_paths)
        sb.mount()
        return sb
    else:
        return None
