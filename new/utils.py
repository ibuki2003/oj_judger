import sys
import configparser
import sandbox
import subprocess

def kill_child_processes(process):
    if sys.platform.startswith('win'):
        # p.kill() doesn't seem to kill the child processes on Windows
        subprocess.run(['TASKKILL', '/F', '/T', '/PID', str(process.pid)], stdout=subprocess.DEVNULL)
    else:
        process.kill()
def newSandbox(cfg):
    if cfg.getboolean('sandbox', 'enabled'):
        addition_paths = cfg.get('sandbox', 'addition_path').split(' ')
        addition_paths.remove('')
        sb=sandbox.SandBox(
            cfg.get('sandbox', 'base_dir'),
            cfg.get('sandbox', 'user'),
            addition_paths)
        sb.mount()
        return sb
    else:
        return None

