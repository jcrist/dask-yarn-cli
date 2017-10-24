from __future__ import absolute_import, print_function

import json
import os
import socket
import subprocess
import sys
import time

from .config import dump_config


PY2 = sys.version_info.major == 2


def get_output_dir(name=None, prefix=None):
    if name and prefix:
        raise ValueError("Cannot specify both ``name`` and ``prefix``")
    elif prefix:
        output_dir = prefix
    else:
        dot_dir = os.path.join(os.path.expanduser('~'), '.dask',
                               'yarn', 'clusters')
        makedirs(dot_dir, exist_ok=True)
        output_dir = os.path.join(dot_dir, name)

    return output_dir


def makedirs(d, exist_ok=False):
    if PY2:
        try:
            os.makedirs(d)
        except OSError as e:
            if e.args[0] == 17 and exist_ok:
                # 'File exists'
                return
            else:
                raise
    else:
        os.makedirs(d, exist_ok=exist_ok)


def _daemon(cache_dir):
    sys.exit(Server(cache_dir).run_until_shutdown())


def start_daemon(cache_dir):
    if hasattr(subprocess, 'DEVNULL'):
        null = subprocess.DEVNULL
    else:
        null = open(os.devnull, 'wb')
    null = None
    script = 'from dask_yarn.core import _daemon;_daemon(%r)' % cache_dir

    pid = subprocess.Popen([sys.executable, '-c', script],
                           stdout=null, stderr=null).pid
    return pid


class Server(object):
    def __init__(self, cache_path):
        self.cache_path = cache_path
        self.address = os.path.join(self.cache_path, 'comm')
        self.config_path = os.path.join(self.cache_path, 'config.yaml')
        self._should_shutdown = False

    def run_until_shutdown(self):
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        listener.bind(self.address)

        listener.listen(5)
        while True:
            conn, address = listener.accept()
            self.handle_connection(conn)
            if self._should_shutdown:
                listener.close()
                break
        return 0

    def handle_connection(self, conn):
        data = conn.makefile().readline()
        try:
            msg = json.loads(data)
            op = msg['op']
        except Exception:
            msg = data
            op = 'badmsg'

        resp = getattr(self, 'handle_%s' % op, self.handle_badmsg)(msg)
        conn.sendall(bytes(resp + '\n', 'utf-8'))
        # Close the connection
        try:
            conn.shutdown(socket.SHUT_WR)
        except OSError:
            pass
        conn.close()

    def handle_badmsg(self, msg):
        return '{"status": "error"}'

    def handle_shutdown(self, msg):
        self._should_shutdown = True
        return '{"status": "ok"}'

    def handle_start(self, msg):
        config = msg['config']
        dump_config(config, self.config_path)
        return '{"status": "ok"}'


class Client(object):
    def __init__(self, cache_path, retries=5):
        self.address = os.path.join(cache_path, 'comm')
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        for _ in range(retries):
            try:
                self.sock.connect(self.address)
                break
            except Exception:
                pass
            time.sleep(0.05)
        else:
            # One un-protected connect call to raise the error
            self.sock.connect(self.address)

    def _sendmsg(self, msg):
        msg = json.dumps(msg)
        self.sock.sendall(bytes(msg + '\n', 'utf-8'))

    def _recvmsg(self):
        return json.loads(self.sock.makefile().readline())

    def shutdown(self):
        self._sendmsg({"op": "shutdown"})
        resp = self._recvmsg()
        return resp['status'] == 'ok'

    def start(self, config):
        self._sendmsg({'op': 'start', 'config': config})
        resp = self._recvmsg()
        return resp['status'] == 'ok'
