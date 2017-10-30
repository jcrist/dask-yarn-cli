from __future__ import absolute_import, print_function

import atexit
import json
import os
import shutil
import socket
import subprocess
import sys
import time
import traceback

from knit import Knit
from distributed import LocalCluster
try:
    import hdfs3
except ImportError:
    hdfs3 = None

from .config import dump_config


if sys.version_info.major < 3:
    def to_bytes(x):
        return x
else:
    def to_bytes(x):
        return bytes(x, 'utf-8')


def _daemon(cache_dir):
    sys.exit(Server(cache_dir).run_until_shutdown())


def start_daemon(cache_dir):
    if hasattr(subprocess, 'DEVNULL'):
        null = subprocess.DEVNULL
    else:
        null = open(os.devnull, 'wb')
    null = None
    script = 'from dask_yarn_cli.core import _daemon;_daemon(%r)' % cache_dir

    pid = subprocess.Popen([sys.executable, '-c', script],
                           stdout=null, stderr=null).pid
    return pid


class Server(object):
    def __init__(self, cache_path):
        self.cache_path = cache_path
        self.address = os.path.join(self.cache_path, 'comm')
        self.config_path = os.path.join(self.cache_path, 'config.yaml')
        self.cluster = None
        self.knit = None
        self._should_shutdown = False

    def run_until_shutdown(self):
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        os.mkdir(self.cache_path)
        atexit.register(shutil.rmtree, self.cache_path)

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
        conn.sendall(to_bytes(json.dumps(resp) + '\n'))
        # Close the connection
        try:
            conn.shutdown(socket.SHUT_WR)
        except OSError:
            pass
        conn.close()

    def handle_badmsg(self, msg):
        if self.knit is None:
            self._should_shutdown = True
        return {"status": "error",
                "exception": "Badmsg: %s" % str(msg),
                "traceback": ""}

    def handle_shutdown(self, msg):
        self._should_shutdown = True
        return {"status": "ok"}

    def handle_start(self, msg):
        config = msg['config']
        try:
            cluster, k, config2 = setup_cluster(config)
        except Exception as e:
            _, _, tb = sys.exc_info()
            exc_msg = str(e)
            tb_msg = ''.join(traceback.format_tb(tb))
            resp = {'status': 'error',
                    'exception': exc_msg,
                    'traceback': tb_msg}
            # Failed to start, shutdown
            self._should_shutdown = True
            return resp

        self.cluster = cluster
        self.knit = k
        dump_config(config2, self.config_path)
        return {'status': 'ok',
                'application.id': config2['application.id'],
                'scheduler.ip': config2['scheduler.ip'],
                'scheduler.port': config2['scheduler.port'],
                'scheduler.bokeh_port': config2['scheduler.bokeh_port']}


def setup_cluster(config):
    if 'scheduler.ip' not in config:
        scheduler_ip = socket.gethostbyname(socket.gethostname())
    else:
        scheduler_ip = config['scheduler.ip']
    cluster = LocalCluster(n_workers=0,
                           ip=scheduler_ip,
                           port=config['scheduler.port'],
                           diagnostics_port=config['scheduler.bokeh_port'])

    if hdfs3 is not None:
        hdfs = hdfs3.HDFileSystem(host=config.get('hdfs.host'),
                                  port=config.get('hdfs.port'))
    else:
        hdfs = None

    knit = Knit(hdfs=hdfs,
                hdfs_home=config.get('hdfs.home'),
                rm=config.get('yarn.host'),
                rm_port=config.get('yarn.port'))

    env_name_zip = os.path.basename(config['cluster.env'])
    env_name = os.path.splitext(env_name_zip)[0]
    env_location = os.path.join(env_name_zip, env_name)
    command = ('{env_location}/bin/python {env_location}/bin/dask-worker '
               '--nprocs={nprocs:d} '
               '--nthreads={nthreads:d} '
               '--memory-limit={memory_limit:d} '
               '--no-bokeh '
               '{scheduler_address} '
               '> /tmp/worker-log.out '
               '2> /tmp/worker-log.err').format(
                    env_location=env_location,
                    nprocs=config['worker.processes'],
                    nthreads=config['worker.threads_per_process'],
                    memory_limit=int(config['worker.memory'] * 1e6),
                    scheduler_address=cluster.scheduler.address)

    app_id = knit.start(command,
                        files=[config['cluster.env']],
                        num_containers=config['cluster.count'],
                        virtual_cores=config['worker.cpus'],
                        memory=config['worker.memory'],
                        queue=config['yarn.queue'],
                        app_name='dask',
                        checks=False)

    # Add a few missing fields to config before writing to disk
    config2 = config.copy()
    # The ip is optional, the port may be chosen dynamically
    config2['scheduler.ip'] = cluster.scheduler.ip
    config2['scheduler.port'] = cluster.scheduler.port
    # Fill in optional parameters with auto-detected versions
    config2['yarn.host'] = knit.conf['rm']
    config2['yarn.port'] = knit.conf['rm_port']
    config2['hdfs.home'] = knit.hdfs_home
    # Add in runtime information like app_id and daemon pid
    config2['application.id'] = app_id
    config2['application.pid'] = os.getpid()

    return cluster, knit, config2


class Client(object):
    def __init__(self, cache_path, retries=20):
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
        self.sock.sendall(to_bytes(msg + '\n'))

    def _recvmsg(self):
        return json.loads(self.sock.makefile().readline())

    def shutdown(self):
        self._sendmsg({"op": "shutdown"})
        return self._recvmsg()

    def start(self, config):
        self._sendmsg({'op': 'start', 'config': config})
        return self._recvmsg()
