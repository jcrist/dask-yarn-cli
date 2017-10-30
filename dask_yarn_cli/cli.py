from __future__ import absolute_import, print_function

import os
import signal
import sys
import traceback

import click

from . import __version__
from .config import load_config, check_config
from .core import start_daemon, Client
from .utils import (asciitable, parse_settings, check_pid, get_output_dir,
                    DaskYarnError)


@click.group()
@click.version_option(prog_name="dask-yarn", version=__version__)
def cli():
    pass


@cli.command()
@click.option("--name",
              "-n",
              "name",
              required=False,
              help="Cluster name")
@click.option("--prefix",
              "-p",
              "prefix",
              type=click.Path(),
              required=False,
              help=("Prefix to output folder. Defaults to "
                    "``~/.dask/yarn/clusters/$name``"))
@click.option("--config",
              "-c",
              "config",
              required=False,
              type=click.Path(),
              help="Path to configuration file")
@click.option("--settings",
              "-s",
              "settings",
              required=False,
              multiple=True,
              help="Additional key-value pairs to override")
def start(name, prefix, config, settings):
    """Start a dask cluster."""
    settings = parse_settings(settings)
    config = load_config(config, **settings)
    check_config(config)

    output_dir = get_output_dir(name=name, prefix=prefix)
    if os.path.exists(output_dir):
        raise DaskYarnError("Cluster output path already exists")

    pid = start_daemon(output_dir)

    try:
        client = Client(output_dir)
    except Exception:
        if check_pid(pid):
            os.kill(pid, signal.SIGINT)
        raise

    click.echo("Starting daemon at pid %d..." % pid)
    resp = client.start(config)
    if resp['status'] == 'ok':
        msg = ("Scheduler:       tcp://{ip}:{port:d}\n"
               "Bokeh:           tcp://{ip}:{bokeh:d}\n"
               "Application ID:  {id}").format(
                    ip=resp['scheduler.ip'],
                    port=resp['scheduler.port'],
                    bokeh=resp['scheduler.bokeh_port'],
                    id=resp['application.id'])
        click.echo(msg)
        status = 0
    else:
        exc = resp['exception']
        tb = resp['traceback']
        msg = ("Failed to start cluster:\n\n"
               "%s\n"
               "%s\n"
               "%s") % (exc, '-'*len(exc), tb)
        click.echo(msg)
        status = 1
    sys.exit(status)


@cli.command()
@click.option("--name",
              "-n",
              "name",
              required=False,
              help="Cluster name")
@click.option("--prefix",
              "-p",
              "prefix",
              required=False,
              type=click.Path(),
              help="Prefix to output folder.")
def stop(name, prefix):
    """Stop a dask cluster."""
    output_dir = get_output_dir(name=name, prefix=prefix)
    client = Client(output_dir)
    click.echo("Shutting down...")
    resp = client.shutdown()
    if resp['status'] == 'ok':
        click.echo("OK")
        status = 0
    else:
        click.echo("Shutdown Failed")
        status = 1
    sys.exit(status)


@cli.command()
@click.option("--name",
              "-n",
              "name",
              required=False,
              help="Cluster name")
@click.option("--prefix",
              "-p",
              "prefix",
              required=False,
              type=click.Path(),
              help="Prefix to output folder.")
def info(name, prefix):
    """Information about running dask clusters.

    If neither name or prefix are provided, returns information about all
    clusters running in `~/.dask/yarn/clusters`.
    """
    if name is None and prefix is None:
        dot_dir = os.path.join(os.path.expanduser('~'), '.dask',
                               'yarn', 'clusters')
        clusters = [os.path.join(dot_dir, d) for d in os.listdir(dot_dir)]
    else:
        output_dir = get_output_dir(name=name, prefix=prefix)
        if not os.path.exists(output_dir):
            raise DaskYarnError("No cluster folder found at %r" % output_dir)
        clusters = [output_dir]

    if clusters:
        data = []
        for c in clusters:
            config = load_config(os.path.join(c, 'config.yaml'))
            ip = config['scheduler.ip']
            data.append((os.path.basename(c.strip('/')),
                         'tcp://%s:%d' % (ip, config['scheduler.port']),
                         'tcp://%s:%d' % (ip, config['scheduler.bokeh_port']),
                         config['application.id'],
                         config['application.pid']))

        msg = asciitable(['cluster', 'scheduler', 'diagnostics',
                          'application id', 'daemon pid'], data)
    else:
        msg = 'No active clusters found.'
    click.echo(msg)


_py3_err_msg = """
Your terminal does not properly support unicode text required by command line
utilities running Python 3. This is commonly solved by specifying encoding
environment variables, though exact solutions may depend on your system:
    $ export LC_ALL=C.UTF-8
    $ export LANG=C.UTF-8
For more information see: http://click.pocoo.org/5/python3/
""".strip()


def main():
    # Pre-check for python3 unicode settings
    try:
        from click import _unicodefun
        _unicodefun._verify_python3_env()
    except (TypeError, RuntimeError):
        click.echo(_py3_err_msg, err=True)

    # run main
    try:
        cli()
    except DaskYarnError as e:
        click.echo("DaskYarnError: %s" % e, err=True)
        sys.exit(1)
    except Exception:
        click.echo(traceback.format_exc(), err=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
