from __future__ import absolute_import, print_function

import os
import shutil
import signal
import sys
import traceback

import click

from . import __version__
from .config import load_config
from .core import start_daemon, Client
from .utils import asciitable, parse_settings, check_pid, get_output_dir


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
    """Start a dask cluster"""
    """
    - Start daemon on random port
    - Write output file
    - Check and log daemon status
    """
    settings = parse_settings(settings)
    config = load_config(config, **settings)

    output_dir = get_output_dir(name=name, prefix=prefix)
    if os.path.exists(output_dir):
        raise ValueError("Cluster output path already exists")
    os.mkdir(output_dir)

    pid = start_daemon(output_dir)

    try:
        client = Client(output_dir)
    except Exception:
        if check_pid(pid):
            os.kill(pid, signal.SIGINT)
        raise

    click.echo("Starting daemon at pid %d..." % pid)
    if client.start(config):
        click.echo("OK")
        status = 0
    else:
        click.echo("Failed to start daemon")
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
    """Stop a dask cluster"""
    output_dir = get_output_dir(name=name, prefix=prefix)
    client = Client(output_dir)
    click.echo("Shutting down daemon...")
    if client.shutdown():
        shutil.rmtree(output_dir)
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
    """Information about running dask clusters

    If neither name or prefix are provided, returns information about all
    clusters running in `./dask/yarn/clusters`.
    """
    if name is None and prefix is None:
        dot_dir = os.path.join(os.path.expanduser('~'), '.dask',
                               'yarn', 'clusters')
        clusters = [os.path.join(dot_dir, d) for d in os.listdir(dot_dir)]
    else:
        output_dir = get_output_dir(name=name, prefix=prefix)
        if not os.path.exists(output_dir):
            raise ValueError("Cluster folder not found at %r" % output_dir)
        clusters = [output_dir]

    if clusters:
        # TODO: actually get address, app_id
        address = 'tcp://foo.bar:8020'
        app_id = 'application_12345_12'
        data = []
        for c in clusters:
            data.append((os.path.basename(c.strip('/')), address, app_id))

        msg = asciitable(['cluster', 'scheduler', 'app_id'], data)
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
    except Exception:
        click.echo(traceback.format_exc(), err=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
