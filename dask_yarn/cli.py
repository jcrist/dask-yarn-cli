from __future__ import absolute_import, print_function

import sys
import traceback

import click

from . import __version__


@click.group()
@click.version_option(prog_name="dask-yarn", version=__version__)
def cli():
    pass


@cli.command()
@click.option("--name",
              "-n",
              "name",
              required=True,
              help="Cluster name")
@click.option("--config",
              "-c",
              "config",
              required=True,
              type=click.Path(),
              help="Path to configuration file")
@click.option("--output",
              "-o",
              "output",
              type=click.Path(),
              required=False,
              help=("Path to output yaml file. Defaults to "
                    "``~/.dask/yarn/clusters/($name).yaml``"))
def start(name, config, output):
    """Start a dask cluster"""
    click.echo(name)
    click.echo(config)
    click.echo(output)


@cli.command()
@click.option("--name",
              "-n",
              "name",
              required=False,
              help="Cluster name")
@click.option("--file",
              "-f",
              "file",
              required=False,
              type=click.Path(),
              help="Filepath to the cluster output yaml file.")
def stop(name, file):
    """Stop a dask cluster"""
    click.echo(name)
    click.echo(file)


@cli.command()
@click.option("--name",
              "-n",
              "name",
              required=False,
              help="Cluster name")
@click.option("--file",
              "-f",
              "file",
              required=False,
              type=click.Path(),
              help="Filepath to the cluster output yaml file.")
def info(name, file):
    """Information about running dask clusters"""
    click.echo(name)
    click.echo(file)


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
