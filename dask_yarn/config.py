from __future__ import absolute_import, print_function, division

import os

import yaml

from .utils import DaskYarnError


__all__ = ('load_config', 'check_config', 'dump_config')


_defaults = {'scheduler.port': 0,
             'scheduler.bokeh_port': 8787,
             'worker.cpus': 2,
             'worker.memory': 2048,
             'worker.processes': 1,
             'worker.threads_per_process': 2,
             'cluster.count': 4,
             'yarn.queue': 'default'}

_mandatory = {'cluster.env'}

_optional = {'scheduler.ip',
             'yarn.host',
             'yarn.port',
             'hdfs.host',
             'hdfs.port',
             'hdfs.home'}

_all_fields = _mandatory.union(_defaults, _optional)


def _format_list(vals):
    return "\n".join(map("- {}".format, sorted(vals)))


def check_config(config):
    extra = set(config).difference(_all_fields)
    if extra:
        raise DaskYarnError("Extra configuration fields:\n"
                            "%s" % _format_list(extra))

    missing = _mandatory.difference(config)
    if missing:
        raise DaskYarnError("Missing configuration fields:\n"
                            "%s" % _format_list(missing))


def load_config(config_path=None, **settings):
    """Load a config.yaml file into a dot-concatenated dict"""
    config = _defaults.copy()

    if config_path is not None:
        if not os.path.exists(config_path):
            raise DaskYarnError("Configuration file not found at "
                                "%r" % config_path)
        with open(config_path) as fil:
            mapping = yaml.load(fil)
        config.update(flatten_mapping(mapping))

    config.update(settings)

    return config


def dump_config(config, path):
    """Write a dot-concatenated dict to a config yaml file"""
    config2 = unflatten_mapping(config)
    with open(path, 'w+') as fil:
        yaml.dump(config2, fil, default_flow_style=False)


def _flatten_mapping(x, prefix, out):
    for k, v in x.items():
        if type(v) is dict:
            _flatten_mapping(v, prefix + (k,), out)
        else:
            out['.'.join(prefix + (k,))] = v


def flatten_mapping(mapping):
    """Flatten a dict of dicts, dot-concatenating keys.

    Examples
    --------
    >>> x = {'a1': {'b1': {'c1': 1},
    ...             'b2': 2},
    ...      'a2': {'b1': 3}}
    >>> flatten_mapping(x)  # doctest: +SKIP
    {'a1.b1.c1': 1,
     'a1.b2': 2,
     'a2.b1': 3}
    """
    out = {}
    _flatten_mapping(mapping, (), out)
    return out


def unflatten_mapping(mapping):
    """Unflatten a dict with dot-concatenated keys to a dict of dicts

    Examples
    --------
    >>> x = {'a1.b1.c1': 1,
    ...      'a1.b2': 2,
    ...      'a2.b1': 3}
    >>> unflatten_mapping(x)  # doctest: +SKIP
    {'a1': {'b1': {'c1': 1},
     'b2': 2},
     'a2': {'b1': 3}}
    """
    out = {}
    for k, v in mapping.items():
        keys = k.split('.')
        o = out
        for k2 in keys[:-1]:
            o = o.setdefault(k2, {})
        o[keys[-1]] = v
    return out
