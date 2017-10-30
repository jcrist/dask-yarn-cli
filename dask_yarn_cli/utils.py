from __future__ import absolute_import, print_function, division

import os
import sys


PY2 = sys.version_info.major == 2


class DaskYarnError(Exception):
    """User-facing exception for dask-yarn. These exceptions here are reported
    nicely in the cli."""
    pass


def asciitable(columns, rows):
    """Formats an ascii table for given columns and rows.

    Parameters
    ----------
    columns : list
        The column names
    rows : list of tuples
        The rows in the table. Each tuple must be the same length as
        ``columns``.
    """
    rows = [tuple(str(i) for i in r) for r in rows]
    columns = tuple(str(i) for i in columns)
    widths = tuple(max(max(map(len, x)), len(c))
                   for x, c in zip(zip(*rows), columns))
    row_template = ('|' + (' %%-%ds |' * len(columns))) % widths
    header = row_template % tuple(columns)
    bar = '+%s+' % '+'.join('-' * (w + 2) for w in widths)
    data = '\n'.join(row_template % r for r in rows)
    return '\n'.join([bar, header, bar, data, bar])


def parse_settings(settings):
    """Convert a list of ("key=val", ...) settings into a dict"""
    out = {}
    for s in settings:
        try:
            k, v = s.split('=')
        except:
            raise DaskYarnError("Unable to parse setting: %r" % s)
        out[k.strip()] = v.strip()
    return out


def get_output_dir(name=None, prefix=None):
    if name is not None and prefix is not None:
        raise DaskYarnError("Cannot specify both ``name`` and ``prefix``")
    elif name is None and prefix is None:
        raise DaskYarnError("Must specify either ``name`` or ``prefix``")
    elif prefix:
        output_dir = prefix
    else:
        dot_dir = os.path.join(os.path.expanduser('~'), '.dask',
                               'yarn', 'clusters')
        makedirs(dot_dir, exist_ok=True)
        output_dir = os.path.join(dot_dir, name)

    return output_dir


def check_pid(pid):
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


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
