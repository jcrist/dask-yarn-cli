dask-yarn-cli
=============

This package provides an experimental ``dask-yarn`` command line tool for
creating dask clusters on yarn.

Commands
--------

**Create a cluster**

.. code:: bash

    $ dask-yarn start -n mycluster -c config.yaml
    Starting daemon at pid 98765...
    Scheduler:       tcp://123.4.5.6:8080
    Bokeh:           tcp://123.4.5.6:8786
    Application ID:  application_86753_09


**Get information about existing clusters**

.. code:: bash

    $ dask-yarn info
    +---------------+----------------------+----------------------+----------------------+------------+
    | cluster       | scheduler            | diagnostics          | application id       | daemon pid |
    +---------------+----------------------+----------------------+----------------------+------------+
    | mycluster     | tcp://123.4.5.6:8080 | tcp://123.4.5.6:8786 | application_86753_09 | 98765      |
    | other_cluster | tcp://123.4.5.6:8081 | tcp://123.4.5.6:8787 | application_22345_56 | 56564      |
    +---------------+----------------------+----------------------+----------------------+------------+


**Stop a cluster**

.. code:: bash

    $ dask-yarn stop -n mycluster
    Shutting down...
    OK
