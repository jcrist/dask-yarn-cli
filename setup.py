from setuptools import setup

setup(name='dask-yarn-cli',
      version='0.0.1',
      license='BSD',
      install_requires=['click', 'distributed', 'knit', 'dask_yarn'],
      packages=['dask_yarn_cli'],
      entry_points='''
        [console_scripts]
        dask-yarn=dask_yarn_cli.cli:main
      ''',
      zip_safe=False)
