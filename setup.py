from setuptools import setup

requirements = ['click',
                'distributed',
                'knit >= 0.2.3']

setup(name='dask-yarn-cli',
      version='0.0.1',
      license='BSD',
      install_requires=requirements,
      packages=['dask_yarn_cli'],
      entry_points='''
        [console_scripts]
        dask-yarn=dask_yarn_cli.cli:main
      ''',
      zip_safe=False)
