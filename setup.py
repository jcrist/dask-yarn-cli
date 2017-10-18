from setuptools import setup

setup(name='dask-yarn',
      version='0.0.1',
      license='BSD',
      install_requires=['click'],
      packages=['dask_yarn'],
      entry_points='''
        [console_scripts]
        dask-yarn=dask_yarn.cli:main
      ''',
      zip_safe=False)
