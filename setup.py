from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='data_minimization_tools',
      version='0.0.1',
      description='Pyhton library for data minimization tools.',
      url='https://github.com/peng-data-minimization/minimizer',
      author='peng-data-minimization',
      long_description=long_description,
      author_email='peng.dataminimization@gmail.com',
      license='MIT',
      packages=['data_minimization_tools', 'data_minimization_tools.utils'],
      install_requires=['numpy']
      )
