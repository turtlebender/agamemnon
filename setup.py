import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.txt')).read()
CHANGES = open(os.path.join(here, 'CHANGES.txt')).read()

requires = ['pycassa']

setup(name='agamemnon',
      version='0.0',
      description='A graph database built on top of cassandra',
      long_description=README + '\n\n' +  CHANGES,
      classifiers=[
        "Programming Language :: Python",
        ],
      author='Tom Howe',
      author_email='trhowe@ci.uchicago.edu',
      url='https://github.com/turtlebender/agamemnon',
      keywords='cassandra',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      install_requires=requires,
      tests_require=requires,
      test_suite="nose.collector",
      )

