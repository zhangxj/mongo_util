import os
from setuptools import setup

# python setup.py sdist bdist_egg upload

# Utility function to read the README file.  
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...

setup(
    name = "mongo_util",
    version = "0.1.3",
    author = "9wfox",
    author_email = "568628130@qq.com",
    description = ("An demonstration of how to create, document, and publish "
                   "to the cheese shop a5 pypi.org."),
    license = "BSD",
    keywords = "mongo_util mongo mongodb utility",
    url = "",
    packages=['mongo_util'],
    setup_requires = ['pymongo'],
    long_description="util opt mongodb",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: BSD License",
    ],
)
