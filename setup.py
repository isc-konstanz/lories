#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    corsys
    ~~~~~~


"""
from os import path
from setuptools import setup, find_namespace_packages

here = path.abspath(path.dirname(__file__))
info = {}
with open(path.join("corsys", "_version.py")) as f:
    exec(f.read(), info)

VERSION = info['__version__']

DESCRIPTION = 'This repository provides a set of core functions for several energy system projects of ISC Konstanz.'

# Get the long description from the README file
with open(path.join(here, 'README.md')) as f:
    README = f.read()

NAME = 'corsys'
LICENSE = 'LGPLv3'
AUTHOR = 'ISC Konstanz'
MAINTAINER_EMAIL = 'adrian.minde@isc-konstanz.de'
URL = 'https://github.com/isc-konstanz/corsys'

INSTALL_REQUIRES = ['numpy >= 1.16',
                    'pandas >= 0.23',
                    'pytz >= 2019.1']

PACKAGES = find_namespace_packages(include=['corsys*'])

SETUPTOOLS_KWARGS = {
    'zip_safe': False,
    'include_package_data': True
}

setup(
    name=NAME,
    version=VERSION,
    license=LICENSE,
    description=DESCRIPTION,
    long_description=README,
    author=AUTHOR,
    author_email=MAINTAINER_EMAIL,
    url=URL,
    packages=PACKAGES,
    install_requires=INSTALL_REQUIRES,
    **SETUPTOOLS_KWARGS
)
