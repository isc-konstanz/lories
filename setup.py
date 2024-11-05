#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
lori
~~~~

Legacy compatibility setup script for the lori package.

"""

import versioneer
from setuptools import setup

setup(
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
)
