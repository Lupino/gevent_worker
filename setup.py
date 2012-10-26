#!/usr/bin/env python

import sys
import os
from distutils.core import setup

try:
    from distutils.command.build_py import build_py_2to3 as build_py
except ImportError:
    from distutils.command.build_py import build_py

setup(
    name = 'gevent-worker',
    version = '0.1',
    url = "https://github.com/Ubasic/gevent_worker",
    description = 'A gevent 1.0 worker',
    long_description = open('README.md').read(),
    author = 'Li Meng Jun',
    author_email = 'lmjubuntu@gmail.com',
    license = 'MIT',
    platforms = 'any',
    packages = [
        'gevent_worker'
    ],
    requires = [
        'gevent (>=1.0)',
    ],
    cmdclass = {'build_py': build_py}
)

