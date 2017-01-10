#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    emonpy
    ~~~~~
    
    Emonpy provides a set of functions to communicate with an emoncms webserver.
    The Energy monitoring Content Management System (see http://emoncms.org/) is an 
    open-source web-app for processing, logging and visualising energy, temperature 
    and other environmental data as part of the OpenEnergyMonitor project.
    
"""
import re
from os import path

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'emonpy/', '__init__.py'), 'r') as f:
    VERSION = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        f.read(), re.MULTILINE).group(1)

if not VERSION:
    raise RuntimeError('Cannot find version information')

DESCRIPTION = 'Emonpy provides a set of functions to communicate with an emoncms webserver.'

# Get the long description from the README file
with open(path.join(here, 'README.md')) as f:
    LONG_DESCRIPTION = f.read()

DISTNAME = 'emonpy'
LICENSE = 'LGPLv3'
AUTHOR = 'ISC Konstanz'
MAINTAINER_EMAIL = 'adrian.minde@isc-konstanz.de'
URL = 'https://github.com/isc-konstanz/emonpy'

INSTALL_REQUIRES = ['numpy >= 1.12.0',
                    'pandas >= 0.19.0',
                    'requests >= 2.12.4']

PACKAGES = ['emonpy']

setuptools_kwargs = {
    'zip_safe': False,
    'scripts': [],
    'include_package_data': True
}

setup(
    name=DISTNAME,
    version=VERSION,
    license=LICENSE,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    author=AUTHOR,
    author_email=MAINTAINER_EMAIL,
    url=URL,
    packages=PACKAGES,
    install_requires=INSTALL_REQUIRES,
    **setuptools_kwargs
)