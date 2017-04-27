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
from os import path

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

here = path.abspath(path.dirname(__file__))


VERSION = '0.1.2'

DESCRIPTION = 'Emonpy provides a set of functions to communicate with an emoncms webserver.'

# Get the long description from the README file
with open(path.join(here, 'README.md')) as f:
    DESCRIPTION_LONG = f.read()

NAME = 'emonpy'
LICENSE = 'LGPLv3'
AUTHOR = 'ISC Konstanz'
MAINTAINER_EMAIL = 'adrian.minde@isc-konstanz.de'
URL = 'https://github.com/isc-konstanz/emonpy'

INSTALL_REQUIRES = ['numpy >= 1.8.2',
                    'pandas >= 0.14.1',
                    'requests']

PACKAGES = ['emonpy']

SETUPTOOLS_KWARGS = {
    'zip_safe': False,
    'scripts': [],
    'include_package_data': True
}

setup(
    name = NAME,
    version = VERSION,
    license = LICENSE,
    description = DESCRIPTION,
    long_description = DESCRIPTION_LONG,
    author = AUTHOR,
    author_email = MAINTAINER_EMAIL,
    url = URL,
    packages = PACKAGES,
    install_requires = INSTALL_REQUIRES,
    **SETUPTOOLS_KWARGS
)