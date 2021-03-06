# -*- coding: utf-8 -*-
"""
    emonpy
    ~~~~~
    
    Emonpy provides a set of functions to communicate with an emoncms webserver.
    The Energy monitoring Content Management System (see http://emoncms.org/) is an 
    open-source web-app for processing, logging and visualising energy, temperature 
    and other environmental data as part of the OpenEnergyMonitor project.
    
"""

import logging
logging.basicConfig(level=logging.INFO)

from . import emoncms
from .emoncms import Emoncms
from .emoncms import EmoncmsData
from .emoncms import EmoncmsException
