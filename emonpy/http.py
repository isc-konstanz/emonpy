# -*- coding: utf-8 -*-
"""
    emonpy.http
    ~~~~~
    
    This module implements the HTTP communication with an emoncms web server.
    It provides inherited objects, to handle actual function calls to access 
    the webserver and e.g. post or retrieve data with :class:`HttpInput` 
    and :class:`HttpFeed` reference objects.
    
"""
import logging
logger = logging.getLogger('emonpy.http')

import requests
import numpy as np
import pandas as pd

from emonpy import emoncms


class HttpEmoncms(emoncms.Emoncms):
    
    def __init__(self, address, apikey, timezone='UTC'):
        self.address = address
        self.apikey = apikey
        self.timezone = timezone
        
        logger.info('Registering connection to emoncms webserver "%s"', self.address)

    def input(self, node, name):
        return HttpInput(self, node, name)
    
    def feed(self, feedid):
        return HttpFeed(self, feedid)
    
    def _request(self, action, parameters):
        parameters['apikey'] = self.apikey
        
        return requests.get(self.address + action, params=parameters)


class HttpInput(emoncms.Input):
    pass


class HttpFeed(emoncms.Feed):
    
    def data(self, start, end, interval, timezone='UTC'):
        logger.debug('Requesting data from feed %i', self.feedid)

        # Convert times to UTC UNIX timestamps
        startstamp = start.tz_convert(self.connection.timezone).astype(np.int64)//10**6
        endstamp = end.tz_convert(self.connection.timezone).astype(np.int64)//10**6
        
        # Legacy: Convert the interval to amount of datapoints to retrieve
        datapoints = int((endstamp - startstamp)/interval)
        
        params = {'id': self.feedid.text.replace('"', ''), 
                  'start': startstamp, 
                  'end': endstamp, 
                  'dp': datapoints}
#                   'interval': interval}
        
        resp = self.connection._request('feed/data.json?', params)
        
        datastr = resp.text
        dataarr = np.array(eval(datastr))
        data = pd.Series(data=dataarr[:,1], index=dataarr[:,0], name='data')
        
        logger.debug('Received %d values from feed %i',len(data), self.feedid)
        
        # The first and last values returned will be the nearest values to 
        # the specified timestamps and can be outside of the actual interval.
        # Those will be dropped to avoid additional index values when resampling
        data = data.ix[startstamp:endstamp]
        data.index = pd.to_datetime(data.index,unit='ms')
        data.index = data.index.tz_localize(self.connection.timezone).tz_convert(timezone)
        data.index.name = 'time'
        
        return data
