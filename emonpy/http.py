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
import json

from .emoncms import EmoncmsException, Emoncms, Input, Feed


class HttpEmoncms(Emoncms):
    
    def __init__(self, address, api_key, timezone='UTC'):
        self.address = address
        self.apikey = api_key
        self.timezone = timezone
        
        logger.debug('Registering connection to emoncms webserver "%s"', self.address)
        
    
    def input(self, node, name):
        return HttpInput(self, node, name)
        
    
    def create_feed(self, name, datatype, engine, options=None, tag=''):
        logger.debug('Requesting to create feed with name "%s"', name)
        
        parameters = {
            'name':name, 
            'tag':tag, 
            'datatype':datatype, 
            'engine':engine }
        
        if options is not None:
            parameters['options'] = options
        
        response = self._request_json('feed/create.json?', parameters)
        return HttpFeed(self, int(response['feedid']))
        
    
    def list_feeds(self):
        logger.debug('Requesting to retrieve feed list')
        
        feeds_json = self._request_json('feed/list.json?')
        feeds = []
        for feed in feeds_json:
            feeds.append(HttpFeed(self, feed))
        
        return feeds
        
    
    def feed(self, feedid):
        return HttpFeed(self, feedid)
        
    
    def _request(self, action, parameters={}):
        parameters['apikey'] = self.apikey
        
        response = requests.get(self.address + action, params=parameters)
        
        if response.text == 'false':
            raise EmoncmsException("Response returned false")
        
        return response
        
    
    def _request_json(self, action, parameters={}):
        response_text = self._request(action, parameters).text
        try:
            response = json.loads(response_text)
            if 'success' in response and not response['success']:
                raise EmoncmsException("Response returned with error: " + response['message'])
            
            return response
            
        except ValueError:
            raise EmoncmsException("Invalid JSON String returned to be parsed: " + response_text)
    

class HttpInput(Input):
    pass
    

class HttpFeed(Feed):
    
    def data(self, start, end, interval, timezone='UTC'):
        logger.debug('Requesting data from feed %i', self._id)

        # Convert times to UTC UNIX timestamps
        startstamp = pd.to_datetime(start).tz_convert(self.connection.timezone).value//10**6 #.astype(np.int64)//10**6
        endstamp = pd.to_datetime(end).tz_convert(self.connection.timezone).value//10**6 #.astype(np.int64)//10**6
        
        parameters = {'id': self._id, 
                      'start': startstamp, 
                      'end': endstamp, 
                      'interval': interval }
        
        response = self.connection._request('feed/data.json?', parameters)
        
        datastr = response.text
        dataarr = np.array(eval(datastr))
        data = pd.Series(data=dataarr[:,1], index=dataarr[:,0], name='data')
        
        logger.debug('Received %d values from feed %i',len(data), self._id)
        
        # The first and last values returned will be the nearest values to 
        # the specified timestamps and can be outside of the actual interval.
        # Those will be dropped to avoid additional index values when resampling
        data = data.ix[startstamp:endstamp]
        data.index = pd.to_datetime(data.index,unit='ms')
        data.index = data.index.tz_localize(self.connection.timezone).tz_convert(timezone)
        data.index.name = 'time'
        
        return data
        
    
    def update(self, value, time):
        logger.debug('Requesting to update data point at %s of feed %i: %d', time.strftime('%d.%m.%Y %H:%M:%S'), self._id, value)
        
        timestamp = pd.to_datetime(time).tz_convert(self.connection.timezone).value//10**9
        
        parameters = {'id': self._id, 
                      'time': timestamp, 
                      'value': float(value) }
        
        self.connection._request('feed/update.json?', parameters)
        
