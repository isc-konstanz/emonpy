# -*- coding: utf-8 -*-
"""
    emonpy.emoncms
    ~~~~~
    
    This module provides basic communication with an emoncms web server.
    It provides the framework for specific connection objects, handling :class:`Input` 
    and :class:`Feed` reference objects, enabling the user to access the webserver 
    and e.g. post or retrieve data.
    
"""
import logging
logger = logging.getLogger('emonpy.emoncms')

import datetime
import pytz as tz

class Emoncms(object):
    """
    The Emoncms object implements basic communication and enables e.g. the acquisition 
    of :class:`Feed` reference objects by calling :func:`feed`.
    It holds the emoncms servers' address, API key and configured timezone.
    
    The connection may be specified to communicate with the webserver via several 
    possible methods:
    
    - HTTP:
        By default, the webservers HTTP API via basic TCP/IP will be utilized.
    
    :param address:
        the address of the emoncms server.
    :type address:
        str or unicode
    
    :param apikey: 
        the API key of the addressed emoncms user.
        May be either Read or Write key to retrieve data.
    :type apikey:
        str or unicode
    
    :param timezone: 
        the timezone, in which the data is logged and available on the emoncms webserver.
        See http://en.wikipedia.org/wiki/List_of_tz_database_time_zones for a list of 
        valid time zones.
    :type timezone:
        str or unicode
    """

    def __init__(self, address, api_key, timezone='UTC', method='HTTP'):
        if method.lower() == 'http':
            from . import http
            self.__class__ = http.HttpEmoncms
        else:
            raise ValueError('Invalid emoncms connection method "{}"'.method)
        
        self.__init__(address, api_key, timezone)
        
    
    def input(self, node, name):
        """
        Acquire a :class:`Input` reference object, enabling e.g. to post
        data to an emoncms input.
        
        :param node:
            the unique node identifier of the input.
        :type node:
            str or unicode
        
        :param name:
            the name of the input, unique for its specified node.
        :type name:
            str or unicode
        
        :returns: 
            the input object for the specified node and name.
        :rtype: 
            :class:`Input`
        """
        raise NotImplementedError()
        
    
    def create_feed(self, name, datatype, engine, options=None, tag=''):
        """
        Create a new feed on the emoncms web server and its 
        corresponding :class:`Feed` reference object.
        
        :param name:
            the unique name of the feed.
        :type name:
            str
        
        :param tag:
            the optional descriptional tag of the feed
        :type tag:
            str
        
        :param datatype:
            the datatype of the feed.
        :type datatype:
            int
        
        :param engine:
            the engine of the feed.
        :type engine:
            int
        
        :param options:
            the optional options, related to the selected engine.
        :type options:
            dict
        
        :returns: 
            the feed object for the newly created reference.
        :rtype: 
            :class:`Feed`
        """
        raise NotImplementedError()
        
    
    def list_feeds(self):
        """
        Acquire a list of all available :class:`Feed` reference objects, 
        enabling e.g. to retrieve logged emoncms feed data.
                
        :returns: 
            the list of feed objects, available for the authenticated emoncms user.
        :rtype: 
            lsit of :class:`Feed`
        """
        raise NotImplementedError()
        
    
    def feed(self, feedid):
        """
        Acquire a :class:`Feed` reference object, enabling e.g. to retrieve
        logged emoncms feed data.
        
        :param feedid:
            the unique identifier of the feed.
        :type id:
            int
        
        :returns: 
            the feed object for the specified identifier.
        :rtype: 
            :class:`Feed`
        """
        raise NotImplementedError()
    

class Input(object):
    
    def __init__(self, connection, node, name):
        self.connection = connection
        
        self.node = node
        self.name = name
    

class Feed(object):
    
    def __init__(self, connection, feed):
        self.connection = connection
        
        if type(feed) is int:
            self._id = feed
        
        elif type(feed) is str:
            self._id = int(feed.replace('"', ''))
        
        elif type(feed) is dict:
            self._id = int(feed['id'])
            self.userid = int(feed['userid'])
            self.name = feed['name']
            self.tag = feed['tag']
            self.datatype = int(feed['datatype'])
            self.engine = int(feed['engine'])
            
            if 'processList' in feed: 
                self.processes = feed['processList']
            else:
                self.processes = ''
            
            if feed['time'] is not None:
                self.time = tz.timezone(connection.timezone).localize(datetime.datetime.fromtimestamp(int(feed['time'])))
                self.value = float(feed['value'])
            
        else:
            raise EmoncmsException('Invalid feed type "{0}" passed while instantiation: {1}'.format(type(feed), str(feed)))
        
    
    def data(self, start, end, interval, timezone='UTC'):
        """
        Retrieves logged emoncms feed data and returns the fetched time values
        as pandas series.
        
        :param start:
            the time, from which feed data should be retrieved.
        :type start:
            :class:`pandas.tslib.Timestamp` or datetime
        
        :param end:
            the time, until which feed data should be retrieved.
        :type end:
            :class:`pandas.tslib.Timestamp` or datetime
            
        :param interval:
            the time interval between single retrieved values in milliseconds.
        :type interval:
            int
        
        :param timezone: 
            the timezone, to which the retrieved data will be converted to.
            See http://en.wikipedia.org/wiki/List_of_tz_database_time_zones for a list of 
            valid time zones.
        :type timezone:
            str or unicode
        
        :returns: 
            the retrieved feed data time values.
        :rtype: 
            :class:`pandas.Series`
        """
        raise NotImplementedError()
        
    
    def update(self, value, time):
        """
        Updates a single feed data point.
            
        :param value:
            the data point value that should be updated.
        :type value:
            float
        
        :param time:
            the time, a data point should be updated at.
        :type time:
            :class:`pandas.tslib.Timestamp` or datetime
        """
        raise NotImplementedError()
    

class EmoncmsException(Exception):
    """
    Raise if any parsing or connection problem regarding emoncms occurs.
    
    """
