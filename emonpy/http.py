# -*- coding: utf-8 -*-
"""
    emonpy.http
    ~~~~~~~~~~~
    
    This module implements the HTTP communication with an emoncms web server.
    It provides inherited objects, to handle actual function calls to access 
    the webserver and e.g. post or retrieve data with :class:`HttpInput` 
    and :class:`HttpFeed` reference objects.
    
"""
import logging
import numpy as np
import pandas as pd
import pytz as tz
import datetime as dt
import requests
import json

from .emoncms import EmoncmsException, Emoncms, Input, Feed

logger = logging.getLogger('emonpy.http')


class HttpEmoncms(Emoncms):

    def __init__(self, address='http://localhost/', apikey='', timezone=tz.UTC):
        self.timezone = timezone
        self.address = address
        self.apikey = apikey

        logger.debug('Registering connection to emoncms webserver "%s"', self.address)

    def input(self, node, name):
        return HttpInput(self, node, name)

    def post(self, data, time=None, **kwargs):
        logger.debug('Requesting to bulk post data')

        if time is None:
            time = dt.datetime.now(tz.utc)

        # Convert time to UTC UNIX timestamp in seconds
        parameters = {
            'time': pd.to_datetime(time).tz_convert(self.timezone).value // 10 ** 9,  # .astype(np.int64)//10**9
            'data': json.dumps(data.parse(time))
        }
        return self._request('input/bulk?', parameters, method='POST', **kwargs)

    def create_feed(self, name, datatype, engine, options=None, tag='', **kwargs):
        logger.debug('Requesting to create feed with name "%s"', name)

        parameters = {
            'name': name,
            'tag': tag,
            'datatype': datatype,
            'engine': engine
        }

        if options is not None:
            parameters['options'] = options

        response = self._request_json('feed/create.json?', parameters, **kwargs)
        return HttpFeed(self, int(response['feedid']))

    def list_feeds(self, **kwargs):
        logger.debug('Requesting to retrieve feed list')

        feeds_json = self._request_json('feed/list.json?', **kwargs)
        feeds = []
        for feed in feeds_json:
            feeds.append(HttpFeed(self, feed))

        return feeds

    def feed(self, feedid):
        return HttpFeed(self, feedid)

    def fetch(self, feeds):
        logger.debug('Requesting to fetch last values of feed list')

        parameters = {'ids': ','.join(str(feed._id) for feed in feeds.values())}
        return self._request_json('feed/fetch.json?', parameters)

    def _request(self, action, parameters, method='GET', **kwargs):
        if 'apikey' in kwargs:
            parameters['apikey'] = kwargs.get('apikey')
        else:
            parameters['apikey'] = self.apikey

        if method.upper() == 'POST':
            response = requests.post(self.address + action, data=parameters)
        else:
            response = requests.get(self.address + action, params=parameters)

        if response.status_code != 200:
            raise EmoncmsException("Response returned with error " + str(response.status_code) + ": " + response.reason)

        if response.text == 'false':
            raise EmoncmsException("Response returned false")

        return response.text

    def _request_json(self, action, parameters=None, method='GET', **kwargs):
        if parameters is None:
            parameters = {}
        response_text = self._request(action, parameters, method=method, **kwargs)
        try:
            response = json.loads(response_text)
            if 'success' in response and not response['success']:
                raise EmoncmsException("Response returned with error: " + response['message'])

            return response

        except ValueError:
            raise EmoncmsException("Invalid JSON String returned to be parsed: " + response_text)


class HttpInput(Input):

    def post(self, value, time=None, **kwargs):
        logger.debug('Requesting to post data to input %s of node %s: %d', self.node, self.node, value)

        parameters = {"fulljson": json.dumps({self.name: value})}
        if time is not None:
            # Convert time to UTC UNIX timestamp in seconds
            parameters["time"] = pd.to_datetime(time).tz_convert(self.connection.timezone).value // 10 ** 9

        response = self.connection._request_json('input/post/' + str(self.node) + '?', parameters, method='POST',
                                                 **kwargs)

        return response['success']


class HttpFeed(Feed):

    def data(self, start, end, interval, timezone='UTC', **kwargs):
        logger.debug('Requesting data from feed %i', self._id)

        # Convert times to UTC UNIX timestamps
        startstamp = pd.to_datetime(start).tz_convert(self.connection.timezone).value // 10 ** 6
        endstamp = pd.to_datetime(end).tz_convert(self.connection.timezone).value // 10 ** 6

        parameters = {'id': self._id,
                      'start': startstamp,
                      'end': endstamp,
                      'interval': interval}

        datastr = self.connection._request('feed/data.json?', parameters, **kwargs)
        dataarr = np.array(eval(datastr))
        if len(dataarr) > 0:
            data = pd.Series(data=dataarr[:, 1], index=dataarr[:, 0], name='data')

            logger.debug('Received %d values from feed %i', len(data), self._id)

            # The first and last values returned will be the nearest values to 
            # the specified timestamps and can be outside of the actual interval.
            # Those will be dropped to avoid additional index values when resampling
            data = data.ix[startstamp:endstamp]
            data.index = pd.to_datetime(data.index, unit='ms')
            data.index = data.index.tz_localize(self.connection.timezone).tz_convert(timezone)
            data.index.name = 'time'
        else:
            data = pd.Series(name='data')

        return data

    def update(self, value, time, **kwargs):
        logger.debug('Requesting to update data point at %s of feed %i: %d',
                     time.strftime('%d.%m.%Y %H:%M:%S'), self._id, value)

        timestamp = pd.to_datetime(time).tz_localize(self.connection.timezone).value // 10 ** 9

        parameters = {'id': self._id,
                      'time': timestamp,
                      'value': float(value)}

        return self.connection._request('feed/update.json?', parameters, **kwargs)
