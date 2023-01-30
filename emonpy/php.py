# -*- coding: utf-8 -*-
"""
    emonpy.php
    ~~~~~

    
"""
from __future__ import annotations
import logging

logger = logging.getLogger('emonpy.http')

import pytz as tz
import pandas as pd
import datetime as dt
from struct import unpack

from .emoncms import Emoncms, Feed


class PhpEmoncms(Emoncms):

    def __init__(self, data_dir='/var/opt/emoncms', timezone='UTC') -> None:
        self.data_dir = data_dir
        self.timezone = timezone

        logger.debug('Registering connection to emoncms webserver "%s"', self.data_dir)

    def feed(self, feedid: int, **kwargs):
        return PhpFeed(self, feedid, **kwargs)


class PhpFeed(Feed):

    def __init__(self, connection: PhpEmoncms, feedid: int, name: str = None):
        super().__init__(connection, feedid)
        if name is None:
            name = f"feed_{self.id}"
        self.name = name
        self.data_file = connection.data_dir + f"/phptimeseries/{name}.MYD"

    def data(self,
             start: pd.Timestamp | dt.datetime = None,
             end: pd.Timestamp | dt.datetime = None) -> pd.Series:

        epoch = dt.datetime(1970, 1, 1, tzinfo=tz.UTC)
        if start is None:
            start = epoch.date()

        times = []
        data = []
        with open(self.data_file, 'rb') as file:
            line = file.read(9)
            while line:
                line_tuple = unpack("<xIf", line)
                timestamp = int(line_tuple[0])
                if timestamp > 0:
                    time = dt.datetime.utcfromtimestamp(timestamp)
                    # verify if in start/end

                    if end is None:
                        if start < time.date():
                            times.append(time)
                            value = float(line_tuple[1])
                            data.append(value)
                    else:
                        if start < time.date() < end:
                            times.append(time)
                            value = float(line_tuple[1])
                            data.append(value)
                line = file.read(9)

        feed = pd.Series(data=data, index=times, name=self.name)
        feed.index = feed.index.tz_localize(tz.utc)
        feed.index.name = 'time'
        feed = feed.loc[feed.index.year > 1970]

        # Drop rows with duplicate index, as this produces problems with reindexing
        feed = feed[~feed.index.duplicated(keep='last')]

        return feed
