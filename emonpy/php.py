# -*- coding: utf-8 -*-
"""
    emonpy.php
    ~~~~~~~~~~

    
"""
from __future__ import annotations
import logging
import pytz as tz
import pandas as pd
import datetime as dt
from struct import unpack
from .emoncms import Emoncms, Feed

logger = logging.getLogger('emonpy.php')


class PhpEmoncms(Emoncms):

    # noinspection PyShadowingNames
    def __init__(self, dir='/var/opt/emoncms') -> None:
        self.dir = dir

        logger.debug('Registering local emoncms PHP engine reader at "%s"', self.dir)

    # noinspection PyShadowingNames
    def feed(self, id: int, **kwargs):
        return PhpFeed(self, id, **kwargs)


class PhpFeed(Feed):

    def __init__(self, connection: PhpEmoncms, feedid: int, name: str = None):
        super().__init__(connection, feedid)
        if name is None:
            name = f"feed_{self.id}"
        self.name = name
        self.file = connection.dir + f"/phptimeseries/feed_{self.id}.MYD"

    def data(self,
             start: pd.Timestamp | dt.datetime = None,
             end: pd.Timestamp | dt.datetime = None) -> pd.Series:

        epoch = dt.datetime(1970, 1, 1, tzinfo=tz.UTC)
        if start is None:
            start = epoch.date()

        times = []
        data = []
        with open(self.file, 'rb') as file:
            line = file.read(9)
            while line:
                line_tuple = unpack("<xIf", line)
                timestamp = int(line_tuple[0])
                if timestamp > 0:
                    time = pd.Timestamp(dt.datetime.utcfromtimestamp(timestamp)).tz_localize(tz.UTC)
                    if time >= start and (end is not None and time <= end):
                        times.append(time)
                        data.append(float(line_tuple[1]))
                line = file.read(9)

        feed = pd.Series(data=data, index=times, name=self.name)
        feed.index.name = 'time'
        feed = feed.loc[feed.index.year > 1970]

        # Drop rows with duplicate index, as this produces problems with reindexing
        feed = feed[~feed.index.duplicated(keep='last')]

        return feed