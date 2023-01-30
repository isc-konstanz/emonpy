# -*- coding: utf-8 -*-
"""
    emonpy.mysql
    ~~~~~

    
"""
from __future__ import annotations
import datetime as dt
import logging
import os
import pandas as pd
import pytz as tz
from mysql import connector
from th_e_core.tools import to_int
from .emoncms import Emoncms, Feed


logger = logging.getLogger('emonpy.mysql')


class MysqlEmoncms(Emoncms):

    def __init__(self,
                 host="127.0.0.1",
                 port=3306,
                 user='emoncms',
                 password='',
                 database_root='emoncms',
                 database_data='emondata',
                 data_dir='/var/opt/emoncms/mysql'):

        self.data_dir = data_dir
        self.database_root = database_root
        self.database_data = database_data
        self.connector = connector.connect(
                                            host=host,
                                            port=to_int(port),
                                            user=user,
                                            passwd=password
                                          )
        logger.debug('Opening connection to emoncms mysql server "%s@%s"', user, host)

    def create_feed(self, name, datatype, options=None, tag='', **kwargs):
        raise NotImplementedError()

    # create table in emondata
    def create_table(self,
                     table_name,
                     column_time='time',
                     column_data='data',
                     data_type='FLOAT'):
        insert = "CREATE TABLE IF NOT EXISTS emondata.{name} " \
                 "({column_time} INT UNSIGNED NOT NULL, " \
                 "{column_data} {data_type})".format(
                                                     name=table_name,
                                                     column_time=column_time,
                                                     column_data=column_data,
                                                     data_type=data_type
                                                    )
        self.connector.cursor().execute(insert)

    # create metafile in file_dir
    def create_meta(self, feedid, table_name):
        meta = {"table_name": str(table_name),
                "value_type": "FLOAT",
                "value_empty": "false",
                "start_time": "0"}

        self._write_meta(feedid, meta)

    def _write_meta(self, feedid, meta):
        file_dir = os.path.join(self.data_dir, '{id}.meta'.format(id=feedid))
        if os.path.exists(file_dir) == False:
            with open(file_dir, "w") as meta_file:
                for key in meta:
                    meta_file.write(key + "=" + meta[key] + "\n")

    def read_meta(self, feedid):
        meta = {}
        with open(os.path.join(self.data_dir, '{id}.meta'.format(id=feedid)), "r") as meta_file:
            for meta_line in meta_file.readlines(0):
                meta_data = meta_line.split('=')
                meta[meta_data[0].strip()] = meta_data[1].strip()

        return meta

    def feed(self, feedid: int):
        return MySqlFeed(self, feedid)


class MySqlFeed(Feed):

    def __init__(self, connection: MysqlEmoncms, feedid: int):
        super().__init__(connection, feedid)
        self.meta = connection.read_meta(feedid)

        self.table_name = self.meta['table_name']

    # Read in field , with different transfer parameters the respective value is to be returned
    def get(self, field: str) -> any:
        cursor = self.connection.connector.cursor()
        insert = "SELECT {field} FROM {database}.{table} WHERE id={feedid}".format(
                                                                                database=self.connection.database_root,
                                                                                field=field,
                                                                                table="feeds",
                                                                                feedid=self._id
                                                                                  )
        cursor.execute(insert)
        value = cursor.fetchone()
        return value[0]

    def set(self, fields: dict([str, str])) -> None:
        cursor = self.connection.connector.cursor()
        key_list = list(fields.keys())
        value_list = list(fields.values())
        if type(value_list[0]) == str:
            value_string_list = ("\"" + value_list[0] + "\"")
            insert = "UPDATE emoncms.feeds SET {key} = {value} WHERE id={feed_id}".format(key=str(key_list[0]),
                                                                                          value=str(value_string_list),
                                                                                          feed_id=self._id)
        else:
            insert = "UPDATE emoncms.feeds SET {key} = {value} WHERE id={feed_id}".format(key=str(key_list[0]),
                                                                                          value=str(value_list[0]),
                                                                                          feed_id=self._id)

        cursor.execute(insert)

    def data(self,
             start: pd.Timestamp | dt.datetime = None,
             end: pd.Timestamp | dt.datetime = None,
             interval: int = None,
             column=None, table=None, resolution=None, **kwargs):

        try:
            data = self._select(column, table, start, end)
            data.index.name = 'time'

        except TypeError as e:
            print(e)

        if resolution is not None and resolution > 900:
            offset = (start - start.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds() % resolution
            data = data.resample(str(int(resolution)) + 's', base=offset).sum()

        return data

    def _select(self,
                column: str,
                table: str,
                start: pd.Timestamp | dt.datetime = None,
                end: pd.Timestamp | dt.datetime = None) -> pd.DataFrame:

        epoch = dt.datetime(1970, 1, 1, tzinfo=tz.UTC)
        if start is None:
            start = epoch

        cursor = self.connector.cursor()
        select = "SELECT time, data FROM {0} WHERE ".format(table)
        if end is None:
            select += "time >= %s ORDER BY time ASC"
            cursor.execute(select, ((start.astimezone(tz.UTC) - epoch).total_seconds(),))
        else:
            select += "time BETWEEN %s AND %s ORDER BY time ASC"
            cursor.execute(select,
                           ((start.astimezone(tz.UTC) - epoch).total_seconds(),
                            (end.astimezone(tz.UTC) - epoch).total_seconds()))

        times = []
        values = []
        for timestamp, value in cursor.fetchall():
            time = dt.datetime.fromtimestamp(timestamp, tz=self.timezone)
            times.append(time)
            values.append(value)

        result = pd.DataFrame(data=values, index=times, columns=[column])
        return result.tz_convert(self.timezone)

    # write values from Pandas Series into table
    def write(self, data: pd.Series, **_):
        epoch = dt.datetime(1970, 1, 1, tzinfo=tz.UTC)

        insert = "INSERT INTO {database}.{tablename} (time,data) VALUES ('%s', '%s') " \
                 "ON DUPLICATE KEY UPDATE data=VALUES(data)".format(database=self.connection.database_data,
                                                                    tablename=self.table_name)

        values = []
        for index, value in data.iteritems():
            time = (index.tz_convert("UTC") - epoch).total_seconds()
            values.append((time, value))

        cursor = self.connection.connector.cursor()
        cursor.executemany(insert, values)
        self.connection.connector.commit()

        cursor.close()
