# -*- coding: utf-8 -*-
"""
    emonpy.mysql
    ~~~~~~~~~~~~


"""
from __future__ import annotations
import datetime as dt
import logging
import os
import pandas as pd
import pytz as tz
from mysql import connector
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
                 meta_dir='/var/opt/emoncms/mysql'):

        self.meta_dir = meta_dir
        self.database_root = database_root
        self.database_data = database_data
        self.connector = connector.connect(host=host,
                                           port=int(port),
                                           user=user,
                                           passwd=password)

        logger.debug('Opening connection to emoncms mysql server "%s@%s"', user, host)

    def create_feed(self, name, datatype, options=None, tag='', **kwargs):
        raise NotImplementedError()

    def create_table(self,
                     table_name,
                     column_time='time',
                     column_data='data',
                     data_type='FLOAT'):
        database = self.database_data
        insert = f"CREATE TABLE IF NOT EXISTS {database}.{table_name} " \
                 f"({column_time} INT UNSIGNED NOT NULL, " \
                 f"{column_data} {data_type})"
        self.connector.cursor().execute(insert)

    # create metafile in file_dir
    def create_meta(self, feedid, table_name):
        meta = {"table_name": str(table_name),
                "value_type": "FLOAT",
                "value_empty": "false",
                "start_time": "0"}

        self._write_meta(feedid, meta)

    def _write_meta(self, feedid, meta):
        file_path = os.path.join(self.meta_dir, '{id}.meta'.format(id=feedid))
        if not os.path.isdir(file_path):
            with open(file_path, "w") as meta_file:
                for key in meta:
                    meta_file.write(key + "=" + meta[key] + "\n")

    def read_meta(self, feedid):
        meta = {}
        with open(os.path.join(self.meta_dir, '{id}.meta'.format(id=feedid)), "r") as meta_file:
            for meta_line in meta_file.readlines(0):
                meta_data = meta_line.split('=')
                meta[meta_data[0].strip()] = meta_data[1].strip()

        return meta

    def feed(self, feedid: int):
        return MySqlFeed(self, feedid)


class MySqlFeed(Feed):

    # noinspection PyShadowingNames
    def __init__(self, connection: MysqlEmoncms, id: int):
        super().__init__(connection, id)
        self.meta = connection.read_meta(id)
        self.table_name = self.meta['table_name']

    # Read in field , with different transfer parameters the respective value is to be returned
    def get(self, field: str) -> any:
        cursor = self.connection.connector.cursor()
        database = self.connection.database_root
        insert = f"SELECT {field} FROM {database}.feeds WHERE id={self.id}"
        cursor.execute(insert)
        value = cursor.fetchone()
        return value[0]

    def set(self, fields: dict[str, str]) -> None:
        cursor = self.connection.connector.cursor()
        database = self.connection.database_root
        for key, value in fields.items():
            if isinstance(value, str):
                value = f"\"{value}\""
            insert = f"UPDATE {database}.feeds SET {key} = {value} WHERE id={self.id}"

        cursor.execute(insert)

    def data(self,
             start: pd.Timestamp | dt.datetime = None,
             end: pd.Timestamp | dt.datetime = None,
             interval: int = None,
             column=None,
             table=None,
             resolution=None,
             **kwargs):

        data = self._select(column, table, start, end)
        data.index.name = 'time'

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
            time = dt.datetime.fromtimestamp(timestamp, tz=tz.UTC)
            times.append(time)
            values.append(value)

        result = pd.DataFrame(data=values, index=times, columns=[column])
        return result

    # write values from Pandas Series into table
    def write(self, data: pd.Series, **_):
        database = self.connection.database_data
        insert = f"INSERT INTO {database}.{self.table_name} (time,data) VALUES ('%s', '%s') " \
                  "ON DUPLICATE KEY UPDATE data=VALUES(data)"

        epoch = dt.datetime(1970, 1, 1, tzinfo=tz.UTC)
        values = []
        for index, value in data.iteritems():
            time = (index.tz_convert(tz.UTC) - epoch).total_seconds()
            values.append((time, value))

        cursor = self.connection.connector.cursor()
        cursor.executemany(insert, values)
        self.connection.connector.commit()

        cursor.close()
