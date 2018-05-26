import abc

import ujson as json
from deeputil import keeprunning
from logagg.util import DUMMY


class BaseForwarder():
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self):
        pass

    @abc.abstractmethod
    def _ensure_connection(self):
        pass

    @abc.abstractmethod
    def handle_logs(self, msgs):
        pass


import pymongo
from pymongo import MongoClient


class MongoDBForwarder(BaseForwarder):
    SERVER_SELECTION_TIMEOUT = 500  # MongoDB server selection timeout

    # FIXME: normalize all var names
    def __init__(self,
                 host, port,
                 user, password,
                 db, collection, log=DUMMY):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = password
        self.db_name = db
        self.coll = collection
        self.log = log

        if host != 'no_host':
            self._ensure_connection()

    # FIXME: clean up the logs
    @keeprunning(wait_secs=SERVER_SELECTION_TIMEOUT, exit_on_success=True)
    def _ensure_connection(self):
        # Establish connection to MongoDB to store the nsq messages
        url = 'mongodb://%s:%s@%s:%s' % (self.user,
                                         self.passwd,
                                         self.host,
                                         self.port)
        client = MongoClient(
            url, serverSelectionTimeoutMS=self.SERVER_SELECTION_TIMEOUT)
        self.log.info('mongodb_server_connection_established', host=self.host)
        self.database = client[self.db_name]
        self.log.info('mongodb_database_created', db=self.db_name)
        self.collection = self.database[self.coll]
        self.log.info('mongodb_collection_created',
                      collection=self.collection, db=self.db_name)

    def _parse_msg_for_mongodb(self, msgs):
        '''
        >>> mdbf = MongoDBForwarder('no_host', '27017', 'deadpool',
        ...                             'chimichanga', 'logs', 'collection')
        >>> log = [{u'data': {u'_': {u'file': u'log.py',
        ...                    u'fn': u'start',
        ...                    u'ln': 8,
        ...                    u'name': u'__main__'},
        ...             u'a': 1,
        ...             u'b': 2,
        ...             u'msg': u'this is a dummy log'},
        ...   u'error': False,
        ...   u'error_tb': u'',
        ...   u'event': u'some_log',
        ...   u'file': u'/var/log/sample.log',
        ...   u'formatter': u'logagg.formatters.basescript',
        ...   u'host': u'deepcompute',
        ...   u'id': u'20180409T095924_aec36d313bdc11e89da654e1ad04f45e',
        ...   u'level': u'info',
        ...   u'raw': u'{...}',
        ...   u'timestamp': u'2018-04-09T09:59:24.733945Z',
        ...   u'type': u'metric'}]

        >>> records = mdbf._parse_msg_for_mongodb(log)
        >>> from pprint import pprint
        >>> pprint(records)
        [{'_id': u'20180409T095924_aec36d313bdc11e89da654e1ad04f45e',
          u'data': {u'_': {u'file': u'log.py',
                           u'fn': u'start',
                           u'ln': 8,
                           u'name': u'__main__'},
                    u'a': 1,
                    u'b': 2,
                    u'msg': u'this is a dummy log'},
          u'error': False,
          u'error_tb': u'',
          u'event': u'some_log',
          u'file': u'/var/log/sample.log',
          u'formatter': u'logagg.formatters.basescript',
          u'host': u'deepcompute',
          u'level': u'info',
          u'raw': u'{...}',
          u'timestamp': u'2018-04-09T09:59:24.733945Z',
          u'type': u'metric'}]
        '''
        msgs_list = []
        for msg in msgs:
            try:
                msg['_id'] = msg.pop('id')
            except KeyError:
                self.log.exception('collector_failure_id_not_found', log=msg)
            msgs_list.append(msg)
        return msgs_list

    def handle_logs(self, msgs):
        msgs_list = self._parse_msg_for_mongodb(msgs)
        try:
            self.log.debug('inserting_msgs_mongodb')
            self.collection.insert_many(msgs_list, ordered=False)
            self.log.info('logs_inserted_into_mongodb',
                          num_records=len(msgs), type='metric')
        except pymongo.errors.AutoReconnect(message='connection_to_mongodb_failed'):
            self._ensure_connection()
        except pymongo.errors.BulkWriteError as bwe:
            self.log.info('logs_inserted_into_mongodb',
                          num_records=bwe.details['nInserted'], type='metric',
                          records_not_inserted=bwe.details['writeErrors'],
                          num_records_missed=len(bwe.details['writeErrors']))


from influxdb import InfluxDBClient
from influxdb.client import InfluxDBClientError
from influxdb.client import InfluxDBServerError

from logagg.util import flatten_dict, is_number, MarkValue


class InfluxDBForwarder(BaseForwarder):
    EXCLUDE_TAGS = set(["id", "raw", "timestamp", "type", "event", "error"])

    def __init__(self,
                 host, port,
                 user, password,
                 db, collection, log=DUMMY):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = password
        self.db_name = db
        self.log = log

        if host != 'no_host':
            self._ensure_connection()

    def _ensure_connection(self):
        # Establish connection to influxDB to store metrics
        self.influxdb_client = InfluxDBClient(self.host, self.port, self.user,
                                              self.passwd, self.db_name)
        self.log.info('influxdb_server_connection_established', host=self.host)
        self.influxdb_database = self.influxdb_client.create_database(
            self.db_name)
        self.log.info('influxdb_database_created', dbname=self.db_name)

    def _tag_and_field_maker(self, event):
        '''
        >>> idbf = InfluxDBForwarder('no_host', '8086', 'deadpool',
        ...                             'chimichanga', 'logs', 'collection')
        >>> log = {u'data': {u'_': {u'file': u'log.py',
        ...                         u'fn': u'start',
        ...                         u'ln': 8,
        ...                         u'name': u'__main__'},
        ...             u'a': 1,
        ...             u'b': 2,
        ...             u'__ignore_this': 'some_string',
        ...             u'msg': u'this is a dummy log'},
        ...   u'error': False,
        ...   u'error_tb': u'',
        ...   u'event': u'some_log',
        ...   u'file': u'/var/log/sample.log',
        ...   u'formatter': u'logagg.formatters.basescript',
        ...   u'host': u'deepcompute',
        ...   u'id': u'20180409T095924_aec36d313bdc11e89da654e1ad04f45e',
        ...   u'level': u'info',
        ...   u'raw': u'{...}',
        ...   u'timestamp': u'2018-04-09T09:59:24.733945Z',
        ...   u'type': u'metric'}

        >>> tags, fields = idbf._tag_and_field_maker(log)
        >>> from pprint import pprint
        >>> pprint(tags)
        {u'data.msg': u'this is a dummy log',
         u'error_tb': u'',
         u'file': u'/var/log/sample.log',
         u'formatter': u'logagg.formatters.basescript',
         u'host': u'deepcompute',
         u'level': u'info'}
        >>> pprint(fields)
        {u'data._': "{u'ln': 8, u'fn': u'start', u'file': u'log.py', u'name': u'__main__'}",
         u'data.a': 1,
         u'data.b': 2}

        '''
        data = event.pop('data')
        data = flatten_dict({'data': data})

        t = dict((k, event[k]) for k in event if k not in self.EXCLUDE_TAGS)
        f = dict()

        for k in data:
            v = data[k]

            if is_number(v) or isinstance(v, MarkValue):
                f[k] = v
            else:
                #if v.startswith('_'): f[k] = eval(v.split('_', 1)[1])
                t[k] = v

        return t, f

    def _parse_msg_for_influxdb(self, msgs):
        '''
        >>> from logagg.forwarders import InfluxDBForwarder
        >>> idbf = InfluxDBForwarder('no_host', '8086', 'deadpool',
        ...                             'chimichanga', 'logs', 'collection')

        >>> valid_log = [{u'data': {u'_force_this_as_field': 'CXNS CNS nbkbsd',
        ...             u'a': 1,
        ...             u'b': 2,
        ...             u'msg': u'this is a dummy log'},
        ...   u'error': False,
        ...   u'error_tb': u'',
        ...   u'event': u'some_log',
        ...   u'file': u'/var/log/sample.log',
        ...   u'formatter': u'logagg.formatters.basescript',
        ...   u'host': u'deepcompute',
        ...   u'id': u'20180409T095924_aec36d313bdc11e89da654e1ad04f45e',
        ...   u'level': u'info',
        ...   u'raw': u'{...}',
        ...   u'timestamp': u'2018-04-09T09:59:24.733945Z',
        ...   u'type': u'metric'}]

        >>> pointvalues = idbf._parse_msg_for_influxdb(valid_log)
        >>> from pprint import pprint
        >>> pprint(pointvalues)
        [{'fields': {u'data._force_this_as_field': "'CXNS CNS nbkbsd'",
                     u'data.a': 1,
                     u'data.b': 2},
          'measurement': u'some_log',
          'tags': {u'data.msg': u'this is a dummy log',
                   u'error_tb': u'',
                   u'file': u'/var/log/sample.log',
                   u'formatter': u'logagg.formatters.basescript',
                   u'host': u'deepcompute',
                   u'level': u'info'},
          'time': u'2018-04-09T09:59:24.733945Z'}]

        >>> invalid_log = valid_log
        >>> invalid_log[0]['error'] = True
        >>> pointvalues = idbf._parse_msg_for_influxdb(invalid_log)
        >>> pprint(pointvalues)
        []

        >>> invalid_log = valid_log
        >>> invalid_log[0]['type'] = 'log'
        >>> pointvalues = idbf._parse_msg_for_influxdb(invalid_log)
        >>> pprint(pointvalues)
        []
        '''

        series = []

        for msg in msgs:
            if msg.get('error'):
                continue

            if msg.get('type').lower() == 'metric':
                time = msg.get('timestamp')
                measurement = msg.get('event')
                tags, fields = self._tag_and_field_maker(msg)
                pointvalues = {
                    "time": time,
                    "measurement": measurement,
                    "fields": fields,
                    "tags": tags}
                series.append(pointvalues)

        return series

    def handle_logs(self, msgs):

        self.log.debug('parsing_of_metrics_started')
        records = self._parse_msg_for_influxdb(msgs)
        self.log.debug('parsing_of_metrics_completed')

        try:
            self.log.debug('inserting_the_metrics_into_influxdb')
            self.influxdb_client.write_points(records)
            self.log.info('metrics_inserted_into_influxdb',
                          num_records=len(records),
                          type='metric')
        except (InfluxDBClientError, InfluxDBServerError) as e:
            self.log.exception('failed_to_insert metric',
                               record=records,
                               num_records=len(records),
                               type='metric')
