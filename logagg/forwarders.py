import abc

import ujson as json
from deeputil import keeprunning
from logagg.util import DUMMY_LOGGER


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
                 db, collection, log=DUMMY_LOGGER):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = password
        self.db_name = db
        self.coll = collection
        self.log = log

        self._ensure_connection()


    # FIXME: clean up the logs
    @keeprunning(wait_secs=SERVER_SELECTION_TIMEOUT, exit_on_success=True)
    def _ensure_connection(self):
        # Establish connection to MongoDB to store the nsq messages
        url = 'mongodb://%s:%s@%s:%s' % (self.user,
                                         self.passwd,
                                         self.host,
                                         self.port)
        client = MongoClient(url, serverSelectionTimeoutMS=self.SERVER_SELECTION_TIMEOUT)
        self.log.info('mongodb_server_connection_established', host=self.host)
        self.database = client[self.db_name]
        self.log.info('mongodb_database_created', db=self.db_name)
        self.collection = self.database[self.coll]
        self.log.info('mongodb_collection_created' ,
                       collection=self.collection, db=self.db_name)

    def _parse_msg_for_mongodb(self, msgs):
        msgs_list = []
        #TODO: We need to do this by using iteration object.
        for msg in msgs:
            msg['_id'] = msg.pop('id')
            msgs_list.append(msg)
        return msgs_list

    def _insert_1by1(self, records):
        for r in records:
            try:
                self.collection.update({'_id': r['_id']}, r, upsert=True)
            except pymongo.errors.OperationFailure as opfail:
                self.log.exception('failed_to_insert_record_in_mongodb',
                                    record=r, tb=opfail.details)

    def handle_logs(self, msgs):
        msgs_list = self._parse_msg_for_mongodb(msgs)
        try:
            self.log.debug('inserting_msgs_mongodb')
            self.collection.insert_many(msgs_list, ordered=False)
            self.log.info('logs_inserted_into_mongodb', num_msgs=len(msgs), type='metric')
        except pymongo.errors.AutoReconnect(message='connection_to_mongodb_failed'):
            self._ensure_connection()
        except pymongo.errors.BulkWriteError as bwe:
            self.log.exception('bulk_write_to_mongodb_failed', tb=bwe.details)
            self._insert_1by1(msgs_list)


from influxdb import InfluxDBClient
from influxdb.client import InfluxDBClientError
from influxdb.client import InfluxDBServerError

from logagg.util import flatten_dict, is_number

class InfluxDBForwarder(BaseForwarder):
    EXCLUDE_TAGS = set(["id", "raw", "timestamp", "type", "event", "error"])

    def __init__(self,
                 host, port,
                 user, password,
                 db, collection, log=DUMMY_LOGGER):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = password
        self.db_name = db
        self.log = log

        self._ensure_connection()

    def _ensure_connection(self):
        # Establish connection to influxDB to store metrics
        self.influxdb_client = InfluxDBClient(self.host, self.port, self.user,
                    self.passwd, self.db_name)
        self.log.info('influxdb_server_connection_established', host=self.host)
        self.influxdb_database = self.influxdb_client.create_database(self.db_name)
        self.log.info('influxdb_database_created', dbname=self.db_name)

    def _tag_and_field_maker(self, event):

        data = event.pop('data')
        data = flatten_dict({'data': data})

        t = dict((k, event[k]) for k in event if k not in self.EXCLUDE_TAGS)
        f = dict()

        for k in data:
            v = data[k]

            if is_number(v):
                f[k] = v
            else:
                t[k] = v

        return t, f

    def _parse_msg_for_influxdb(self, msgs):
        series = []

        for msg in msgs:
            if msg.get('error') == True:
                continue

            if msg.get('type').lower() == 'metric':
                time = msg.get('timestamp')
                measurement = msg.get('event')
                tags, fields = self._tag_and_field_maker(msg)
                pointvalues = {
                        "time": time,
                        "measurement": measurement,
                        "fields": fields,
                        "tags": tags }
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
                           length=len(records),
                           type='metric')
        except (InfluxDBClientError, InfluxDBServerError) as e:
            self.log.exception('failed_to_insert metric',
                                record=records,
                                length=len(records))
