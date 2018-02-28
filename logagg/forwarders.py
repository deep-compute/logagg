import abc
import json

class BaseForwarder():
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self):
        pass

    @abc.abstractmethod
    def handle_logs(self, msgs):
        pass


import pymongo
from pymongo import MongoClient
from logagg import util

class MongoDBForwarder(BaseForwarder):
    SERVER_SELECTION_TIMEOUT = 500  # MongoDB server selection timeout


    # FIXME: normalize all var names
    def __init__(self,
                 host, port,
                 user, password,
                 db, collection):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = password
        self.db_name = db
        self.coll = collection

        self.log = util.DUMMY_LOGGER
        self._ensure_connection()


    # FIXME: clean up the logs
    def _ensure_connection(self):
        # Establish connection to MongoDB to store the nsq messages
        url = 'mongodb://%s:%s@%s:%s' % (self.user,
                                         self.passwd,
                                         self.host,
                                         self.port)
        client = MongoClient(url, serverSelectionTimeoutMS=self.SERVER_SELECTION_TIMEOUT)
        self.log.info('Established connecton to MongoDB server: %s' % (self.host))
        self.database = client[self.db_name]
        self.log.info('Created database: %s at MongoDB' % (self.db_name))
        self.collection = self.database[self.coll]
        self.log.info('Created collection: %s for MongoDB database %s' % (self.collection, self.db_name))

    def handle_logs(self, msgs):
        msgs_list = []
        #TODO: We need to do this by using iteration object.
        for msg in msgs:
            msg_body = json.loads(msg.body.decode(encoding='utf-8',errors='strict'))
            msg_body['id'] = msg_body.pop('id')
            msgs_list.append(msg_body)

        try:
            self.log.info('inserting %d msgs into mongodb' % (len(msgs)))
            self.collection.insert_many([msg for msg in msgs_list], ordered=False)
            self.log.info("inserted %d msgs into mongodb" % (len(msgs)))
        except pymongo.errors.BulkWriteError as bwe:
            self.log.exception('Write to mongo failed. Details: %s' % bwe.details)
        except pymongo.errors.ServerSelectionTimeoutError:
            self.log.exception('Push to databases and ack to nsq failed')


from influxdb import InfluxDBClient
from influxdb.client import InfluxDBClientError
from influxdb.client import InfluxDBServerError

class InfluxDBForwarder(BaseForwarder):
    INFLUXDB_RECORDS = []
    MIN_RECORDS_TO_PUSH = 100

    def __init__(self,
                 host, port,
                 user, password,
                 db, collection):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = password
        self.db_name = db

        self.log = util.DUMMY_LOGGER
        self._ensure_connection()

    def _ensure_connection(self):
        # Establish connection to influxdb to store metrics
        self.influxdb_client = InfluxDBClient(self.host, self.port, self.user,
                    self.passwd, self.db_name)
        self.log.info('Established connection to InfluxDB server: %s' % (self.host))
        self.influxdb_database = self.influxdb_client.create_database(self.influxdb_database)
        self.log.info('Created database: %s at InfluxDB' % (self.db_name))


    def handle_logs(self, logs):
        msgs_list = []

        self.log.info('Parsing of metrics started')
        records = self.parse_msg_to_send_influxdb(msgs_list)
        self.INFLUXDB_RECORDS.extend(records)
        self.log.info('Parsing of metrics is completed')

        if len(self.INFLUXDB_RECORDS) >= self.MIN_RECORDS_TO_PUSH:
            self.INFLUXDB_RECORDS = [record for record in self.INFLUXDB_RECORDS if record]
            print 'record length %d' %(len(self.INFLUXDB_RECORDS))
            try:
                self.log.info('inserting the %d metrics into influxdb' % (len(self.INFLUXDB_RECORDS)))
                self.influxdb_client.write_points(self.INFLUXDB_RECORDS)
                self.log.info("inserted the metrics into influxdb %d" % (len(self.INFLUXDB_RECORDS)))
                self.INFLUXDB_RECORDS = []
            except (InfluxDBClientError, InfluxDBServerError) as e:
                self.log.exception("failed to insert metric %s" % (self.INFLUXDB_RECORDS))
