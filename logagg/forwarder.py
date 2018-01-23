import json
import time
import Queue
from threading import Thread
import pymongo
from pymongo import MongoClient
from influxdb import InfluxDBClient
from influxdb.client import InfluxDBClientError
from influxdb.client import InfluxDBServerError
from nsq.reader import Reader
from basescript import BaseScript

class LogForwarder(BaseScript):
    DESC = "Gets all the logs from nsq and stores in the storage engines"

    MAX_IN_FLIGHT = 200 # Number of messages to read from NSQ per shot
    QUEUE_MAX_SIZE = 5000
    SERVER_SELECTION_TIMEOUT = 500 # MongoDB server selection timeout

    SLEEP_TIME = 1
    QUEUE_TIMEOUT = 1
    MAX_SECONDS_TO_PUSH = 1
    MAX_MESSAGES_TO_PUSH = 200

    API = 'api'
    METRIC = 'metric'
    NGINX_METRIC = 'nginx_metric'
    DJANGO_METRIC = 'django_metric'
    NGINX_HANDLER = 'logagg.collect.handlers.nginx_access'
    DJANGO_HANDLER = 'logagg.collect.handlers.django'

    INFLUXDB_RECORDS = []

    def __init__(self, log, args, nsqtopic, nsqchannel, nsqd_tcp_address, mongodb_server_url,\
            mongodb_port, mongodb_user_name, mongodb_password, mongodb_database, mongodb_collection,\
            influxdb_server_url, influxdb_port, influxdb_user_name, influxdb_password, influxdb_database):

        self.log = log
        self.args = args
        self.nsqtopic = nsqtopic
        self.nsqchannel = nsqchannel
        self.nsqd_tcp_address = nsqd_tcp_address
        self.mongodb_server_url = mongodb_server_url
        self.mongodb_port = mongodb_port
        self.mongodb_user_name = mongodb_user_name
        self.mongodb_password = mongodb_password
        self.mongodb_database = mongodb_database
        self.mongodb_collection = mongodb_collection

        self.influxdb_server_url = influxdb_server_url
        self.influxdb_port = influxdb_port
        self.influxdb_user_name = influxdb_user_name
        self.influxdb_password = influxdb_password
        self.influxdb_database = influxdb_database

    def start(self):

        # Establish connection to MongoDB to store the nsq messages
        url = 'mongodb://%s:%s@%s:%s' % (self.mongodb_user_name, self.mongodb_password,
                self.mongodb_server_url, self.mongodb_port)
        client = MongoClient(url, serverSelectionTimeoutMS=self.SERVER_SELECTION_TIMEOUT)
        self.log.info('Established connecton to MongoDB server: %s' % (self.mongodb_server_url))
        self.mongo_database = client[self.mongodb_database]
        self.log.info('Created database: %s at MongoDB' % (self.mongo_database))
        self.mongo_coll = self.mongo_database[self.mongodb_collection]
        self.log.info('Created collection: %s for MongoDB database %s' % (self.mongo_coll, self.mongo_database))

        # Establish connection to influxdb to store metrics
        self.influxdb_client = InfluxDBClient(self.influxdb_server_url, self.influxdb_port, self.influxdb_user_name,
                    self.influxdb_password, self.influxdb_database)
        self.log.info('Established connection to InfluxDB server: %s' % (self.influxdb_server_url))
        self.influxdb_database = self.influxdb_client.create_database(self.influxdb_database)
        self.log.info('Created database: %s at InfluxDB' % (self.influxdb_database))

        # Initialize a queue to carry messages between the
        # producer (nsq_reader) and the consumer (read_from_q)
        self.msgqueue = Queue.Queue(maxsize=self.QUEUE_MAX_SIZE)
        self.log.info('Created Queue object with max size of %d' % (self.QUEUE_MAX_SIZE))

        # Starts the thread which we get the messages from queue
        th = self.consumer_thread = Thread(target=self.read_from_q)
        th.daemon = True
        th.start()

        # Establish connection to nsq from where we get the logs
        # Since, it is a blocking call we are starting the reader here.
        self.log.info('Starting nsq reader')
        self.reader = Reader(self.args.nsqtopic, self.args.nsqchannel, nsqd_tcp_addresses=[self.args.nsqd_tcp_address])
        self.handle_msg(self.reader)

        th.join()

    def handle_msg(self, msg_reader):
        for msg in msg_reader:
            self.msgqueue.put(msg)

    def read_from_q(self):
        msgs = []
        last_push_ts = time.time()

        while True:
            try:
                msg = self.msgqueue.get(block=True, timeout=self.QUEUE_TIMEOUT)
                msgs.append(msg)

            except Queue.Empty:
                time.sleep(self.SLEEP_TIME)
                continue

            cur_ts = time.time()
            time_since_last_push = cur_ts - last_push_ts

            is_msg_limit_reached = len(msgs) >= self.MAX_MESSAGES_TO_PUSH
            is_max_time_elapsed = time_since_last_push >= self.MAX_SECONDS_TO_PUSH

            should_push = len(msgs) > 0 and (is_max_time_elapsed or is_msg_limit_reached)

            try:
                if should_push:
                    self.log.info('Writing %d messages to databases' % (len(msgs)))
                    self._write_messages(msgs)
                    self._ack_messages(msgs)
                    self.log.info('Ack to nsq is done for %d msgs' % (len(msgs)))
                    
                    msgs = []
                    last_push_ts = time.time()

            except (SystemExit, KeyboardInterrupt): raise
            except pymongo.errors.ServerSelectionTimeoutError:
                self.log.exception('Push to databases and ack to nsq failed')

    def parse_msg_to_send_influxdb(self, msgs_list):
        series = []
        for msg in msgs_list:
            if msg.get('error'):
                continue
            time = msg.get('timestamp')
            if msg.get('type') == self.METRIC:
                baseScript_metric = self.parse_baseScript_metric(msg)
                series.append(baseScript_metric)

            elif msg.get('handler') == self.NGINX_HANDLER:
                nginx_metric = self.parse_nginx_metric(msg)
                series.append(nginx_metric)

            elif msg.get('handler') == self.DJANGO_HANDLER:
                django_metric = self.parse_django_metric(msg)
                series.append(django_metric)
        return series


    def parse_nginx_metric(self, msg):
        event = msg.get('data', {})
        measurement = self.NGINX_METRIC
        time = msg.get('timestamp')
        #to be stored as tags
        host =  msg.get('host', '')
        request = event.get('request', '')
        status = event.get('status')
        remote_addr = event.get('remote_addr', '')
        
        #feilds that are metrics
        request_time = event.get('request_time')
        body_bytes_sent = event.get('body_bytes_sent')
        upstream_response_time = event.get('upstream_response_time')
        connection_requests = event.get('connection_requests')
        
        pointValues = {
            "time": time,
            "measurement": measurement,
            "fields": {
        	 "request_time": request_time,
        	 "body_bytes_sent": body_bytes_sent,
        	 "upstream_response_time": upstream_response_time,
        	 "connection_requests": connection_requests
                    },
            "tags": {
                "host": host,
                "request": request,
                "status": status,
                "remote_addr": remote_addr
                    }
            }
        return pointValues


    def parse_baseScript_metric(self, msg):
        time = msg.get('timestamp')
        event = msg.get('data').get('event')
        measurement = event.get('req_fn')
        tags = dict()
        metrics = dict()
        for key in event:
            if key == 'timestamp' and key == 'req_fn':
                pass
            elif isinstance(event[key], basestring):
                tags[key] = event[key]
            elif isinstance(event[key], (int,float)):
                metrics[key] = event[key]     
        pointValues = {
                "time": time,
                "measurement": measurement,
                "fields": metrics,
                "tags": tags
               }
        return pointValues


    def parse_django_metric(self, msg):
        time = msg.get('timestamp')
	data = msg.get('data')
	loglevel = data.get('loglevel')
	host = msg.get('host')
	if isinstance(data, dict) and isinstance(data.get('message'), dict):
	    event = data.get('message')
	    if 'processing_time' in event:
		url = event.get('url')
		user = event.get('user')
		method = event.get('method')
		processing_time = event.get('processing_time')
		pointValues = {
		    "time": time,
		    "measurement": self.DJANGO_METRIC,
		    "fields": {
                        "processing_time": processing_time,
		        },
		    "tags": {
		        "host": host,
		        "url": url,
		        "user": user,
		        "loglevel": loglevel,
		        "method" : method,
		        }
		    }
                return pointValues


    def _ack_messages(self, msgs):
        for msg in msgs:
            try:
                msg.fin()
            except (SystemExit, KeyboardInterrupt): raise
            except:
                self.log.exception('msg ack failed')

    def _write_messages(self, msgs):
        msgs_list = []
        #TODO: We need to do this by using iteration object.
        for msg in msgs:
            msg_body = json.loads(msg.body.decode(encoding='utf-8',errors='strict'))
            msg_body['_id'] = msg_body.pop('id')
            msgs_list.append(msg_body)

        try:
            self.log.info('inserting %d msgs into mongodb' % (len(msgs)))
            self.mongo_coll.insert_many([msg for msg in msgs_list], ordered=False)
            self.log.info("inserted %d msgs into mongodb" % (len(msgs)))
        except pymongo.errors.BulkWriteError as bwe:
            self.log.exception('Write to mongo failed. Details: %s' % bwe.details)

        self.log.info('Parsing of metrics started')
        records = self.parse_msg_to_send_influxdb(msgs_list)
        self.INFLUXDB_RECORDS.extend(records)
        self.log.info('Parsing of metrics is completed')

        if self.INFLUXDB_RECORDS and len(self.INFLUXDB_RECORDS) >= 200:
            self.INFLUXDB_RECORDS = [record for record in self.INFLUXDB_RECORDS if record]
            print 'record length %d' %(len(self.INFLUXDB_RECORDS))
            try:
                self.log.info('inserting the %d metrics into influxdb' % (len(self.INFLUXDB_RECORDS)))
                self.influxdb_client.write_points(self.INFLUXDB_RECORDS)
                self.log.info("inserted the metrics into influxdb %d" % (len(self.INFLUXDB_RECORDS)))
                self.INFLUXDB_RECORDS = []
            except (InfluxDBClientError, InfluxDBServerError) as e:
                self.log.exception("failed to insert metric %s" % (self.INFLUXDB_RECORDS))
