import json
import time
import Queue
from threading import Thread

import pymongo
from pymongo import MongoClient
from nsq.reader import Reader
from basescript import BaseScript

class LogForwarder(BaseScript):
    DESC = "Gets all the logs from nsq and stores in the storage engines"

    MAX_IN_FLIGHT = 100 # Number of messages to read from NSQ per shot
    QUEUE_MAX_SIZE = 5000
    SERVER_SELECTION_TIMEOUT = 500 # MongoDB server selection timeout

    SLEEP_TIME = 1
    QUEUE_TIMEOUT = 1
    MAX_SECONDS_TO_PUSH = 1
    MAX_MESSAGES_TO_PUSH = 200

    def __init__(self, log, args, nsqtopic, nsqchannel, nsqd_tcp_address, mongodb_server_url,\
            mongodb_port, mongodb_user_name, mongodb_password, mongodb_database, mongodb_collection):

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

        # Initialize a queue to carry messages between the
        # producer (nsq_reader) and the consumer (read_from_q)
        self.msgqueue = Queue.Queue(maxsize=self.QUEUE_MAX_SIZE)
        self.log.info('Created Queue object with max size of %d' % (QUEUE_MAX_SIZE))

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
                    self.log.info('Writing messages to MongoDB')
                    self._write_messages(msgs)
                    self._ack_messages(msgs)
                    self.log.info('Ack to nsq is done for %d msgs' % (len(msgs)))

                    msgs = []
                    last_push_ts = time.time()

            except (SystemExit, KeyboardInterrupt): raise
            except pymongo.errors.ServerSelectionTimeoutError:
                self.log.exception('Push to mongo and ack to nsq failed')

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
            msg_body = json.loads(msg.body)
            msg_body['_id'] = msg_body.pop('id')
            msgs_list.append(msg_body)
        try:
            self.mongo_coll.insert_many([msg for msg in msgs_list], ordered=False)
            self.log.info("inserted %d msgs into mongodb" % (len(msgs)))
        except pymongo.errors.BulkWriteError as bwe:
            self.log.exception('Write to mongo failed. Details: %s' % bwe.details)
