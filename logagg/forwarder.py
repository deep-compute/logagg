import time
import Queue
from threading import Thread

from deeputil import Dummy
from logagg import util

DUMMY_LOGGER = Dummy()

class LogForwarder(object):
    DESC = "Gets all the logs from nsq and stores in the storage engines"


    SLEEP_TIME = 1
    QUEUE_TIMEOUT = 1
    QUEUE_MAX_SIZE = 5000

    MAX_SECONDS_TO_PUSH = 1
    MAX_MESSAGES_TO_PUSH = 200

    def __init__(self, message_source, targets, log=DUMMY_LOGGER):

        self.message_source = message_source
        self.targets = targets
        self.log = log

    def start(self):

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
        self.handle_msg(self.message_source)

        th.join()

    def handle_msg(self, nsq_receiver):
        for msg in nsq_receiver:
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

    def _ack_messages(self, msgs):
        for msg in msgs:
            try:
                msg.fin()
            except (SystemExit, KeyboardInterrupt): raise
            except:
                self.log.exception('msg ack failed')

    def _write_messages(self, msgs):
        # FIXME: what if a target fails to handle logs properly and
        # throws an exception? we need to protect against that case.
        for one_target in self.targets:
            util.start_daemon_thread(one_target.handle_logs(msgs)).join
            self.log.debug('Starting to write messeges ', target=one_target)

