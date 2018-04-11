import time
import Queue
from threading import Thread
from copy import deepcopy
from multiprocessing.pool import ThreadPool

from logagg import util
import ujson as json


class LogForwarder(object):
    DESC = "Gets all the logs from nsq and stores in the storage engines"

    QUEUE_EMPTY_SLEEP_TIME = 0.1
    QUEUE_TIMEOUT = 1
    QUEUE_MAX_SIZE = 50000

    MAX_SECONDS_TO_PUSH = 1
    MAX_MESSAGES_TO_PUSH = 200

    WAIT_TIME_TARGET_FAILURE = 2

    def __init__(self, message_source, targets, log=util.DUMMY):

        self.message_source = message_source
        self.targets = targets
        self.log = log
        self._pool = ThreadPool()

    def start(self):

        # Initialize a queue to carry messages between the
        # producer (nsq_reader) and the consumer (read_from_q)
        self.msgqueue = Queue.Queue(maxsize=self.QUEUE_MAX_SIZE)
        self.log.info('created_Queue_object', size=(self.QUEUE_MAX_SIZE))

        # Starts the thread which we get the messages from queue
        th = self.consumer_thread = Thread(target=self.read_from_q)
        th.daemon = True
        th.start()

        # Establish connection to nsq from where we get the logs
        # Since, it is a blocking call we are starting the reader here.
        self.log.debug('starting_nsq_reader')
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
                time.sleep(self.QUEUE_EMPTY_SLEEP_TIME)
                continue

            cur_ts = time.time()
            time_since_last_push = cur_ts - last_push_ts

            is_msg_limit_reached = len(msgs) >= self.MAX_MESSAGES_TO_PUSH
            is_max_time_elapsed = time_since_last_push >= self.MAX_SECONDS_TO_PUSH

            should_push = len(msgs) > 0 and (
                is_max_time_elapsed or is_msg_limit_reached)

            try:
                if should_push:
                    self.log.debug('writing_messages_to_databases')
                    self._write_messages(msgs)
                    self._ack_messages(msgs)
                    self.log.debug('ack_to_nsq_is_done_for_msgs',
                        num_msgs=len(msgs))

                    msgs = []
                    last_push_ts = time.time()

            except (SystemExit, KeyboardInterrupt):
                raise

    def _ack_messages(self, msgs):
        for msg in msgs:
            try:
                msg.fin()
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException:
                self.log.exception('msg_ack_failed')

    def _send_msgs_to_target(self, target, msgs):
        while True:
            try:
                target.handle_logs(msgs)
                break
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException:
                # FIXME: do we log the failed messages themselves somewhere?
                self.log.exception(
                    '_send_msgs_to_target_failed', target=target)
                time.sleep(self.WAIT_TIME_TARGET_FAILURE)
                # FIXME: also implement some sort of backoff sleep

    def _write_messages(self, msgs):
        fn = self._send_msgs_to_target
        msgs = [json.loads(m.body) for m in msgs]

        jobs = []
        for t in self.targets:
            jobs.append(self._pool.apply_async(fn, (t, deepcopy(msgs))))

        for j in jobs:
            j.wait()
