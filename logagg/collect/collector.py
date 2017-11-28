import os
import sys
import time
import json
import glob
import uuid
import Queue
import socket
import operator
import datetime
import traceback
from threading import Thread

import requests
from pygtail import Pygtail
from basescript import BaseScript

HOST = socket.gethostname()
MPUB_URL = 'http://%s/mpub?topic=%s'
STATS_URL = 'http://%s/stats?format=json&topic=%s'

# TODO
'''
What if requests session expires?
After a downtime of collector, pygtail is missing logs from rotational files
'''

class LogCollector(BaseScript):
    DESC = 'Collects the log information and sends to NSQChannel'

    QUEUE_MAX_SIZE = 2000
    MAX_MSGS_TO_PUSH = 100
    MAX_SECONDS_TO_PUSH = 1

    TIME_WAIT = 0.25
    SLEEP_TIME = 1
    QUEUE_TIMEOUT = 1
    PYGTAIL_ACK_WAIT_TIME = 0.05
    WAIT_TIME_TO_CHECK_DEPTH_AT_NSQ = 5

    def __init__(self, log, args, _file, nsqtopic, nsqchannel, nsqd_http_address, depth_limit_at_nsq, exception_logs_file):
        self.log = log
        self.args = args
        self.file = _file
        self.nsqtopic = nsqtopic
        self.nsqchannel = nsqchannel
        self.nsqd_http_address = nsqd_http_address
        self.depth_limit_at_nsq = depth_limit_at_nsq
        self.exception_logs_file = open(exception_logs_file, 'w')

    def _load_handler_fn(self, imp):
        self.log.info('Entered the _load_handler_fn')
        module_name, fn_name = imp.split('.', 1)
        module = __import__(module_name)
        fn = operator.attrgetter(fn_name)(module)
        self.log.info('Loaded the function for %s-%s' % (module_name, fn_name))
        return fn

    def _collect_log_lines(self, log_file):
        L = log_file
        self.log.info('Going to read log lines from the file %s' % (L['fpath']))
        freader = Pygtail(L['fpath'])
        for line_info in freader:
            _id = uuid.uuid1().hex
            line = line_info['line'][:-1] # remove new line char at the end
            log = dict(
                    id=uuid.uuid1().hex,
                    file=L['fpath'],
                    host=HOST,
                    handler=L['handler'],
                    raw=line,
                    timestamp=datetime.datetime.utcnow().isoformat()
                  )

            try:
                _log = L['handler_fn'](line)
                log.update(_log)
                self.validate_log_format(log)
            except (SystemExit, KeyboardInterrupt) as e: raise
            except:
                self.log.exception('Error during handling log line', log=log)
                log['error'] = True
                log['error_tb'] = traceback.format_exc()

            self.queue.put(dict(log=log, freader=freader, line_info=line_info))

        while not freader.is_fully_acknowledged():
            self.log.debug('Waiting for %f secs for the pygtail to get fully acknowledged' % (self.PYGTAIL_ACK_WAIT_TIME))
            time.sleep(self.PYGTAIL_ACK_WAIT_TIME)

    def collect_log_lines(self, log_file):
        while 1:
            try:
                self._collect_log_lines(log_file)
            except (SystemExit, KeyboardInterrupt): raise
            except:
                self.log.exception('Error during reading from log file')

            time.sleep(self.TIME_WAIT)

    def validate_log_format(self, log):
        assert isinstance(log, dict)
        assert isinstance(log['id'], str)
        assert isinstance(log['data'], dict)
        assert isinstance(log['timestamp'], basestring)
        assert isinstance(log['file'], str)
        assert isinstance(log['host'], str)
        assert isinstance(log['handler'], str)
        assert isinstance(log['raw'], str)

    def check_depth_at_nsq(self):
        url = STATS_URL % (self.nsqd_http_address, self.nsqtopic)
        while 1:
            try:
                data = self.session.get(url)
                data = json.loads(data.content)
                topics = data.get('topics', []) or data.get('data', {}).get('topics', [])
                for record in topics:
                    topic_name = record['topic_name']
                    if self.nsqtopic == topic_name:
                        channels = record.get('channels', [])
                        if channels:
                            for channel in channels:
                                channel_name = channel.get('channel_name')
                                if self.nsqchannel == channel_name:
                                    depth_val = channel.get('depth')
                        if not channels:
                            depth_val = record.get('depth')
                        self.log.info('Present depth count at nsq %d' % (depth_val))
                        if depth_val > self.depth_limit_at_nsq:
                            self.has_nsq_limit_exceeded = True
                        else:
                            self.has_nsq_limit_exceeded = False
            except:
                pass
            finally:
                time.sleep(self.WAIT_TIME_TO_CHECK_DEPTH_AT_NSQ)

    def send_to_nsq(self):
        self.log.info('Entered the send_to_nsq function')
        msgs = []
        last_push_ts = time.time()
        url = MPUB_URL % (self.nsqd_http_address, self.nsqtopic)
        while 1:
            read_from_q = False

            try:
                msg = self.queue.get(block=True, timeout=self.QUEUE_TIMEOUT)
                read_from_q = True
                msgs.append(msg)

            except Queue.Empty:
                time.sleep(self.SLEEP_TIME)
                if not msgs:
                    continue

            cur_ts = time.time()
            time_since_last_push = cur_ts - last_push_ts

            is_msg_limit_reached = len(msgs) >= self.MAX_MSGS_TO_PUSH
            is_max_time_elapsed = time_since_last_push >= self.MAX_SECONDS_TO_PUSH

            should_push = len(msgs) > 0 and (is_max_time_elapsed or is_msg_limit_reached)

            try:
                if should_push:
                    counter = 0
                    self.SLEEP_TIME = 1
                    while 1:
                        if counter < 5 and counter > 0:
                            self.SLEEP_TIME *= 2
                        elif counter >= 5:
                            self.log.debug('Logs which are failed to publish to nsq are writing to a file: %s' % (self.exception_logs_file.name))
                            self.exception_logs_file.write('\n'.join(json.dumps(x['log']) for x in msgs))
                            msgs = []
                            break

                        if not self.has_nsq_limit_exceeded:
                            try:
                                self.session.post(url, data='\n'.join(json.dumps(x['log']) for x in msgs).encode(encoding='UTF-8',errors ='strict'),timeout=5.000) # TODO What if session expires?
                                self.log.info('Sent logs to nsq, num sent = %d' % (len(msgs)))
                                self.confirm_success(msgs)
                                self.log.info('Updated the offset file of pygtail for %d lines' % (len(msgs)))

                                msgs = []
                                last_push_ts = time.time()
                            except (SystemExit, KeyboardInterrupt): raise
                            except:
                                counter += 1
                                self.log.exception('During sending to nsq. Will retry ...')
                                self.log.debug('Waiting for %d seconds' % (self.SLEEP_TIME))
                                time.sleep(self.SLEEP_TIME)
                                continue
                            break
                        else:
                            self.log.debug('nsq depth limit exceeded, waiting for %d secs' % (self.WAIT_TIME_TO_CHECK_DEPTH_AT_NSQ))
                            time.sleep(self.WAIT_TIME_TO_CHECK_DEPTH_AT_NSQ)

            except (SystemExit, KeyboardInterrupt): raise
            finally:
                if read_from_q: self.queue.task_done()

    def confirm_success(self, msgs):
        for msg in msgs:
            freader = msg['freader']
            freader.update_offset_file(msg['line_info'])

    def _prepare_log_files_list(self):
        log_files = []

        for f in self.file:
            fpattern, handler = f.split(':', 1)

            try:
                handler_fn = self._load_handler_fn(handler)
            except (SystemExit, KeyboardInterrupt): raise
            except (ImportError, AttributeError):
                sys.exit(-1)

            fpaths = glob.glob(fpattern)
            # TODO: We need to poll for fpattern if file was not available
            if not fpaths:
                raise IOError('file doesnot exist %s' % (fpattern))

            for fpath in fpaths:
                log_f = dict(fpath=fpath, fpattern=fpattern, handler=handler, handler_fn=handler_fn)
                log_files.append(log_f)

        return log_files

    def start(self):
        self.queue = Queue.Queue(maxsize=self.QUEUE_MAX_SIZE)
        self.log.info('Created queue object with max size %d' % (self.QUEUE_MAX_SIZE))
        self.session = requests.Session()
        self.log.info('Created requests session object')
        self.has_nsq_limit_exceeded = False

        log_files = self._prepare_log_files_list()

        for log_f in log_files:
            th = Thread(target=self.collect_log_lines, args=(log_f,))
            th.daemon = True
            th.start()

        th = Thread(target=self.send_to_nsq)
        th.daemon = True
        th.start()

        th = Thread(target=self.check_depth_at_nsq)
        th.daemon = True
        th.start()
        self.log.info('Started checking depth at nsq')

        th.join()
