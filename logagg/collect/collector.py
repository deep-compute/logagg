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
    MAX_SECONDS_TO_PUSH = 2

    TIME_WAIT = 0.25
    SLEEP_TIME = 1
    QUEUE_TIMEOUT = 1

    def __init__(self, log, args, _file, nsqtopic, nsqd_http_address):
        self.log = log
        self.args = args
        self.file = _file
        self.nsqtopic = nsqtopic
        self.nsqd_http_address = nsqd_http_address

    def _load_handler_fn(self, imp):
        module_name, fn_name = imp.split('.', 1)
        module = __import__(module_name)
        fn = operator.attrgetter(fn_name)(module)
        return fn

    def _collect_log_lines(self, log_file):
        L = log_file

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
            time.sleep(0.05)

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
        data = self.session.get(url)
        data = json.loads(data.content)
        if 'health' in data:
            topics = data.get('topics')
        else:
            topics = data.get('data').get('topics')
        for record in topics:
            topic_name = record['topic_name']
            if self.nsqtopic in topic_name:
                get_depth_count = record.get('depth')
                return get_depth_count

    def send_to_nsq(self):
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
                    while 1:
                        has_nsq_depth_limit_reached = self.check_depth_at_nsq() > 100
                        if not has_nsq_depth_limit_reached:
                            try:
                                self.session.post(url, data='\n'.join(json.dumps(x['log']) for x in msgs)) # TODO What if session expires?
                                self.log.info('sent logs to nsq, num sent = %d' % (len(msgs)))
                                self.confirm_success(msgs)

                                msgs = []
                                last_push_ts = time.time()
                            except (SystemExit, KeyboardInterrupt): raise
                            except:
                                self.log.exception('During sending to nsq. Will retry ...')
                                time.sleep(self.SLEEP_TIME)
                                continue
                            break
                        else:
                            time.sleep(self.SLEEP_TIME)

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
        self.session = requests.Session()

        log_files = self._prepare_log_files_list()

        for log_f in log_files:
            th = Thread(target=self.collect_log_lines, args=(log_f,))
            th.daemon = True
            th.start()

        th = Thread(target=self.send_to_nsq)
        th.daemon = True
        th.start()

        th.join()
