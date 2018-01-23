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
from deeputil.misc import Dummy, AttrDict
from deeputil.keeprunning import keeprunning

from pygtail import Pygtail

# TODO
'''
What if requests session expires?
After a downtime of collector, pygtail is missing logs from rotational files
'''

DUMMY_LOGGER = Dummy()

def _start_daemon_thread(target, args=()):
    th = Thread(target=target, args=args)
    th.daemon = True
    th.start()
    return th

class LogCollector(object):
    '''
    Instantiate LogCollector class with an object
    
    >>> from logagg import LogCollector
    >>> lc_obj = LogCollector('~/Desktop/log_samples/access_new.log:logagg.handlers.nginx_access', \
                                'test_topic', 'localhost:4151', 10000, 30)
    >>> lc_obj.log
    <deeputil.misc.Dummy object at 0x7fc902bb7c10>
    >>> lc_obj.nsqd_http_address
    'localhost:4151'
    >>> lc_obj.nsq_max_depth
    10000
    '''
    DESC = 'Collects the log information and sends to NSQTopic'

    QUEUE_MAX_SIZE = 2000           # Maximum number of messages in in-mem queue
    MIN_MSGS_TO_PUSH = 100          # Wait till we accumulate this many msgs before pushing
    NBYTES_TO_SEND = 5000000        # Number of bytes from in-mem queue minimally required to push
    MAX_SECONDS_TO_PUSH = 1         # Wait till this much time elapses before pushing
    LOG_FILE_POLL_INTERVAL = 0.25   # Wait time to pull log file for new lines added
    NSQ_READY_CHECK_INTERVAL = 1    # Wait time to check nsq readiness (alive and not full)
    QUEUE_READ_TIMEOUT = 1          # Wait time when doing blocking read on the in-mem q
    PYGTAIL_ACK_WAIT_TIME = 0.05    # TODO: Document this
    SCAN_FPATTERNS_INTERVAL = 30    # How often to scan filesystem for files matching fpatterns

    HOST = socket.gethostname()
    MPUB_URL = 'http://%s/mpub?topic=%s'

    def __init__(self, fpaths,
                       nsqtopic, nsqd_http_address, nsq_max_depth,
                       heartbeat_interval, log=DUMMY_LOGGER):
        self.fpaths = fpaths

        self.nsqtopic = nsqtopic
        self.nsqd_http_address = nsqd_http_address
        self.nsq_max_depth = nsq_max_depth
        self.heartbeat_interval = heartbeat_interval
        self.log = log

        # Log fpath to thread mapping
        self.log_reader_threads = {}

        # Handle name to handler fn obj map
        self.handlers = {}

        self.queue = Queue.Queue(maxsize=self.QUEUE_MAX_SIZE)
        self.session = requests.Session()

    def _load_handler_fn(self, imp):
        self.log.debug('Loading handler', name=imp)

        module_name, fn_name = imp.split('.', 1)
        module = __import__(module_name)
        fn = operator.attrgetter(fn_name)(module)

        self.log.debug('Loaded handler %s' %str((module_name, fn_name, imp)))
        return fn

    def _log_exception(self, fn, exc):
        self.log.error('During run of %s. e=%r, Continuing ...' % (fn.func_name, traceback.format_exc()))

    @keeprunning(LOG_FILE_POLL_INTERVAL, on_error=_log_exception)
    def collect_log_lines(self, log_file):
        L = log_file
        fpath = L['fpath']
        self.log.info('Tracking log file for log lines', fpath=fpath)

        freader = Pygtail(fpath)
        for line_info in freader:
            line = line_info['line'][:-1] # remove new line char at the end
            log = dict(
                    id=uuid.uuid1().hex,
                    file=fpath,
                    host=self.HOST,
                    handler=L['handler'],
                    raw=line,
                    timestamp=datetime.datetime.utcnow().isoformat(),
                    type='log',
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

            self.queue.put(dict(log=json.dumps(log), freader=freader, line_info=line_info))
            #self.log.debug("TALLY: PUT into self.queue")

        while not freader.is_fully_acknowledged():
            t = self.PYGTAIL_ACK_WAIT_TIME
            self.log.debug('Waiting for pygtail to fully ack', wait_time=t)
            time.sleep(t)

    def validate_log_format(self, log):
        assert isinstance(log, dict)
        assert isinstance(log['id'], str)
        assert isinstance(log['data'], dict)
        assert isinstance(log['timestamp'], basestring)
        assert isinstance(log['file'], str)
        assert isinstance(log['host'], str)
        assert isinstance(log['handler'], str)
        assert isinstance(log['raw'], str)

    def wait_till_nsq_ready(self):
        '''
        Is NSQ running and have space to receive messages?
        '''
        url = 'http://%s/stats?format=json&topic=%s' % (self.nsqd_http_address, self.nsqtopic)
        while 1:
            try:
                data = self.session.get(url).json()
                '''
                data = {u'start_time': 1516164866, u'version': u'1.0.0-compat', \
                        u'health': u'OK', u'topics': [{u'message_count': 19019, \
                        u'paused': False, u'topic_name': u'test_topic', u'channels': [], \
                        u'depth': 19019, u'backend_depth': 9019, u'e2e_processing_latency': {u'count': 0, \
                        u'percentiles': None}}]}
                '''
                # FIXME: why "or"? Aren't we sure about what the server returns?
                topics = data.get('topics', [])
                topics = [t for t in topics if t['topic_name'] == self.nsqtopic]

                if not topics:
                    raise Exception('Topic missing', topic=self.nsqtopic)

                topic = topics[0]
                depth = topic['depth']
                depth += sum(c.get('depth', 0) for c in topic['channels'])
                self.log.debug('nsq depth depth=%d max_depth=%d', depth, self.nsq_max_depth)

                if depth < self.nsq_max_depth:
                    break

                self.log.debug('nsq is full. waiting for it to clear ...')

            except (SystemExit, KeyboardInterrupt): raise
            except: 
                # FIXME: use a more specific exception class here (some request exception)
                self.log.exception('Exception wait_till_nsq_ready')
            finally:
                s = self.NSQ_READY_CHECK_INTERVAL
                time.sleep(s)

    @keeprunning(wait_secs=NSQ_READY_CHECK_INTERVAL, exit_on_success=True, on_error=_log_exception)
    def _send_msgs_to_nsq(self, msgs, on_error=_log_exception):
        data = '\n'.join(m['log'] for m in msgs)

        url = self.MPUB_URL % (self.nsqd_http_address, self.nsqtopic)
        self.session.post(url, data=data, timeout=5) # TODO What if session expires?

        self.log.debug('nsq push done nmsgs=%d nbytes=%d', len(msgs), len(data))

    def _get_msgs_from_queue(self, msgs, msgs_nbytes, timeout):
        read_from_q = False

        ts = time.time()

        while 1:
            try:
                msg = self.queue.get(block=True, timeout=self.QUEUE_READ_TIMEOUT)
                read_from_q = True
                #self.log.debug("TALLY: GET")

                msgs.append(msg)
                msgs_nbytes += len(msg['log'])

                if msgs_nbytes > self.NBYTES_TO_SEND: # FIXME: class level const
                    self.log.debug('Msg bytes read from in-queue got exceeded')
                    break

                if time.time() - ts >= timeout and msgs:
                    self.log.debug('Msg reading timeout from in-queue got exceeded')
                    break

                # TODO: What if a single log message itself is bigger than max bytes limit?

            except Queue.Empty:
                self.log.debug('QUEUE empty')
                time.sleep(self.QUEUE_READ_TIMEOUT)
                if not msgs:
                    continue
                else:
                    return msgs, msgs_nbytes, read_from_q
        self.log.debug('Got msgs from in-queue')
        return msgs, msgs_nbytes, read_from_q

    @keeprunning(0, on_error=_log_exception) # FIXME: what wait time var here?
    def send_to_nsq(self, state):
        self.log.debug('send_to_nsq')
        self.wait_till_nsq_ready()

        msgs = []
        msgs_nbytes = 0
        should_push = False

        while not should_push:
            self.log.debug('should_push', is_true=should_push)
            cur_ts = time.time()
            time_since_last_push = cur_ts - state.last_push_ts

            msgs, msgs_nbytes, read_from_q = self._get_msgs_from_queue(msgs, msgs_nbytes,
                        self.MAX_SECONDS_TO_PUSH)

            have_enough_msgs = len(msgs) >= self.MIN_MSGS_TO_PUSH
            is_max_time_elapsed = time_since_last_push >= self.MAX_SECONDS_TO_PUSH
            self.log.debug('desciding wheather to push is_max_time_elapsed=%s have_enough_msgs=%s len(msgs)=%d', is_max_time_elapsed, have_enough_msgs, len(msgs))
            should_push = len(msgs) > 0 and (is_max_time_elapsed or have_enough_msgs)

        try:
            self.log.debug('trying to push to nsq')
            self._send_msgs_to_nsq(msgs)
            #self.log.debug("TMP: sent %d msgs to oblivion" % len(msgs))
            self.confirm_success(msgs)
            msgs = []
            state.last_push_ts = time.time()
        except (SystemExit, KeyboardInterrupt): raise
        finally:
            if read_from_q: self.queue.task_done()

    def confirm_success(self, msgs):
        for msg in msgs:
            freader = msg['freader']
            freader.update_offset_file(msg['line_info'])
            #self.log.debug("TALLY: ACK")

    @keeprunning(2, on_error=_log_exception) #FIXME: no hardcoding of value here. class level const.
    def send_heartbeat(self, state):
        # Sends continuous heartbeats to a seperate topic in nsq
        url = self.MPUB_URL % (self.nsqd_http_address, 'heartbeat#ephemeral')
        heartbeat_payload = {'host': self.HOST,
                'heartbeat_number': state.heartbeat_number,
                'timestamp': time.time()
                }
        self.session.post(url, data=json.dumps(heartbeat_payload), timeout=5) #FIXME: const

        state.heartbeat_number += 1
        time.sleep(self.heartbeat_interval)

    def _ensure_topic(self):
        u = 'http://%s/topic/create?topic=%s' % (self.nsqd_http_address, self.nsqtopic)
        self.session.post(u)
        self.log.debug('Created topic', topic=self.nsqtopic)

    @keeprunning(SCAN_FPATTERNS_INTERVAL, on_error=_log_exception)
    def _scan_fpatterns(self):
        # TODO: document example of fpath pattern here

        for f in self.fpaths:
            fpattern, handler = f.split(':', 1)
            # TODO code for scanning fpatterns for the files not yet present goes here
            fpaths = glob.glob(fpattern)
            try:
                    handler_fn = self.handlers.get(handler, self._load_handler_fn(handler))
                    self.handlers[handler] = handler_fn
            except (SystemExit, KeyboardInterrupt): raise
            except (ImportError, AttributeError):
                sys.exit(-1)

            for fpath in fpaths:
                log_f = dict(fpath=fpath, fpattern=fpattern, handler=handler, handler_fn=handler_fn)
                log_key = (fpath, fpattern, handler)
                if log_key not in self.log_reader_threads:
                    # There is no existing thread tracking this log file. Start one
                    self.log_reader_threads[log_key] = _start_daemon_thread(self.collect_log_lines, (log_f,))

        time.sleep(self.SCAN_FPATTERNS_INTERVAL)

    def start(self):
        self._ensure_topic()

        _start_daemon_thread(self._scan_fpatterns)

        state = AttrDict(last_push_ts=time.time())
        _start_daemon_thread(self.send_to_nsq, (state,))

        state = AttrDict(heartbeat_number=0)
        _start_daemon_thread(self.send_heartbeat, (state,)).join()

