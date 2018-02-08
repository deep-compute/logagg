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

from deeputil import Dummy, AttrDict, keeprunning
from pygtail import Pygtail

# TODO
'''
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
    >>> lc = LogCollector('~/Desktop/log_samples/access_new.log:logagg.handlers.nginx_access', \
                                'test_topic', 'localhost:4151', 10000, 30)
    >>> lc.log
    <deeputil.misc.Dummy object at 0x7fc902bb7c10>
    >>> lc.nsqd_http_address
    'localhost:4151'
    >>> lc.nsq_max_depth
    10000
    '''
    DESC = 'Collects the log information and sends to NSQTopic'
    
    QUEUE_MAX_SIZE = 2000           # Maximum number of messages in in-mem queue
    NBYTES_TO_SEND = 5000000        # Number of bytes from in-mem queue minimally required to push
    MAX_SECONDS_TO_PUSH = 1         # Wait till this much time elapses before pushing
    LOG_FILE_POLL_INTERVAL = 0.25   # Wait time to pull log file for new lines added
    QUEUE_READ_TIMEOUT = 1          # Wait time when doing blocking read on the in-mem q
    PYGTAIL_ACK_WAIT_TIME = 0.05    # TODO: Document this
    SCAN_FPATTERNS_INTERVAL = 30    # How often to scan filesystem for files matching fpatterns
    HOST = socket.gethostname()
    HEARTBEAT_RESTART_INTERVAL = 30 # Wait time if heartbeat sending stops

    def __init__(self, fpaths, nsq_sender,
                heartbeat_interval, log=DUMMY_LOGGER):
        self.fpaths = fpaths
        self.nsq_sender = nsq_sender
        self.heartbeat_interval = heartbeat_interval
        self.log = log

        # Log fpath to thread mapping
        self.log_reader_threads = {}
        # Handle name to handler fn obj map
        self.handlers = {}
        self.queue = Queue.Queue(maxsize=self.QUEUE_MAX_SIZE)

    def _load_handler_fn(self, imp):
        self.log.debug('Loading handler', name=imp)
        
        module_name, fn_name = imp.split('.', 1)
        module = __import__(module_name)
        fn = operator.attrgetter(fn_name)(module)
        
        self.log.info('Loaded handler ', module=module_name, fn=fn_name,)
        return fn

    def _log_exception(self, __fn__):
        self.log.exception('Error during run Continuing ...', \
                            fn=__fn__.func_name, tb=repr(traceback.format_exc()))

    @keeprunning(LOG_FILE_POLL_INTERVAL, on_error=_log_exception)
    def collect_log_lines(self, log_file):
        L = log_file
        fpath = L['fpath']
        self.log.debug('Tracking log file for log lines', fpath=fpath)
        
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
                
            self.queue.put(dict(log=json.dumps(log), 
                                freader=freader, line_info=line_info))
            self.log.debug("TALLY: PUT into self.queue")
            
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


    def _get_msgs_from_queue(self, msgs, msgs_nbytes, timeout):
        read_from_q = False
        ts = time.time()
        
        while 1:
            try:
                msg = self.queue.get(block=True, timeout=self.QUEUE_READ_TIMEOUT)
                read_from_q = True
                self.log.debug("TALLY: GET from self.queue")
                
                msgs.append(msg)
                msgs_nbytes += len(msg['log'])
                
                if msgs_nbytes > self.NBYTES_TO_SEND: # FIXME: class level const
                    self.log.debug('Msg bytes read from in-queue got exceeded')
                    break
                #FIXME condition never met
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
        msgs = []
        msgs_nbytes = 0
        should_push = False
        
        while not should_push:
            self.log.debug('should_push', is_true=should_push)
            cur_ts = time.time()
            time_since_last_push = cur_ts - state.last_push_ts
            
            msgs, msgs_nbytes, read_from_q = self._get_msgs_from_queue(msgs, msgs_nbytes,
                        self.MAX_SECONDS_TO_PUSH)
                        
            have_enough_msgs = msgs_nbytes >= self.NBYTES_TO_SEND
            is_max_time_elapsed = time_since_last_push >= self.MAX_SECONDS_TO_PUSH
            
            should_push = len(msgs) > 0 and (is_max_time_elapsed or have_enough_msgs)
            self.log.debug('desciding wheather to push', should_push=should_push)
            
        try:
            self.log.debug('trying to push to nsq', msgs_length=len(msgs))
            self.nsq_sender.handle_logs(msgs)
            self.confirm_success(msgs)
            self.log.info('pushed to nsq', msgs_length=len(msgs))
            msgs = []
            state.last_push_ts = time.time()
        except (SystemExit, KeyboardInterrupt): raise
        finally:
            if read_from_q: self.queue.task_done()

    def confirm_success(self, msgs):
        for msg in msgs:
            freader = msg['freader']
            freader.update_offset_file(msg['line_info'])

    @keeprunning(SCAN_FPATTERNS_INTERVAL, on_error=_log_exception)
    def _scan_fpatterns(self):
        '''
        fpaths = '~/Desktop/log_samples/access_new.log:logagg.handlers.nginx_access'
        fpattern = '~/Desktop/log_samples/access_new.log'
        '''
        for f in self.fpaths:
            fpattern, handler = f.split(':', 1)
            # TODO code for scanning fpatterns for the files not yet present goes here
            fpaths = glob.glob(fpattern)
            # Load handler_fn if not in list
            if handler not in self.handlers:
                try:
                        handler_fn = self.handlers.get(handler,
                                        self._load_handler_fn(handler))
                        self.handlers[handler] = handler_fn
                except (SystemExit, KeyboardInterrupt): raise
                except (ImportError, AttributeError):
                    sys.exit(-1)
                # Start a thread for every file
                for fpath in fpaths:
                    log_f = dict(fpath=fpath, fpattern=fpattern,
                                    handler=handler, handler_fn=handler_fn)
                    log_key = (fpath, fpattern, handler)
                    if log_key not in self.log_reader_threads:
                        #FIXME There is no existing thread tracking this log file. Start one
                        self.log_reader_threads[log_key] = _start_daemon_thread(self.collect_log_lines, (log_f,))
                        self.log.info('Started collect_log_lines thread ', log_key=log_key)
        time.sleep(self.SCAN_FPATTERNS_INTERVAL)

    @keeprunning(HEARTBEAT_RESTART_INTERVAL, on_error=_log_exception)
    def send_heartbeat(self, state):
        # Sends continuous heartbeats to a seperate topic in nsq
        heartbeat_payload = {'host': self.HOST,
                            'heartbeat_number': state.heartbeat_number,
                            'timestamp': time.time()
                            }
        self.nsq_sender.handle_heartbeat(heartbeat_payload)
        state.heartbeat_number += 1
        time.sleep(self.heartbeat_interval)

    def start(self):
        _start_daemon_thread(self._scan_fpatterns)
        
        state = AttrDict(last_push_ts=time.time())
        _start_daemon_thread(self.send_to_nsq, (state,))
        
        state = AttrDict(heartbeat_number=0)
        _start_daemon_thread(self.send_heartbeat, (state,)).join()

