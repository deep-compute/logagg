import os
import sys
import time
import ujson as json
import glob
import uuid
import Queue
import socket
import datetime
from operator import attrgetter
import traceback

from deeputil import AttrDict, keeprunning
from pygtail import Pygtail
from logagg import util
from logagg.formatters import RawLog

# TODO
'''
After a downtime of collector, pygtail is missing logs from rotational files
'''

class LogCollector(object):
    DESC = 'Collects the log information and sends to NSQTopic'

    QUEUE_MAX_SIZE = 2000           # Maximum number of messages in in-mem queue
    MAX_NBYTES_TO_SEND = 4.5 * (1024**2)  # Number of bytes from in-mem queue minimally required to push
    MIN_NBYTES_TO_SEND = 512 * 1024 # Minimum number of bytes to send to nsq in mpub
    MAX_SECONDS_TO_PUSH = 1         # Wait till this much time elapses before pushing
    LOG_FILE_POLL_INTERVAL = 0.25   # Wait time to pull log file for new lines added
    QUEUE_READ_TIMEOUT = 1          # Wait time when doing blocking read on the in-mem q
    PYGTAIL_ACK_WAIT_TIME = 0.05     # TODO: Document this
    SCAN_FPATTERNS_INTERVAL = 30    # How often to scan filesystem for files matching fpatterns
    HOST = socket.gethostname()
    HEARTBEAT_RESTART_INTERVAL = 30 # Wait time if heartbeat sending stops
    #TODO check for structure in validate_log_format

    LOG_STRUCTURE = {
        'id': basestring,
        'timestamp': basestring,
        'file' : basestring,
        'host': basestring,
        'formatter' : basestring,
        'raw' : basestring,
        'type' : basestring,
        'level' : basestring,
        'event' : basestring,
        'data' : dict,
        'error' : bool,
        'error_tb' : basestring,
    }

    def __init__(self, fpaths, nsq_sender,
                heartbeat_interval, log=util.DUMMY_LOGGER):
        self.fpaths = fpaths
        self.nsq_sender = nsq_sender
        self.heartbeat_interval = heartbeat_interval
        self.log = log

        # Log fpath to thread mapping
        self.log_reader_threads = {}
        # Handle name to formatter fn obj map
        self.formatters = {}
        self.queue = Queue.Queue(maxsize=self.QUEUE_MAX_SIZE)

    def _remove_redundancy(self, log):
        for key in log:
            if key in log and key in log['data']:
                log[key] = log['data'].pop(key)
        return log

    def validate_log_format(self, log):
        for key in log:
            assert (key in self.LOG_STRUCTURE)
            try:
                assert isinstance(log[key], self.LOG_STRUCTURE[key])
            except AssertionError as e:
                self.log.exception('formatted_log_structure_rejected' , log=log)

    @keeprunning(LOG_FILE_POLL_INTERVAL, on_error=util.log_exception)
    def collect_log_lines(self, log_file):
        L = log_file
        fpath = L['fpath']
        self.log.debug('tracking_file_for_log_lines', fpath=fpath)

        freader = Pygtail(fpath)
        for line_info in freader:
            line = line_info['line'][:-1] # remove new line char at the end

            # assign default values
            log = dict(
                    id=None,
                    file=fpath,
                    host=self.HOST,
                    formatter=L['formatter'],
                    event='event',
                    data={},
                    raw=line,
                    timestamp=datetime.datetime.utcnow().isoformat(),
                    type='log',
                    level='debug',
                    error= False,
                    error_tb='',
                  )

            try:
                _log = L['formatter_fn'](line)

                if isinstance(_log, RawLog):
                    formatter, raw_log = _log['formatter'], _log['raw']
                    log.update(_log)
                    _log = util.load_object(formatter)(raw_log)

                log.update(_log)
            except (SystemExit, KeyboardInterrupt) as e: raise
            except:
                log['error'] = True
                log['error_tb'] = traceback.format_exc()
                self.log.exception('error_during_handling_log_line', log=log['raw'])

            if log['id'] == None:
                log['id'] = uuid.uuid1().hex

            log = self._remove_redundancy(log)
            self.validate_log_format(log)

            self.queue.put(dict(log=json.dumps(log),
                                freader=freader, line_info=line_info))
            self.log.debug('tally:put_into_self.queue', size=self.queue.qsize())

        while not freader.is_fully_acknowledged():
            t = self.PYGTAIL_ACK_WAIT_TIME
            self.log.debug('waiting_for_pygtail_to_fully_ack', wait_time=t)
            time.sleep(t)
        time.sleep(self.LOG_FILE_POLL_INTERVAL)

    def _get_msgs_from_queue(self, msgs, timeout):
        msgs_pending = []
        read_from_q = False
        ts = time.time()

        msgs_nbytes = sum(len(m['log']) for m in msgs)

        while 1:
            try:
                msg = self.queue.get(block=True, timeout=self.QUEUE_READ_TIMEOUT)
                read_from_q = True
                self.log.debug("tally:get_from_self.queue")

                _msgs_nbytes = msgs_nbytes + len(msg['log'])
                _msgs_nbytes += 1 # for newline char

                if _msgs_nbytes > self.MAX_NBYTES_TO_SEND:
                    msgs_pending.append(msg)
                    self.log.debug('msg_bytes_read_mem_queue_exceeded')
                    break

                msgs.append(msg)
                msgs_nbytes = _msgs_nbytes

                #FIXME condition never met
                if time.time() - ts >= timeout and msgs:
                    self.log.debug('msg_reading_timeout_from_mem_queue_got_exceeded')
                    break
                    # TODO: What if a single log message itself is bigger than max bytes limit?

            except Queue.Empty:
                self.log.debug('queue_empty')
                time.sleep(self.QUEUE_READ_TIMEOUT)
                if not msgs:
                    continue
                else:
                    return msgs_pending, msgs_nbytes, read_from_q

        self.log.debug('got_msgs_from_mem_queue')
        return msgs_pending, msgs_nbytes, read_from_q


    @keeprunning(0, on_error=util.log_exception) # FIXME: what wait time var here?
    def send_to_nsq(self, state):
        self.log.debug('send_to_nsq')
        msgs = []
        should_push = False

        while not should_push:
            cur_ts = time.time()
            self.log.debug('should_push', should_push=should_push)
            time_since_last_push = cur_ts - state.last_push_ts

            msgs_pending, msgs_nbytes, read_from_q = self._get_msgs_from_queue(msgs,
                                                                        self.MAX_SECONDS_TO_PUSH)

            have_enough_msgs = msgs_nbytes >= self.MIN_NBYTES_TO_SEND
            is_max_time_elapsed = time_since_last_push >= self.MAX_SECONDS_TO_PUSH

            should_push = len(msgs) > 0 and (is_max_time_elapsed or have_enough_msgs)
            self.log.debug('deciding_to_push', should_push=should_push,
                            time_since_last_push=time_since_last_push,
                            msgs_nbytes=msgs_nbytes)

        try:
            self.log.debug('trying_to_push_to_nsq', msgs_length=len(msgs))
            self.nsq_sender.handle_logs(msgs)
            self.confirm_success(msgs)
            self.log.debug('pushed_to_nsq', msgs_length=len(msgs))
            msgs = msgs_pending
            state.last_push_ts = time.time()
        except (SystemExit, KeyboardInterrupt): raise
        finally:
            if read_from_q: self.queue.task_done()

    def confirm_success(self, msgs):
        ack_fnames = set()

        for msg in reversed(msgs):
            freader = msg['freader']
            fname = freader.filename

            if fname in ack_fnames:
                continue

            ack_fnames.add(fname)
            freader.update_offset_file(msg['line_info'])

    @keeprunning(SCAN_FPATTERNS_INTERVAL, on_error=util.log_exception)
    def _scan_fpatterns(self, state):
        '''
        fpaths = 'file=/var/log/nginx/access.log:formatter=logagg.formatters.nginx_access'
        fpattern = '/var/log/nginx/access.log'
        '''
        for f in self.fpaths:
            fpattern, formatter =(a.split('=')[1] for a in f.split(':', 1))
            self.log.debug('scan_fpatterns', fpattern=fpattern, formatter=formatter)
            # TODO code for scanning fpatterns for the files not yet present goes here
            fpaths = glob.glob(fpattern)
            # Load formatter_fn if not in list
            for fpath in fpaths:
                if fpath not in state.files_tracked:
                    try:
                        formatter_fn = self.formatters.get(formatter,
                                      util.load_object(formatter))
                        self.log.info('found_formatter_fn', fn=formatter)
                        self.formatters[formatter] = formatter_fn
                    except (SystemExit, KeyboardInterrupt): raise
                    except (ImportError, AttributeError):
                        self.log.exception('formatter_fn_not_found', fn=formatter)
                        sys.exit(-1)
                    # Start a thread for every file
                    self.log.info('found_log_file', log_file=fpath)
                    log_f = dict(fpath=fpath, fpattern=fpattern,
                                    formatter=formatter, formatter_fn=formatter_fn)
                    log_key = (fpath, fpattern, formatter)
                    if log_key not in self.log_reader_threads:
                        self.log.info('starting_collect_log_lines_thread', log_key=log_key)
                        # There is no existing thread tracking this log file. Start one
                        log_reader_thread = util.start_daemon_thread(self.collect_log_lines, (log_f,))
                        self.log_reader_threads[log_key] = log_reader_thread
                    state.files_tracked.append(fpath)
        time.sleep(self.SCAN_FPATTERNS_INTERVAL)

    @keeprunning(HEARTBEAT_RESTART_INTERVAL, on_error=util.log_exception)
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
        state = AttrDict(files_tracked=list())
        util.start_daemon_thread(self._scan_fpatterns, (state,))

        state = AttrDict(last_push_ts=time.time())
        util.start_daemon_thread(self.send_to_nsq, (state,))

        state = AttrDict(heartbeat_number=0)
        th_heartbeat = util.start_daemon_thread(self.send_heartbeat, (state,))

        while True:
            th_heartbeat.join(1)
            if not th_heartbeat.isAlive(): break
