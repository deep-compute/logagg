import requests
import time
import traceback

from deeputil import keeprunning

class NSQSender(object):

    NSQ_READY_CHECK_INTERVAL = 1            # Wait time to check nsq readiness (alive and not full)
    HEARTBEAT_TOPIC = 'Heartbeat#ephemeral' # Topic name at which heartbeat is to be sent
    MPUB_URL = 'http://%s/mpub?topic=%s'    # Url to post msgs to NSQ

    def __init__(self, http_loc, nsq_topic, nsq_max_depth, log):
        self.nsqd_http_address = http_loc
        self.topic_name = nsq_topic
        self.nsq_max_depth = nsq_max_depth
        self.log = log
        
        self.session = requests.Session()
        self._ensure_topic()

    def _ensure_topic(self):
        u = 'http://%s/topic/create?topic=%s' % (self.nsqd_http_address, self.topic_name)
        try:
            self.session.post(u)
        except requests.exceptions.RequestException as e:
            self.log.exception('Could not create topic=', e, topic=self.topic_name)
            sys.exit(1)
        self.log.info('Created topic ', topic=self.topic_name)

    def _is_ready(self, topic_name):
        '''
        Is NSQ running and have space to receive messages?
        '''
        url = 'http://%s/stats?format=json&topic=%s' % (self.nsqd_http_address, topic_name)
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
                topics = data.get('topics', [])
                topics = [t for t in topics if t['topic_name'] == topic_name]
                
                if not topics:
                    raise Exception('Topic missing', topic=topic_name)
                
                topic = topics[0]
                depth = topic['depth']
                depth += sum(c.get('depth', 0) for c in topic['channels'])
                self.log.info('nsq depth check', depth=depth, max_depth=self.nsq_max_depth)
                
                if depth < self.nsq_max_depth:
                    break
                
                self.log.info('nsq is full. waiting for it to clear ...')
                
            except (SystemExit, KeyboardInterrupt): raise
            except requests.exceptions.RequestException as e:
                self.log.exception('Exception wait_till_nsq_ready=', e)
            finally:
                s = self.NSQ_READY_CHECK_INTERVAL
                time.sleep(s)

    def _log_exception(self, __fn__):
        self.log.exception('During run of %s. exp=%r, Continuing ...' % (__fn__.func_name, traceback.format_exc()))

    @keeprunning(NSQ_READY_CHECK_INTERVAL, exit_on_success=True, on_error=_log_exception)
    def _send_messages(self, msgs, topic_name):
        if not isinstance(msgs, list):
            data = msgs
        else:
            data = '\n'.join(m['log'] for m in msgs) #FIXME only works if 'log' is there
        url = self.MPUB_URL % (self.nsqd_http_address, topic_name)
        try:
            self.session.post(url, data=data, timeout=5) # TODO What if session expires?
        except (SystemExit, KeyboardInterrupt): raise
        except requests.exceptions.RequestException as e:
            self.log.exception('Exception in _send_messages=', e)
        self.log.debug('nsq push done nmsgs=%d nbytes=%d', len(msgs), len(data))

    def handle_log(self, msgs):
        self._is_ready(topic_name=self.topic_name)
        self._send_messages(msgs, topic_name=self.topic_name)

    def handle_heartbeat(self, msgs):
        self._send_messages(msgs, topic_name=self.HEARTBEAT_TOPIC)

