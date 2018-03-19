import time

import requests
from deeputil import keeprunning
from logagg import util

class NSQSender(object):

    NSQ_READY_CHECK_INTERVAL = 1            # Wait time to check nsq readiness (alive and not full)
    HEARTBEAT_TOPIC = 'Heartbeat#ephemeral' # Topic name at which heartbeat is to be sent
    MPUB_URL = 'http://%s/mpub?topic=%s'    # Url to post msgs to NSQ

    def __init__(self, http_loc, nsq_topic, nsq_max_depth, log=util.DUMMY_LOGGER):
        self.nsqd_http_address = http_loc
        self.topic_name = nsq_topic
        self.nsq_max_depth = nsq_max_depth
        self.log = log
        
        self.session = requests.Session()
        self._ensure_topic(self.topic_name)
        self._ensure_topic(self.HEARTBEAT_TOPIC)

    @keeprunning(NSQ_READY_CHECK_INTERVAL,
                 exit_on_success=True,
                 on_error=util.log_exception)
    def _ensure_topic(self, topic_name):
        u = 'http://%s/topic/create?topic=%s' % (self.nsqd_http_address, topic_name)
        try:
            self.session.post(u)
        except requests.exceptions.RequestException as e:
            self.log.debug('Could not create/find topic, retrying....', topic=topic_name)
            raise
        self.log.info('Created topic ', topic=topic_name)

    def _is_ready(self, topic_name):
        '''
        Is NSQ running and have space to receive messages?
        '''
        url = 'http://%s/stats?format=json&topic=%s' % (self.nsqd_http_address, topic_name)
        #Cheacking for ephmeral channels
        if '#' in topic_name:
            topic_name, tag =topic_name.split("#", 1)
        
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
                    raise Exception('Topic missing at nsq..')
                
                topic = topics[0]
                depth = topic['depth']
                depth += sum(c.get('depth', 0) for c in topic['channels'])
                self.log.debug('nsq depth check', topic=topic_name, depth=depth, max_depth=self.nsq_max_depth)
                
                if depth < self.nsq_max_depth:
                    break
                
                self.log.info('nsq is full. waiting for it to clear ...')
                
            except (SystemExit, KeyboardInterrupt): raise
            except requests.exceptions.RequestException as e:
                self.log.exception('Exception wait_till_nsq_ready=',
                                    tb=repr(traceback.format_exc()))
            finally:
                s = self.NSQ_READY_CHECK_INTERVAL
                time.sleep(s)

    @keeprunning(NSQ_READY_CHECK_INTERVAL,
                 exit_on_success=True,
                 on_error=util.log_exception)
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
            raise
        self.log.debug('nsq push done ', nmsgs=len(msgs), nbytes=len(data))

    def handle_logs(self, msgs):
        self._is_ready(topic_name=self.topic_name)
        self._send_messages(msgs, topic_name=self.topic_name)

    def handle_heartbeat(self, msgs):
        self._is_ready(topic_name=self.HEARTBEAT_TOPIC)
        self._send_messages(msgs, topic_name=self.HEARTBEAT_TOPIC)

