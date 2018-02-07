import re
import json
import datetime

def nginx_access(line):
    '''
    >>> import pprint
    >>> input_line1 = '{ \
                    "remote_addr": "127.0.0.1","remote_user": "-","timestamp": "1515144699.201", \
                    "request": "GET / HTTP/1.1","status": "200","request_time": "0.000", \
                    "body_bytes_sent": "396","http_referer": "-","http_user_agent": "python-requests/2.18.4", \
                    "http_x_forwarded_for": "-","upstream_response_time": "-" \
                        }'
    >>> output_line1 = nginx_access(input_line1)
    >>> pprint.pprint(output_line1)
    {'data': {u'body_bytes_sent': 396.0,
              u'http_referer': u'-',
              u'http_user_agent': u'python-requests/2.18.4',
              u'http_x_forwarded_for': u'-',
              u'remote_addr': u'127.0.0.1',
              u'remote_user': u'-',
              u'request': u'GET / HTTP/1.1',
              u'request_time': 0.0,
              u'status': 200.0,
              u'timestamp': '2018-01-05T09:31:39.201000',
              u'upstream_response_time': 0.0},
     'timestamp': '2018-01-05T09:31:39.201000'}

    >>> input_line2 = '{ \
                    "remote_addr": "192.158.0.51","remote_user": "-","timestamp": "1515143686.415", \
                    "request": "POST /mpub?topic=heartbeat HTTP/1.1","status": "404","request_time": "0.000", \
                    "body_bytes_sent": "152","http_referer": "-","http_user_agent": "python-requests/2.18.4", \
                    "http_x_forwarded_for": "-","upstream_response_time": "-" \
                       }'
    >>> output_line2 = nginx_access(input_line2)
    >>> pprint.pprint(output_line2)
    {'data': {u'body_bytes_sent': 152.0,
              u'http_referer': u'-',
              u'http_user_agent': u'python-requests/2.18.4',
              u'http_x_forwarded_for': u'-',
              u'remote_addr': u'192.158.0.51',
              u'remote_user': u'-',
              u'request': u'POST /mpub?topic=heartbeat HTTP/1.1',
              u'request_time': 0.0,
              u'status': 404.0,
              u'timestamp': '2018-01-05T09:14:46.415000',
              u'upstream_response_time': 0.0},
     'timestamp': '2018-01-05T09:14:46.415000'}
    '''
#TODO Handle nginx error logs

    log = json.loads(line)
    timestamp_iso = datetime.datetime.utcfromtimestamp(float(log['timestamp'])).isoformat()
    log.update({'timestamp':timestamp_iso})
    if '-' in log.get('upstream_response_time'):
        log['upstream_response_time'] = 0.0
    log = convert_str2int(log)

    return dict(
        timestamp=log.get('timestamp',' '),
        data=log
    )

def mongodb(line):
    '''
    >>> import pprint
    >>> input_line1 = '2017-08-17T07:56:33.489+0200 I REPL     [signalProcessingThread] shutting down replication subsystems'
    >>> output_line1 = mongodb(input_line1)
    >>> pprint.pprint(output_line1)
    {'data': {'component': 'REPL',
              'context': '[signalProcessingThread]',
              'message': 'shutting down replication subsystems',
              'severity': 'I',
              'timestamp': '2017-08-17T07:56:33.489+0200'},
     'timestamp': '2017-08-17T07:56:33.489+0200'}

    >>> input_line2 = '2017-08-17T07:56:33.515+0200 W NETWORK  [initandlisten] No primary detected for set confsvr_repl1'
    >>> output_line2 = mongodb(input_line2)
    >>> pprint.pprint(output_line2)
    {'data': {'component': 'NETWORK',
              'context': '[initandlisten]',
              'message': 'No primary detected for set confsvr_repl1',
              'severity': 'W',
              'timestamp': '2017-08-17T07:56:33.515+0200'},
     'timestamp': '2017-08-17T07:56:33.515+0200'}
    '''

    keys = ['timestamp', 'severity', 'component', 'context', 'message']
    values = re.split(r'\s+', line, maxsplit=4)
    mongodb_log = dict(zip(keys,values))

    return dict(
        timestamp=values[0],
        data=mongodb_log
    )

def convert_str2int(data):
    '''
    >>> event = {"event": "api,fn=functioname,host=localhost,name=Server,success=True c_invoked=1, t_duration_count=1,t_duration_lower=0.0259876251221,t_duration_mean=0.0259876251221, t_duration_sum=0.0259876251221,t_duration_upper=0.0259876251221 1494850222862"}
    >>> convert_str2int(event)
    {'event': 'api,fn=functioname,host=localhost,name=Server,success=True c_invoked=1, t_duration_count=1,t_duration_lower=0.0259876251221,t_duration_mean=0.0259876251221, t_duration_sum=0.0259876251221,t_duration_upper=0.0259876251221 1494850222862'}
    '''
    for key, val in data.items():
        if isinstance(val, basestring):
            if val.isdigit() or val.replace('.', '', 1).isdigit() or val.lstrip('-+').replace('.', '', 1).isdigit():
                data[key] = float(val)
    return data

def django(line):
    '''
    >>> import pprint
    >>> input_line1 = '[23/Aug/2017 11:35:25] INFO [app.middleware_log_req:50]View func called:{"exception": null,"processing_time": 0.00011801719665527344, "url": "<url>",host": "localhost", "user": "testing", "post_contents": "", "method": "POST" }'
    >>> output_line1 = django(input_line1)
    >>> pprint.pprint(output_line1)
    {'data': {'loglevel': 'INFO',
              'logname': '[app.middleware_log_req:50]',
              'message': 'View func called:{"exception": null,"processing_time": 0.00011801719665527344, "url": "<url>",host": "localhost", "user": "testing", "post_contents": "", "method": "POST" }',
              'timestamp': '2017-08-23T11:35:25'},
     'timestamp': '2017-08-23T11:35:25'}

    >>> input_line2 = '[22/Sep/2017 06:32:15] INFO [app.function:6022] \
                    {"UUID": "c47f3530-9f5f-11e7-a559-917d011459f7", "timestamp":1506061932546, \
                    "misc": {"status": 200, "ready_state": 4, "end_time_ms": 1506061932546, "url": "/api/function?", \
                    "start_time_ms": 1506061932113, "response_length": 31, "status_message": "OK", "request_time_ms": 433}, \
                    "user": "root", "host_url": "localhost:8888", "message": "ajax success"}'
    >>> output_line2 = django(input_line2)
    >>> pprint.pprint(output_line2)
    {'data': {'loglevel': 'INFO',
              'logname': '[app.function:6022]',
              'message': {u'UUID': u'c47f3530-9f5f-11e7-a559-917d011459f7',
                          u'host_url': u'localhost:8888',
                          u'message': u'ajax success',
                          u'misc': {u'end_time_ms': 1506061932546,
                                    u'ready_state': 4,
                                    u'request_time_ms': 433,
                                    u'response_length': 31,
                                    u'start_time_ms': 1506061932113,
                                    u'status': 200,
                                    u'status_message': u'OK',
                                    u'url': u'/api/function?'},
                          u'timestamp': 1506061932546,
                          u'user': u'root'},
              'timestamp': '2017-09-22T06:32:15'},
     'timestamp': '2017-09-22T06:32:15'}
    
        Case2:
    [18/Sep/2017 05:40:36] ERROR [app.apps:78] failed to get the record, collection = Collection(Database(MongoClient(host=['localhost:27017'], document_class=dict, tz_aware=False, connect=True, serverselectiontimeoutms=3000), u'collection_cache'), u'function_dummy_version')
    Traceback (most recent call last):
      File "/usr/local/lib/python2.7/dist-packages/mongo_cache/mongocache.py", line 70, in __getitem__
    result = self.collection.find_one({"_id": key})
    OperationFailure: not authorized on collection_cache to execute command { find: "function", filter: { _id: "zydelig-cosine-20" }, limit: 1, singleBatch: true }
    '''
#TODO we need to handle case2 logs
    data = {}
    log = re.findall(r'^(\[\d+/\w+/\d+ \d+:\d+:\d+\].*)', line)
    if len(log) == 1:
        data['timestamp'] = datetime.datetime.strptime(re.findall(r'(\d+/\w+/\d+ \d+:\d+:\d+)',\
                log[0])[0],"%d/%b/%Y %H:%M:%S").isoformat()
        data['loglevel'] = re.findall('[A-Z]+', log[0])[1]
        data['logname'] = re.findall('\[\D+.\w+:\d+\]', log[0])[0]
        message = re.findall('\{.+\}', log[0])
        try:
            if len(message) > 0:
                message = json.loads(message[0])
            else:
                message = re.split(']', log[0])
                message = ''.join(message[2:])
        except ValueError:
            message = re.split(']', log[0])
            message = ''.join(message[2:])

        data['message'] = message

        return dict(
                timestamp=data['timestamp'],
                data=data
            )
    else:
        return dict(
            timestamp=datetime.datetime.isoformat(datetime.datetime.utcnow()),
            data=line
        )

def _parse_metric_event(event):
    '''
    >>> event = "api,fn=functioname,host=localhost,name=Server,success=True c_invoked=1, t_duration_count=1,t_duration_lower=0.0259876251221,t_duration_mean=0.0259876251221, t_duration_sum=0.0259876251221,t_duration_upper=0.0259876251221 1494850222862"
    
    >>> _parse_metric_event(event)
    {' t_duration_sum': 0.0259876251221, ' t_duration_count': 1.0, 'name': 'Server', 'success': 'True', 'timestamp': 1494850222862.0, 't_duration_upper': 0.0259876251221, 'c_invoked': 1.0, 'req_fn': 'api', 'host': 'localhost', 't_duration_lower': 0.0259876251221, 't_duration_mean': 0.0259876251221, 'fn': 'functioname'}
    '''
    d = {}
    timestamp = event.split()[-1].strip()
    d['timestamp'] = timestamp
    
    line_parts = event.split(',')
    for index, part in enumerate(line_parts, 1):
    # @part: "server_stats", "api"
        if '=' not in part:
            d['req_fn'] = part
            continue
            
        # Handle cases like
        # "success=True c_inovked=1"
        # "name=server_stats g_cpu_idle_percent=100"
        if part.count('=') == 2:
           val_parts = part.split(' ')
           key, val = val_parts[0].split('=')
           d[key] = val
           key, val = val_parts[1].split('=')
           d[key] = val
           continue

        # @part: "host=localhost"
        key, val = part.split('=')
        if index == len(line_parts):
           val = val.split(' ')[0] # last part, ex: g_mem_percent=0.0 1500029864225

        d[key] = val

    d = convert_str2int(d)
    return d

def basescript(line):
    '''
    >>> import pprint
    >>> input_line1 = '{ "influx_metric": true, "level": "info", "timestamp": "2017-05-15T12:10:22.862458Z", "event": "api,fn=functioname,host=localhost,name=Server,success=True c_invoked=1, t_duration_count=1,t_duration_lower=0.0259876251221,t_duration_mean=0.0259876251221, t_duration_sum=0.0259876251221,t_duration_upper=0.0259876251221 1494850222862" }'
    >>> output_line1 = basescript(input_line1)
    >>> pprint.pprint(output_line1)
    {'data': {u'event': u'api,fn=functioname,host=localhost,name=Server,success=True c_invoked=1, t_duration_count=1,t_duration_lower=0.0259876251221,t_duration_mean=0.0259876251221, t_duration_sum=0.0259876251221,t_duration_upper=0.0259876251221 1494850222862',
              u'influx_metric': True,
              u'level': u'info',
              u'timestamp': u'2017-05-15T12:10:22.862458Z'},
     'id': '',
     'timestamp': u'2017-05-15T12:10:22.862458Z',
     'type': 'log'}
    
    >>> input_line2 = '{"timestamp": "2017-06-14T15:36:09.183493Z", "event": "No precomputed trie found. Creating ...", "_": {"ln": 432, "file": "server.py", "name": "__main__", "fn": "function"}, "level": "debug"}'
    >>> output_line2 = basescript(input_line2)
    >>> pprint.pprint(output_line2)
    {'data': {u'_': {u'file': u'server.py',
                     u'fn': u'function',
                     u'ln': 432,
                     u'name': u'__main__'},
              u'event': u'No precomputed trie found. Creating ...',
              u'level': u'debug',
              u'timestamp': u'2017-06-14T15:36:09.183493Z'},
     'id': '',
     'timestamp': u'2017-06-14T15:36:09.183493Z',
     'type': 'log'}
    '''

    log = json.loads(line)
    type = log.get('type', 'log')
    if type == "metric":
        event = log.get('event', ' ')
        event_dict = _parse_metric_event(event)
        log['event'] = event_dict
        log['session_id'] = event_dict.get("g_session_id", "")
        log['url_id'] = event_dict.get('g_url_id', '')

    log_id = log.get('id', '')
    if isinstance(log_id, unicode):
        log_id = str(log_id)

    return dict(
        timestamp=log.get('timestamp', ' '),
        data=log,
        id=log_id,
        type=type
    )

def elasticsearch(line):
    '''
    >>> import pprint
    >>> input_line = '[2017-08-30T06:27:19,158] \
... [WARN ][o.e.m.j.JvmGcMonitorService] [Glsuj_2] [gc][296816] \
... overhead, spent [1.2s] collecting in the last [1.3s]'
    >>> output_line = elasticsearch(input_line)
    >>> pprint.pprint(output_line)
    {'data': {'garbage_collector': 'gc',
              'gc_count': 296816.0,
              'level': 'WARN ',
              'message': 'o.e.m.j.JvmGcMonitorService',
              'plugin': 'Glsuj_2',
              'query_time_ms': 1200.0,
              'resp_time_ms': 1300.0,
              'timestamp': '2017-08-30T06:27:19,158'},
     'timestamp': '2017-08-30T06:27:19,158'}

    Case 2:
    [2017-09-13T23:15:00,415][WARN ][o.e.i.e.Engine           ] [Glsuj_2] [filebeat-2017.09.09][3] failed engine [index]
    java.nio.file.FileSystemException: /home/user/elasticsearch/data/nodes/0/indices/jsVSO6f3Rl-wwBpQyNRCbQ/3/index/_0.fdx: Too many open files
            at sun.nio.fs.UnixException.translateToIOException(UnixException.java:91) ~[?:?]
    '''

    # TODO we need to handle case2 logs
    elasticsearch_log = line
    actuallog = re.findall(r'(\[\d+\-+\d+\d+\-+\d+\w+\d+:\d+:\d+,+\d\d\d+\].*)', elasticsearch_log)
    if len(actuallog) == 1:
        keys = ['timestamp','level','message','plugin','garbage_collector','gc_count','query_time_ms', 'resp_time_ms']
        values = re.findall(r'\[(.*?)\]', actuallog[0])
        for index, i in enumerate(values):
            if not isinstance(i, str):
                continue
            if len(re.findall(r'.*ms$', i)) > 0 and 'ms' in re.findall(r'.*ms$', i)[0]:
                num = re.split('ms', i)[0]
                values[index]  = float(num)
                continue
            if len(re.findall(r'.*s$', i)) > 0 and 's' in re.findall(r'.*s$', i)[0]:
                num = re.split('s', i)[0]
                values[index] = float(num) * 1000
                continue

        data = dict(zip(keys,values))
        data = convert_str2int(data)

        return dict(
                timestamp=values[0],
                data=data
        )

    else:
        return dict(
                timestamp=datetime.datetime.isoformat(datetime.datetime.now()),
                data=line
        )
