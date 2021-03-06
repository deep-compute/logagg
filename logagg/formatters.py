import re
import ujson as json
import datetime


class RawLog(dict):
    pass


# FIXME: cannot do both returns .. should it?
def docker_file_log_driver(line):
    log = json.loads(json.loads(line)["msg"])
    if "formatter" in log.get("extra"):
        return RawLog(
            dict(
                formatter=log.get("extra").get("formatter"),
                raw=log.get("message"),
                host=log.get("host"),
                timestamp=log.get("timestamp"),
            )
        )
    return dict(timestamp=log.get("timestamp"), data=log, type="log")


HAPROXY_HEADERS = re.compile(r"{(.*)} ")


def haproxy(line):
    # TODO Handle all message formats
    """
    >>> import pprint
    >>> input_line1 = 'Apr 24 00:00:02 node haproxy[12298]: 1.1.1.1:48660 [24/Apr/2019:00:00:02.358] pre-staging~ pre-staging_doc/pre-staging_active 261/0/2/8/271 200 2406 - - ---- 4/4/0/1/0 0/0 {AAAAAA:AAAAA_AAAAA:AAAAA_AAAAA_AAAAA:300A|||HMAC user@mail.net:sdasdasdasdsdasAHDivsjd=|user@mail.net|2018} "GET /doc/api/get?call=apple HTTP/1.1"'
    >>> output_line1 = haproxy(input_line1)
    >>> pprint.pprint(output_line1)
    {'data': {'Tc': 2.0,
              'Tq': 261.0,
              'Tr': 8.0,
              'Tw': 0.0,
              '_headers': ['AAAAAA:AAAAA_AAAAA:AAAAA_AAAAA_AAAAA:300A',
                           '',
                           '',
                           'HMAC user@mail.net:sdasdasdasdsdasAHDivsjd=',
                           'user@mail.net',
                           '2018'],
              '_url_params': {'call': 'apple'},
              'actconn': 4,
              'backend': 'pre-staging_doc/pre-staging_active',
              'backend_queue': 0,
              'beconn': 0,
              'bytes_read': 2406,
              'client_port': 48660,
              'client_server': '1.1.1.1',
              'feconn': 4,
              'front_end': 'pre-staging~',
              'haproxy_server': 'node',
              'http_version': 'HTTP/1.1',
              'method': 'GET',
              'resp_time': 271.0,
              'retries': 0,
              'srv_conn': 1,
              'srv_queue': 0,
              'status': '200',
              'timestamp': '2019-04-24T00:00:02.358000',
              'url_path': '/doc/api/get'},
     'event': 'haproxy_event',
     'timestamp': '2019-04-24T00:00:02.358000',
     'type': 'metric'}
    """

    _headers = HAPROXY_HEADERS.findall(line)
    if _headers:
        _headers = _headers[0].strip().split("|")
    else:
        _headers = []

    http_version = re.findall(r'(HTTP.*)"', line)
    if http_version:
        http_version = http_version[0].strip('"').strip()
    else:
        http_version = ""

    line = re.sub(r"(HTTP.*)", "", line).strip()

    _line = HAPROXY_HEADERS.sub("", line).strip().split()

    _, _, _, haproxy_server, _, client_server_info, timestamp, front_end, backend, resp_timings, status, bytes_read, _, _, _, conn_stats, inqueue_stats, method, url = (
        _line
    )

    keys = [
        "_headers",
        "haproxy_server",
        "client_server",
        "client_port",
        "timestamp",
        "front_end",
        "backend",
        "Tq",
        "Tw",
        "Tc",
        "Tr",
        "resp_time",
        "status",
        "bytes_read",
        "retries",
        "actconn",
        "feconn",
        "beconn",
        "srv_conn",
        "srv_queue",
        "backend_queue",
        "method",
        "url_path",
        "_url_params",
        "http_version",
    ]

    haproxy_server = haproxy_server.strip()

    client_server, client_port = client_server_info.split(":")
    client_server = client_server.strip()
    client_port = int(client_port.strip())

    _timestamp = re.findall(r"\[(.*?)\]", timestamp)[0]
    timestamp = datetime.datetime.strptime(
        _timestamp, "%d/%b/%Y:%H:%M:%S.%f"
    ).isoformat()

    front_end = front_end.strip()
    backend = backend.strip()

    Tq, Tw, Tc, Tr, resp_time = [float(rt.strip()) for rt in resp_timings.split("/")]

    status = status.strip()
    bytes_read = int(bytes_read.strip())

    actconn, feconn, beconn, srv_conn, retries = [
        int(cs.strip()) for cs in conn_stats.split("/")
    ]
    srv_queue, backend_queue = [
        int(inqstat.strip()) for inqstat in inqueue_stats.split("/")
    ]
    method = method.strip('"').strip()

    _url = url.strip().split("?")
    url_params = ""
    if len(_url) == 2:
        url_path, url_params = [u.strip() for u in _url]
    else:
        url_path = _url[0]

    _url_params = {}
    for params in url_params.split("&"):
        if not params:
            continue

        _p = params.strip().split("=")
        if not len(_p) == 2:
            continue

        key, val = _p
        _url_params[key.strip()] = val.strip()

    http_version = http_version.strip('"').strip()

    log = dict(zip(keys, [eval(k) for k in keys]))

    return dict(
        data=log, event="haproxy_event", timestamp=log.get("timestamp"), type="metric"
    )


def nginx_access(line):
    """
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
              u'status': u'200',
              u'timestamp': '2018-01-05T09:31:39.201000',
              u'upstream_response_time': 0.0},
     'event': 'nginx_event',
     'timestamp': '2018-01-05T09:31:39.201000',
     'type': 'metric'}

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
              u'status': u'404',
              u'timestamp': '2018-01-05T09:14:46.415000',
              u'upstream_response_time': 0.0},
     'event': 'nginx_event',
     'timestamp': '2018-01-05T09:14:46.415000',
     'type': 'metric'}
    """
    # TODO Handle nginx error logs
    log = json.loads(line)
    timestamp_iso = datetime.datetime.utcfromtimestamp(
        float(log["timestamp"])
    ).isoformat()
    log.update({"timestamp": timestamp_iso})
    if "-" in log.get("upstream_response_time"):
        log["upstream_response_time"] = 0.0
    log["body_bytes_sent"] = float(log["body_bytes_sent"])
    log["request_time"] = float(log["request_time"])
    log["upstream_response_time"] = float(log["upstream_response_time"])

    return dict(
        timestamp=log.get("timestamp", " "),
        data=log,
        type="metric",
        event="nginx_event",
    )


def mongodb(line):
    """
    >>> import pprint
    >>> input_line1 = '2017-08-17T07:56:33.489+0200 I REPL     [signalProcessingThread] shutting down replication subsystems'
    >>> output_line1 = mongodb(input_line1)
    >>> pprint.pprint(output_line1)
    {'data': {'component': 'REPL',
              'context': '[signalProcessingThread]',
              'message': 'shutting down replication subsystems',
              'severity': 'I',
              'timestamp': '2017-08-17T07:56:33.489+0200'},
     'timestamp': '2017-08-17T07:56:33.489+0200',
     'type': 'log'}

    >>> input_line2 = '2017-08-17T07:56:33.515+0200 W NETWORK  [initandlisten] No primary detected for set confsvr_repl1'
    >>> output_line2 = mongodb(input_line2)
    >>> pprint.pprint(output_line2)
    {'data': {'component': 'NETWORK',
              'context': '[initandlisten]',
              'message': 'No primary detected for set confsvr_repl1',
              'severity': 'W',
              'timestamp': '2017-08-17T07:56:33.515+0200'},
     'timestamp': '2017-08-17T07:56:33.515+0200',
     'type': 'log'}
    """

    keys = ["timestamp", "severity", "component", "context", "message"]
    values = re.split(r"\s+", line, maxsplit=4)
    mongodb_log = dict(zip(keys, values))

    return dict(timestamp=values[0], data=mongodb_log, type="log")


def django(line):
    """
    >>> import pprint
    >>> input_line1 = '[23/Aug/2017 11:35:25] INFO [app.middleware_log_req:50]View func called:{"exception": null,"processing_time": 0.00011801719665527344, "url": "<url>",host": "localhost", "user": "testing", "post_contents": "", "method": "POST" }'
    >>> output_line1 = django(input_line1)
    >>> pprint.pprint(output_line1)
    {'data': {'loglevel': 'INFO',
              'logname': '[app.middleware_log_req:50]',
              'message': 'View func called:{"exception": null,"processing_time": 0.00011801719665527344, "url": "<url>",host": "localhost", "user": "testing", "post_contents": "", "method": "POST" }',
              'timestamp': '2017-08-23T11:35:25'},
     'level': 'INFO',
     'timestamp': '2017-08-23T11:35:25'}

    >>> input_line2 = '[22/Sep/2017 06:32:15] INFO [app.function:6022] {"UUID": "c47f3530-9f5f-11e7-a559-917d011459f7", "timestamp":1506061932546, "misc": {"status": 200, "ready_state": 4, "end_time_ms": 1506061932546, "url": "/api/function?", "start_time_ms": 1506061932113, "response_length": 31, "status_message": "OK", "request_time_ms": 433}, "user": "root", "host_url": "localhost:8888", "message": "ajax success"}'
    >>> output_line2 = django(input_line2)
    >>> pprint.pprint(output_line2)
    {'data': {'loglevel': 'INFO',
              'logname': '[app.function:6022]',
              'message': {u'UUID': u'c47f3530-9f5f-11e7-a559-917d011459f7',
                          u'host_url': u'localhost:8888',
                          u'message': u'ajax success',
                          u'misc': {u'end_time_ms': 1506061932546L,
                                    u'ready_state': 4,
                                    u'request_time_ms': 433,
                                    u'response_length': 31,
                                    u'start_time_ms': 1506061932113L,
                                    u'status': 200,
                                    u'status_message': u'OK',
                                    u'url': u'/api/function?'},
                          u'timestamp': 1506061932546L,
                          u'user': u'root'},
              'timestamp': '2017-09-22T06:32:15'},
     'level': 'INFO',
     'timestamp': '2017-09-22T06:32:15'}

        Case2:
    [18/Sep/2017 05:40:36] ERROR [app.apps:78] failed to get the record, collection = Collection(Database(MongoClient(host=['localhost:27017'], document_class=dict, tz_aware=False, connect=True, serverselectiontimeoutms=3000), u'collection_cache'), u'function_dummy_version')
    Traceback (most recent call last):
      File "/usr/local/lib/python2.7/dist-packages/mongo_cache/mongocache.py", line 70, in __getitem__
    result = self.collection.find_one({"_id": key})
    OperationFailure: not authorized on collection_cache to execute command { find: "function", filter: { _id: "zydelig-cosine-20" }, limit: 1, singleBatch: true }
    """
    # TODO we need to handle case2 logs
    data = {}
    log = re.findall(r"^(\[\d+/\w+/\d+ \d+:\d+:\d+\].*)", line)
    if len(log) == 1:
        data["timestamp"] = datetime.datetime.strptime(
            re.findall(r"(\d+/\w+/\d+ \d+:\d+:\d+)", log[0])[0], "%d/%b/%Y %H:%M:%S"
        ).isoformat()
        data["loglevel"] = re.findall("[A-Z]+", log[0])[1]
        data["logname"] = re.findall("\[\D+.\w+:\d+\]", log[0])[0]
        message = re.findall("\{.+\}", log[0])
        try:
            if len(message) > 0:
                message = json.loads(message[0])
            else:
                message = re.split("]", log[0])
                message = "".join(message[2:])
        except ValueError:
            message = re.split("]", log[0])
            message = "".join(message[2:])

        data["message"] = message

        return dict(timestamp=data["timestamp"], level=data["loglevel"], data=data)
    else:
        return dict(
            timestamp=datetime.datetime.isoformat(datetime.datetime.utcnow()),
            data={raw: line},
        )


def basescript(line):
    """
    >>> import pprint
    >>> input_line = '{"level": "warning", "timestamp": "2018-02-07T06:37:00.297610Z", "event": "exited via keyboard interrupt", "type": "log", "id": "20180207T063700_4d03fe800bd111e89ecb96000007bc65", "_": {"ln": 58, "file": "/usr/local/lib/python2.7/dist-packages/basescript/basescript.py", "name": "basescript.basescript", "fn": "start"}}'
    >>> output_line1 = basescript(input_line)
    >>> pprint.pprint(output_line1)
    {'data': {u'_': {u'file': u'/usr/local/lib/python2.7/dist-packages/basescript/basescript.py',
                     u'fn': u'start',
                     u'ln': 58,
                     u'name': u'basescript.basescript'},
              u'event': u'exited via keyboard interrupt',
              u'id': u'20180207T063700_4d03fe800bd111e89ecb96000007bc65',
              u'level': u'warning',
              u'timestamp': u'2018-02-07T06:37:00.297610Z',
              u'type': u'log'},
     'event': u'exited via keyboard interrupt',
     'id': u'20180207T063700_4d03fe800bd111e89ecb96000007bc65',
     'level': u'warning',
     'timestamp': u'2018-02-07T06:37:00.297610Z',
     'type': u'log'}
    """

    log = json.loads(line)

    return dict(
        timestamp=log["timestamp"],
        data=log,
        id=log["id"],
        type=log["type"],
        level=log["level"],
        event=log["event"],
    )


def elasticsearch(line):
    """
    >>> import pprint
    >>> input_line = '[2017-08-30T06:27:19,158] [WARN ][o.e.m.j.JvmGcMonitorService] [Glsuj_2] [gc][296816] overhead, spent [1.2s] collecting in the last [1.3s]'
    >>> output_line = elasticsearch(input_line)
    >>> pprint.pprint(output_line)
    {'data': {'garbage_collector': 'gc',
              'gc_count': 296816.0,
              'level': 'WARN',
              'message': 'o.e.m.j.JvmGcMonitorService',
              'plugin': 'Glsuj_2',
              'query_time_ms': 1200.0,
              'resp_time_ms': 1300.0,
              'timestamp': '2017-08-30T06:27:19,158'},
     'event': 'o.e.m.j.JvmGcMonitorService',
     'level': 'WARN ',
     'timestamp': '2017-08-30T06:27:19,158',
     'type': 'metric'}

    Case 2:
    [2017-09-13T23:15:00,415][WARN ][o.e.i.e.Engine           ] [Glsuj_2] [filebeat-2017.09.09][3] failed engine [index]
    java.nio.file.FileSystemException: /home/user/elasticsearch/data/nodes/0/indices/jsVSO6f3Rl-wwBpQyNRCbQ/3/index/_0.fdx: Too many open files
            at sun.nio.fs.UnixException.translateToIOException(UnixException.java:91) ~[?:?]
    """

    # TODO we need to handle case2 logs
    elasticsearch_log = line
    actuallog = re.findall(
        r"(\[\d+\-+\d+\d+\-+\d+\w+\d+:\d+:\d+,+\d\d\d+\].*)", elasticsearch_log
    )
    if len(actuallog) == 1:
        keys = [
            "timestamp",
            "level",
            "message",
            "plugin",
            "garbage_collector",
            "gc_count",
            "query_time_ms",
            "resp_time_ms",
        ]
        values = re.findall(r"\[(.*?)\]", actuallog[0])
        for index, i in enumerate(values):
            if not isinstance(i, str):
                continue
            if len(re.findall(r".*ms$", i)) > 0 and "ms" in re.findall(r".*ms$", i)[0]:
                num = re.split("ms", i)[0]
                values[index] = float(num)
                continue
            if len(re.findall(r".*s$", i)) > 0 and "s" in re.findall(r".*s$", i)[0]:
                num = re.split("s", i)[0]
                values[index] = float(num) * 1000
                continue

        data = dict(zip(keys, values))
        if "level" in data and data["level"][-1] == " ":
            data["level"] = data["level"][:-1]
        if "gc_count" in data:
            data["gc_count"] = float(data["gc_count"])
        event = data["message"]
        level = values[1]
        timestamp = values[0]

        return dict(
            timestamp=timestamp, level=level, type="metric", data=data, event=event
        )

    else:
        return dict(
            timestamp=datetime.datetime.isoformat(datetime.datetime.now()),
            data={"raw": line},
        )


LOG_BEGIN_PATTERN = [re.compile(r"^\s+\["), re.compile(r"^\[")]


def elasticsearch_ispartial_log(line):
    """
    >>> line1 = '  [2018-04-03T00:22:38,048][DEBUG][o.e.c.u.c.QueueResizingEsThreadPoolExecutor] [search17/search]: there were [2000] tasks in [809ms], avg task time [28.4micros], EWMA task execution [790nanos], [35165.36 tasks/s], optimal queue is [35165], current capacity [1000]'
    >>> line2 = '  org.elasticsearch.ResourceAlreadyExistsException: index [media_corpus_refresh/6_3sRAMsRr2r63J6gbOjQw] already exists'
    >>> line3 = '   at org.elasticsearch.cluster.metadata.MetaDataCreateIndexService.validateIndexName(MetaDataCreateIndexService.java:151) ~[elasticsearch-6.2.0.jar:6.2.0]'
    >>> elasticsearch_ispartial_log(line1)
    False
    >>> elasticsearch_ispartial_log(line2)
    True
    >>> elasticsearch_ispartial_log(line3)
    True
    """
    match_result = []

    for p in LOG_BEGIN_PATTERN:
        if re.match(p, line) != None:
            return False
    return True


elasticsearch.ispartial = elasticsearch_ispartial_log
