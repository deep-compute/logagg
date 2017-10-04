### Handling the logs

- To get the nginx logs in json format we have to modify the `nginx.conf` file in `/etc/nginx` directory.

- Modify the `Logging Settings` section in the `nginx.conf` file by adding the below lines:
```bash
log_format  json  '{'
                    '"remote_addr": "$remote_addr",'
                    '"remote_user": "$remote_user",'
                    '"timestamp": "$time_iso8601",'
                    '"request": "$request",'
                    '"status": "$status",'
                    '"request_time": "$request_time",'
                    '"body_bytes_sent": "$body_bytes_sent",'
                    '"http_referer": "$http_referer",'
                    '"http_user_agent": "$http_user_agent",'
                    '"http_x_forwarded_for": "$http_x_forwarded_for"'
                    '}';
```

- Modify the line: `access_log /var/log/nginx/access.log;` to `access_log /var/log/nginx/access.log json;`

- To test nginx config file
```bash 
nginx -t
```

- It shows like:
```bash
nginx: the configuration file /etc/nginx/nginx.conf syntax is ok

nginx: configuration file /etc/nginx/nginx.conf test is successful
```

- Restart the nginx by using command:
```bash
/etc/init.d/nginx restart
```

### Install the `logagg` package from
```bash
https://github.com/deep-compute/logagg.git
cd logagg
sudo python setup.py install
```

### Usage

- After installation the logagg module, We can perform operations in the python shell.


```python
>>> import logagg
>>> dir(logagg)
['LogCollector', 'LogForwarder', '__builtins__', '__doc__', '__file__', '__loader__', '__name__', '__package__', '__path__', 'collect', 'command', 'forward', 'main']

>>> dir(logagg.collect)
['__builtins__', '__doc__', '__file__', '__loader__', '__name__', '__package__', '__path__', 'collector', 'handlers']

>>> dir(logagg.collect.handlers)
>>>['__builtins__', '__doc__', '__file__', '__loader__', '__name__', '__package__', '_parse_metric_event', 'basescript', 'convert_str2int', 'datetime', 'django', 'elasticsearch', 'json', 'mongodb', 'nginx_access', 're']

>>> logagg.collect.handlers.mongodb('2017-08-17T07:56:33.489+0200 I REPL     [signalProcessingThread] shutting down replication subsystems')
{'timestamp': '2017-08-17T07:56:33.489+0200', 'data': {'timestamp': '2017-08-17T07:56:33.489+0200', 'message': 'shutting down replication subsystems', 'component': 'REPL', 'severity': 'I', 'context': '[signalProcessingThread]'}, 'type': 'log'}
```
