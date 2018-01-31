# logagg
logs aggregation framework

Collect all the logs from the server and parses it by making a common schema for all the logs and stores at given storage engine.

## Installation
> Prerequisites: Python2.7

> Note: tested and it works in python2.7 and not tested in other versions.

- Install the `nsq` package, at where we need to bring up the `nsq` server.
- Run the following commands to install `nsq`:
```bash
sudo apt-get install libsnappy-dev
wget https://s3.amazonaws.com/bitly-downloads/nsq/nsq-1.0.0-compat.linux-amd64.go1.8.tar.gz
tar zxvf nsq-1.0.0-compat.linux-amd64.go1.8.tar.gz
sudo cp nsq-1.0.0-compat.linux-amd64.go1.8/bin/{nsqd,nsqlookupd,nsqadmin} /usr/local/bin
```

### Install the `logagg` package by running commands,
- Install the `logagg` package, at where we collect the logs and at where we forward the logs:
- Run the following commands to install `logagg`:
```bash
git clone "https://github.com/deep-compute/logagg.git"
cd logagg
sudo python setup.py install
```

## Usage

### Bring up the `nsq` instances at the required server:
```
nsqlookupd
nsqd -lookupd-tcp-address <ip-addr or DNS>:4160
nsqadmin -lookupd-http-address <ip-addr or DNS>:4161
```
### Handling the logs

- To get the nginx logs in json format we have to modify the `nginx.conf` file in `/etc/nginx` directory.

- Modify the `Logging Settings` section in the `nginx.conf` file by adding the below lines:
```bash
log_format  json  '{'
                    '"remote_addr": "$remote_addr",'
                    '"remote_user": "$remote_user",'
                    '"timestamp": "$msec",'
                    '"request": "$request",'
                    '"status": "$status",'
                    '"request_time": "$request_time",'
                    '"body_bytes_sent": "$body_bytes_sent",'
                    '"http_referer": "$http_referer",'
                    '"http_user_agent": "$http_user_agent",'
                    '"http_x_forwarded_for": "$http_x_forwarded_for",'
                    '"upstream_response_time": "$upstream_response_time"'
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

### Usage

- After installation of the logagg module, we can perform operations in the python shell.


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

### Types of handlers we support
- nginx_access
- Django
- MongoDB
- Elasticsearch
- Basescript

### Help command
- For `logagg`
```bash
logagg --help
```
- you should see something like:
```bash
usage: logagg [-h] [--name NAME] [--log-level LOG_LEVEL]
              [--log-format {json,pretty}] [--log-file LOG_FILE] [--quiet]
              {collect,forward,run} ...

Logagg command line tool

optional arguments:
  -h, --help            show this help message and exit
  --name NAME           Name to identify this instance
  --log-level LOG_LEVEL
                        Logging level as picked from the logging module
  --log-format {json,pretty}
                        Force the format of the logs. By default, if the
                        command is from a terminal, print colorful logs.
                        Otherwise, print json.
  --log-file LOG_FILE   Writes logs to log file if specified, default: None
  --quiet               if true, does not print logs to stderr, default: False

commands:
  {collect,forward,run}
    collect             Collects the logs from different processes and sends
                        to nsq
    forward             Collects all the messages from nsq and pushes to
                        storage engine
```

- For `logagg collect`
```bash
logagg collect --help
```
- You should see something like
```bash
usage: logagg collect [-h] [--nsqchannel NSQCHANNEL]
                      [--nsqd-http-address NSQD_HTTP_ADDRESS]
                      [--depth-limit-at-nsq DEPTH_LIMIT_AT_NSQ]
                      [--exception-logs-file EXCEPTION_LOGS_FILE]
                      file [file ...] nsqtopic

positional arguments:
  file                  Provide absolute path of log file including the module name
                        and function name, eg: /var/log/nginx/access.log:logag
                        g.collect.handlers.nginx_access
  nsqtopic              Topic name to publish messages. Ex: logs_and_metrics

optional arguments:
  -h, --help            show this help message and exit
  --nsqchannel NSQCHANNEL
                        Channel of nsqd
  --nsqd-http-address NSQD_HTTP_ADDRESS
                        nsqd HTTP address where we send the messages
  --depth-limit-at-nsq DEPTH_LIMIT_AT_NSQ
                        To limit the depth at nsq channel
  --exception-logs-file EXCEPTION_LOGS_FILE
                        If collector fails to publish messages to nsq, will
                        write the logs to a file
```

- For `logagg forward`,
```bash
logagg forward --help
```
- You should see something like
```bash
usage: logagg forward [-h] [--nsqtopic NSQTOPIC] [--nsqchannel NSQCHANNEL]
                      [--nsqd-tcp-address NSQD_TCP_ADDRESS]
                      [--mongodb-server-url MONGODB_SERVER_URL]
                      [--mongodb-port MONGODB_PORT]
                      [--mongodb-user-name MONGODB_USER_NAME]
                      [--mongodb-password MONGODB_PASSWORD]
                      [--mongodb-database MONGODB_DATABASE]
                      [--mongodb-collection MONGODB_COLLECTION]
                      [--influxdb-server-url INFLUXDB_SERVER_URL]
                      [--influxdb-port INFLUXDB_PORT]
                      [--influxdb-user-name INFLUXDB_USER_NAME]
                      [--influxdb-password INFLUXDB_PASSWORD]
                      [--influxdb-database INFLUXDB_DATABASE]

optional arguments:
  -h, --help            show this help message and exit
  --nsqtopic NSQTOPIC   NSQ topic name to read messages from. Ex:
                        logs_and_metrics
  --nsqchannel NSQCHANNEL
                        the channel of nsqd
  --nsqd-tcp-address NSQD_TCP_ADDRESS
                        nsqd TCP address where we get the messages
  --mongodb-server-url MONGODB_SERVER_URL
                        DNS of the server where mongo is running
  --mongodb-port MONGODB_PORT
                        port where mongo is running
  --mongodb-user-name MONGODB_USER_NAME
                        username of MongoDB
  --mongodb-password MONGODB_PASSWORD
                        password to authenticate MongoDB
  --mongodb-database MONGODB_DATABASE
                        database to store logs
  --mongodb-collection MONGODB_COLLECTION
                        collection to store logs
  --influxdb-server-url INFLUXDB_SERVER_URL
                        DNS of the server where influxdb is running
  --influxdb-port INFLUXDB_PORT
                        port where influxdb is running
  --influxdb-user-name INFLUXDB_USER_NAME
                        username of influxdb
  --influxdb-password INFLUXDB_PASSWORD
                        password to authenticate influxdb
  --influxdb-database INFLUXDB_DATABASE
                        database to store metrics
```
> The channel name `--nsqchannel` at logagg collect and logagg forward should be the same name.

### How to run the collector?

- Run `collector` by using command:
```bash
logagg collect /path/to/input/log_file:logagg.collect.handlers.<handler_name> <topic name> --nsqchannel <channel name> --nsqd-http-address <nsqd http address> --depth-limit-at-nsq <limit value> --exception-logs-file <file to write logs>
```

- Example run command:
```bash
logagg collect /var/log/nginx/access.log:logagg.collect.handlers.nginx_access nginx --nsqchannel test --nsqd-http-address localhost:4151 --depth-limit-at-nsq 150000 --exception-logs-file /var/log/logagg/exception_logs.log
```

### How to run the forwarder?

- Run `forwarder` by using command:
```bash
logagg forward --nsqtopic <topic name> --nsqchannel <channel name> --nsqd-tcp-address <nsqd tcp address> --mongodb-user-name <username> --mongodb-password <password> --mongodb-server-url <server or  host> --mongodb-port <port num> --mongodb-database <database name> --mongodb-collection <collection name> --influxdb-server-url <server or host> --influxdb-port <port num> --influxdb-user-name <username> --influxdb-password <password> --influxdb-database <database name>
```

- Example run command:
```bash
logagg forward --nsqtopic nginx --nsqchannel test --nsqd-tcp-address localhost:4150 --mongodb-user-name abc --mongodb-password xxxxxx --mongodb-server-url localhost:27017 --mongodb-port 27017 --mongodb-database logs --mongodb-collection nginx --influxdb-server-url localhost --influxdb-port 8086 --influxdb-user-name abc --influxdb-password xxxxxx --influxdb-database metrics
```

### Check the message traffic at nsq
- We can check, how many messages that are being written to nsq and reading from nsq through the browser, by going through the link:
```bash
<nsq server name or ip-addr>:4171
```

### How to check the records at forwarder end with a storage engine.
- We are sending logs to MongoDB and metrics to InfluxDB. Further, we can support to send the logs to different storage engines.

#### How to check records at MongoDB?
- Connect to the mongo shell and perform queries:
```mongodb
> use database_name
> db.collection_name.find({'handler': 'logagg.collect.handlers.<handler_name>'})
```
- You can see the basic format of record like below:
```json
{
  "_id": "UUID1",
  "timestamp": "isoformat_time. Ex: 2017-08-01T07:32:24.183981Z",
  "data": {},
  "raw": "raw_log_line",
  "host": "x.com",
  "handler": "logagg.collect.handlers.<handler-name>",
  "file": "/path/to/log/file",
  "type": "log | metric"
}
```

- Example to get the records for `nginx`:
```mongodb  
> use nginx
> db.logs.find({'handler': 'logagg.collect.handlers.nginx_access', 'data.request_time' : {$gt: 0}})
```
- You should see something like below:
```json
{
  "_id": "4ca83315a2b711e7bcf910bf487fe126",
  "timestamp": "2017-08-01T07:32:24.183981Z",
  "data": {
    "status": 404,
    "body_bytes_sent": 152,
    "remote_user": "-",
    "request_time": 3.744,
    "http_referer": "-",
    "remote_addr": "127.0.0.1",
    "http_x_forwarded_for": "-",
    "request": "GET /muieblackcat HTTP/1.1",
    "http_user_agent": "-",
    "time_local": "2017-09-26T13:49:37+02:00"
  },
  "raw": "{\"remote_addr\": \"127.0.0.1\",\"remote_user\": \"-\",\"time_local\": \"2017-09-26T13:49:37+02:00\",\"request\": \"GET /muieblackcat HTTP/1.1\",\"status\": \"404\",\"request_time\": \"3.744\",\"body_bytes_sent\": \"152\",\"http_referer\": \"-\",\"http_user_agent\": \"-\",\"http_x_forwarded_for\": \"-\"}",
  "host": "localhost",
  "handler": "logagg.collect.handlers.nginx_access",
  "file": "/var/log/nginx/access.log",
  "type": "log"
}
```
#### How to check metrics at InfluxDB?
- For metrics, connect to the InfluxDB shell and perform queries.
```influxdb
> use database_name
> show measurements
> select <field_key> from <measurement_name>
```
- Example to get the metrics for a `measurement`:
```influxdb
> use nginx
> select request_time from nginx_metric limit 10
```
- You should see something like below:
```
time                request_time
----                ------------
1508770751000000000 0.027
1508770751000000000 0.026
1508770753000000000 0.272
1508770754000000000 0.028
1508770756000000000 0.026
1508770756000000000 0.007
1508770757000000000 0.511
1508770758000000000 0
1508770761000000000 0.228
1508770761000000000 0.247
```


-------------------------------------------------------------------


