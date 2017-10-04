# logagg
logs aggregation framework

Collect all the logs from the server and parses it by making a common schema for all the logs and stores at given storage engine.

## Installation
> Prerequisites: Python2.7

> Note: tested and it works in python2.7 and not tested in other versions.

- Install the `logagg` package by running commands,
```bash
git clone "https://github.com/deep-compute/logagg.git"
cd logagg
sudo python setup.py install
```

## Usage

### Bring up the `nsq` instances at the server
```
nsqlookupd
nsqd -lookupd-tcp-address localhost:4160
nsqadmin -lookupd-http-address localhost:4161
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
                        MongoDB
```

- For `logagg collect`
```bash
logagg collect --help
```
- You should see something like
```bash
usage: logagg collect [-h] [--nsqd-http-address NSQD_HTTP_ADDRESS]
                      file [file ...] nsqtopic

positional arguments:
  file                  Provide absolute path of log file including the module name
                        and function name, eg: /var/log/nginx/access.log:logag
                        g.collect.handlers.nginx_access
  nsqtopic              Topic name to publish messages. Ex: logs_and_metrics

optional arguments:
  -h, --help            show this help message and exit
  --nsqd-http-address NSQD_HTTP_ADDRESS
                        nsqd HTTP address where we send the messages
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

```

### How to run the collector?

- Run `collector` by using command:
```bash
logagg collect /path/to/input/log_file:logagg.collect.handlers.<handler_name> <topic name> --nsqd-http-address <nsqd http address>
```

- Example run command:
```bash
logagg collect /var/log/nginx/access.log:logagg.collect.handlers.nginx_access nginx --nsqd-http-address localhost:4151  
```

### How to run the forwarder?

- Run `forwarder` by using command:
```bash
logagg forward --nsqtopic <topic name> --nsqchannel <channel name> --nsqd-tcp-address <nsqd tcp address> --mongodb-user-name <username> --mongodb-password <password> --mongodb-server-url <server host> --mongodb-port <port num> --mongodb-database <database name> --mongodb-collection <collection name>
```

- Example run command:
```bash
logagg forward --nsqtopic nginx --nsqchannel test --nsqd-tcp-address localhost:4150 --mongodb-user-name abc --mongodb-password xxxxxx --mongodb-server-url localhost:27017 --mongodb-port 27017 --mongodb-database logs --mongodb-collection nginx
```

### Check the message traffic at nsq
- We can check, how many messages that are being written to nsq and reading from nsq through the browser, by going through the link:
```bash
localhost:4171
```

### How to check the records at forwarder end with a storage engine.
- Initially, we are sending logs to MongoDB. Further, we can support to send the logs to different storage engines.

- Connect to the mongo shell and perform queries:
- Example to get the records for `nginx`:
```mongodb  
> use nginx
> db.logs.find({'handler': 'logagg.collect.handlers.nginx_access', 'data.request_time' : {$gt: 0}})
```
```json
{
  "_id": "4ca83315a2b711e7bcf910bf487fe126",
  "timestamp": " ",
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

- Example to get the records for `MongoDB`
```mongodb
> use mongodb
> db.mongodb_collection.find({handler: 'logagg.collect.handlers.mongodb', timestamp: '2017-07-29T10:01:13.295+0200'})
```
```json
{
  "_id": "07dd0055a28c11e7b37ec86000be3ada",
  "timestamp": "2017-07-29T10:01:13.295+0200",
  "data": {
    "timestamp": "2017-07-29T10:01:13.295+0200",
    "message": "distarch: x86_64",
    "component": "CONTROL",
    "severity": "I",
    "context": "[initandlisten]"
  },
  "raw": "2017-07-29T10:01:13.295+0200 I CONTROL  [initandlisten]     distarch: x86_64",
  "host": "localhost",
  "handler": "logagg.collect.handlers.mongodb",
  "file": "/var/log/mongodb/mongodb.log",
  "type": "log"
}
```

- Example to get the records for `django`
```bash
> use django
> db.django.find({handler: 'logagg.collect.handlers.django', 'data.message.processing_time': 617.0011630058289})
```
```json
{
  "_id": "bf20a6bfa2c511e7b530c86000be3ada",
  "timestamp": "2017-09-26T14:19:45",
  "data": {
    "loglevel": "INFO",
    "timestamp": "2017-09-26T14:19:45",
    "message": {
      "exception": null,
      "processing_time": 617.0011630058289,
      "url": "<url>",
      "host": "localhost:9998",
      "user": "root",
      "post_contents": "",
      "method": "POST"
    },
    "logname": "[app.middleware_log_req:50]"
  },
  "raw": "[26/Sep/2017 14:19:45] INFO [app.middleware_log_req:50] View func called: {\"exception\": null, \"processing_time\": 617.0011630058289, \"url\": \"<url>\", \"host\": \"localhost:9998\", \"user\": \"root\", \"post_contents\": \"\", \"method\": \"POST\"}",
  "host": "localhost",
  "handler": "logagg.collect.handlers.django",
  "file": "/var/log/web/web.log",
  "type": "log"
}
```

- Example to get the records for `elasticsearch`
```bash
> use elasticsearch
> db.eslog.find({'handler': 'logagg.collect.handlers.elasticsearch', 'data.resp_time_ms' : {$gt : 1000}})
```
```json
{
  "_id": "22b6983fa34811e7bcf910bf487fe126",
  "timestamp": "2017-09-25T06:19:00,843",
  "data": {
    "gc_count": 1702635,
    "resp_time_ms": 2400,
    "level": "WARN ",
    "timestamp": "2017-09-25T06:19:00,843",
    "plugin": "Glsuj_2",
    "query_time": 1400,
    "garbage_collector": "gc"
    "message": "o.e.m.j.JvmGcMonitorService"
  },
  "raw": "[2017-09-25T06:19:00,843][WARN ][o.e.m.j.JvmGcMonitorService] [Glsuj_2] [gc][1702635] overhead, spent [1.4s] collecting in the last [2.4s]",
  "host": "localhost",
  "handler": "logagg.collect.handlers.elasticsearch",
  "file": "/var/log/es/elasticsearch.log",
  "type": "log"
}
```

- Example to get records for `basescript`
```bash
> use basescript
> db.servers_stats.find({'handler': 'logagg.collect.handlers.basescript', 'data.event.g_ram_usage_percent': {$gt : 32}})
```

```json
{
  "_id": "39116307a45811e7abd338d547000c37",
  "timestamp": "2017-09-10T01:11:02.162474Z",
  "data": {
    "timestamp": "2017-09-10T01:11:02.162474Z",
    "event": {
      "g_ram_avail": 134063063040,
      "g_disk_usage_percent": 14.3,
      "g_cpu_idle_percent": 99.83,
      "name": "server_stats",
      "g_ram_free": 4724101120,
      "g_disk_total": 943679549440,
      "timestamp": 1505005862162,
      "g_swapmemory_usage_percent": 0,
      "g_disk_usage": 128429932544,
      "g_swapmemory_usage": 0,
      "req_fn": "server_stats",
      "g_ram_total": 270377811968,
      "g_ram_usage": 133936140288,
      "g_swapmemory_free": 0,
      "g_swapmemory_total": 0,
      "host": "localhost",
      "g_cpu_usage_precent": 0.17,
      "g_disk_free": 767289798656,
      "g_ram_usage_percent": 50.4
    },
    "influx_metric": true,
    "level": "info"
  },
  "raw": "{\"timestamp\": \"2017-09-10T01:11:02.162474Z\", \"event\": \"server_stats,host=localhost,name=server_stats g_cpu_idle_percent=99.83,g_cpu_usage_precent=0.17,g_disk_free=767289798656,g_disk_total=943679549440,g_disk_usage=128429932544,g_disk_usage_percent=14.3,g_ram_avail=134063063040,g_ram_free=4724101120,g_ram_total=270377811968,g_ram_usage=133936140288,g_ram_usage_percent=50.4,g_swapmemory_free=0,g_swapmemory_total=0,g_swapmemory_usage=0,g_swapmemory_usage_percent=0.0 1505005862162\", \"influx_metric\": true, \"level\": \"info\"}",
  "host": "localhost",
  "handler": "logagg.collect.handlers.basescript",
  "file": "/var/log/stats/server_stats.log",
  "type": "metric"
}
```
