# logagg
**logs aggregation framework**
![screenshot from 2018-04-10 19-23-20](https://user-images.githubusercontent.com/33823698/39081130-94c32ab8-4559-11e8-8dbe-9208b4ed4cf3.png)

Collects all the logs from the server and parses it for making a common schema for all the logs and stores at given storage engine.


----------
## Prerequisites
* We expect users to follow [Best practices](https://github.com/deep-compute/logagg/issues/85) for logging their application
* Most importantly, do structured logging. Since, parsing/formatting logs is way easier that way.
----------
## Components/Architecture/Terminology

* `files` : Log files which are being tracked by logagg
* `node` : The server(s) where the log `files` reside
* `collector` : A program that runs on each `node` to collect and parse log lines in the `files`
* `formatters` : The parser function that the `collector` uses to format the log lines to put it the common format.

* `nsq` : The central location where logs are sent by `collector`(s) after formatting as messages.

* `forwarder` : The program that runs on the central node which receives messages from `nsq` and passes it on to `forwarders`
* `forwarders` : The parsers that take messages and formats it for storing at `target`(s) databases
* `targets` : The databases that store the logs finally so that we can query on them easily

![](https://i.imgur.com/tgY0zjp.jpg)

----------


## Features

* Guaranteed delivery of each log line from files to `targets`
* Reduced latency between a log being generated an being present in the `targets`
* Options to add custom `formatters` & `target` databases
* File poll if log file not yet generated
* Works on rotational log files
* Custom `formatters` to support parsing of any log file.
* Custom `forwarders` to support usage of any database/storage-engine file.
* Output format of processed log lines (dictionary)
    * `id` (str) - A unique id per log with time ordering. Useful to avoid storing duplicates.
    * `timestamp` (str) - ISO Format time. eg:
    * `data` (dict) - Parsed log data
    * `raw` (str) - Raw log line read from the log file
    * `host` (str) - Hostname of the node where this log was generated
    * `formatter` (str) - name of the formatter that processed the raw log line
    * `file` (str) - Full path of the file in the host where this log line came from
    * `type` (str) - One of "log", "metric" (Is there one more type?)
    * `level` (str) - Log level of the log line.
    * `event` (str) - LOG event
    * `error` (bool) - True if collection handler failed during processing
    * `error_tb` (str) - Error traceback


---

## Installation
> Prerequisites: Python2.7
### Setup

####  [Install](http://nsq.io/deployment/installing.html) the `nsq` package, at where we need to bring up the `nsq` server.
- Run the following commands to install `nsq`:
    ```BASH
    $ sudo apt-get install libsnappy-dev
    $ wget https://s3.amazonaws.com/bitly-downloads/nsq/nsq-1.0.0-compat.linux-amd64.go1.8.tar.gz
    $ tar zxvf nsq-1.0.0-compat.linux-amd64.go1.8.tar.gz
    $ sudo cp nsq-1.0.0-compat.linux-amd64.go1.8/bin/* /usr/local/bin
    ```

#### [Install](https://docs.docker.com/install/linux/docker-ce/ubuntu/#extra-steps-for-aufs) the Docker package, at both `forwarder` and `collector` nodes. (**If you will be using Docker image to run logagg**)
- Run the following commands to install :
    ```
    $ sudo apt-get update
    $ sudo apt-get install \
        apt-transport-https \
        ca-certificates \
        curl \
        software-properties-common
    $ curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
    $ sudo add-apt-repository \
       "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
       $(lsb_release -cs) \
       stable"
    $ sudo apt-get update
    $ sudo apt-get install docker-ce
    ```
- Check Docker version >= 17.12.1
    ```
    $ sudo docker -v
    Docker version 18.03.1-ce, build 9ee9f40
    ```

### Install the `logagg` package, at where we collect the logs and at where we forward the logs:
- Run the following command to **pip** install `logagg`: 
    ```
    $ sudo pip install https://github.com/deep-compute/pygtail/tarball/master/#egg=pygtail-0.6.1
    $ sudo pip install logagg
    ```
    #### or
- Run the following command to pull **docker** image of `logagg`:
    ```
    $ sudo docker pull deepcompute/logagg
    ```


---

## Basic Usage

### Bring up the `nsq` instances at the required server with following commands:
- **NOTE:** Run each command in a seperate Terminal window
- nsqlookupd
    ```
    $ nsqlookupd
    ```
- nsqd -lookupd-tcp-address **<ip-addr or DNS>**:4160
    ```
    $ nsqd -lookupd-tcp-address localhost:4160
    ```
- nsqadmin -lookupd-http-address **<ip-addr or DNS>**:4161
    ```
    $ nsqadmin -lookupd-http-address localhost:4161
    ```
### Collect logs from `node`
#### For collecting logs we need a process writing logs in a file
**NOTE:** Run each command in a seperate Terminal window

- We will use [serverstats](https://github.com/deep-compute/serverstats)
    - Install `serverstats` 
        ```BASH
        $ sudo pip install serverstats
        ```
    - Run `serverstats` to write logs in a `file` which will be tracked by `logagg collect`
        ```bash
        $ sudo serverstats --log-file /var/log/serverstats.log run
        2018-03-01T06:57:00.709472Z [info     ] system_metrics                 _={'ln': 113, 'file': '/usr/local/lib/python2.7/dist-packages/serverstats/serverstats.py', 'name': 'serverstats.serverstats', 'fn': '_log_system_metrics'} cpu={'avg_load_5_min': 15.0, 'avg_load_15_min': 0.11, 'idle_percent': 89.0, 'iowait': 34.34, 'avg_load_1_min': 23.0, 'usage_percent': 11.0} disk={'usage': 6000046080, 'total': 41083600896, 'free_percent': 80.25838230555476, 'usage_percent': 15.4, 'free': 32973033472} id=20180301T065700_bd9ad0bc1d1d11e8bcf1000c2925b24d network_traffic={'lo': {'received': 93836, 'sent': 93836}, 'docker0': {'received': 0, 'sent': 0}, 'ens33': {'received': 268122175, 'sent': 3999917}} ram={'avail': 724705280, 'usage_percent': 59.5, 'avail_percent': 40.49408598212978, 'usage': 883863552, 'total': 1789657088, 'free': 120479744} swap={'usage': 11022336, 'total': 1071640576, 'free_percent': 98.97145215972112, 'free': 1060618240, 'usage_percent': 1.0} type=metric
        2018-03-01T06:57:05.719910Z [info     ] system_metrics                 _={'ln': 113, 'file': '/usr/local/lib/python2.7/dist-packages/serverstats/serverstats.py', 'name': 'serverstats.serverstats', 'fn': '_log_system_metrics'} cpu={'avg_load_5_min': 15.0, 'avg_load_15_min': 0.11, 'idle_percent': 89.0, 'iowait': 34.34, 'avg_load_1_min': 21.0, 'usage_percent': 11.0} disk={'usage': 6000046080, 'total': 41083600896, 'free_percent': 80.25838230555476, 'usage_percent': 15.4, 'free': 32973033472} id=20180301T065705_c09761401d1d11e8bcf1000c2925b24d network_traffic={'lo': {'received': 93836, 'sent': 93836}, 'docker0': {'received': 0, 'sent': 0}, 'ens33': {'received': 268122175, 'sent': 3999917}} ram={'avail': 724721664, 'usage_percent': 59.5, 'avail_percent': 40.49500146477223, 'usage': 883859456, 'total': 1789657088, 'free': 120479744} swap={'usage': 11022336, 'total': 1071640576, 'free_percent': 98.97145215972112, 'free': 1060618240, 'usage_percent': 1.0} type=metric
        ```
#### Run `logagg collect` command
- Normal run
    ```bash
    $ sudo logagg collect --file file=/var/log/serverstats.log:formatter=logagg.formatters.basescript --nsqtopic logagg --nsqd-http-address localhost:4151
    
    2018-03-01T08:59:25.768443Z [info     ] Created topic                  _={'ln': 33, 'file': '/usr/local/lib/python2.7/dist-packages/logagg/nsqsender.py', 'name': 'logagg.nsqsender', 'fn': '_ensure_topic'} id=20180301T085925_d799dd6c-1d2e-11e8-bcf1-000c2925b24d topic=logagg type=log
    2018-03-01T08:59:25.771411Z [info     ] Created topic                  _={'ln': 33, 'file': '/usr/local/lib/python2.7/dist-packages/logagg/nsqsender.py', 'name': 'logagg.nsqsender', 'fn': '_ensure_topic'} id=20180301T085925_d799dd6d-1d2e-11e8-bcf1-000c2925b24d topic=Heartbeat#ephemeral type=log
    2018-03-01T08:59:25.772415Z [info     ] found_formatter_fn             _={'ln': 208, 'file': '/usr/local/lib/python2.7/dist-packages/logagg/collector.py', 'name': 'logagg.collector', 'fn': '_scan_fpatterns'} fn=logagg.formatters.basescript id=20180301T085925_d79a74c0-1d2e-11e8-bcf1-000c2925b24d type=log
    2018-03-01T08:59:25.772980Z [info     ] found_log_file                 _={'ln': 216, 'file': '/usr/local/lib/python2.7/dist-packages/logagg/collector.py', 'name': 'logagg.collector', 'fn': '_scan_fpatterns'} id=20180301T085925_d79a74c1-1d2e-11e8-bcf1-000c2925b24d log_file=/var/log/serverstats.log type=log
    2018-03-01T08:59:25.773873Z [info     ] Started collect_log_lines thread  _={'ln': 223, 'file': '/usr/local/lib/python2.7/dist-packages/logagg/collector.py', 'name': 'logagg.collector', 'fn': '_scan_fpatterns'} id=20180301T085925_d79a74c2-1d2e-11e8-bcf1-000c2925b24d log_key=('/var/log/serverstats.log', '/var/log/serverstats.log', 'logagg.formatters.basescript') type=log
    ```
    ##### or
- Docker run
    ```bash
    $ sudo docker run --name collector --hostname $HOSTNAME --volume /var/log:/var/log deepcompute/logagg logagg collect --file file=/var/log/serverstats.log:formatter=logagg.formatters.basescript --nsqtopic logagg --nsqd-http-address <nsq-server-ip-or-DNS>:4151
    ```
    - **Note**: Replace **<nsq-server-ip-or-DNS>** with the ip of `nsq` server eg.: **192.168.0.211**
    - **Note**: **--volume** argument is to mount local directory of log file into `Docker` `container`
    - **Note**: **--hostname** argument is to use the same hostname and not the docker container hostname
- You can check message traffic at `nsq` by going through the link:
        **http://<nsq-server-ip-or-DNS>:4171/** for **localhost** see [here](http://localhost:4171/)
- You can see the collected logs in realtime using the following command:
    ```bash
    $ nsq_tail --topic=logagg --channel=test --lookupd-http-address=<nsq-server-ip-or-DNS>:4161
    ```
### Forward logs to `target` database(s) from `nsq`
#### For forwarding logs we need a database instance up
- We will use `mongoDB`
    - Install [`mongoDB`](https://docs.mongodb.com/manual/tutorial/install-mongodb-on-linux/)
    - Start `mongoDB` 
        ```
        $ sudo mongod --dbpath <database-path> --bind_ip_all
        ```
    - Create user for `mongoDB` using the following commands:
        ```mongo
        $ mongo
        .
        .
        2018-03-01T03:47:54.027-0800 I CONTROL  [initandlisten] 
        > use admin
        > db.createUser(
        ...    {
        ...      user: "deadpool",
        ...      pwd: "chimichanga",
        ...      roles: [ "readWrite", "dbAdmin" ]
        ...    }
        ... )
        Successfully added user: { "user" : "deadpool", "roles" : [ "readWrite", "dbAdmin" ] }
        ```
#### Run `logagg forward` command
- Normal run
    ```
    $ logagg forward --nsqtopic logagg --nsqchannel test --nsqd-tcp-address localhost:4150 --target forwarder=logagg.forwarders.MongoDBForwarder:host=localhost:port=27017:user=deadpool:password=chimichanga:db=logs:collection=cluster_logs_and_metric
    ```
    #### or
- Docker run
    ```
    sudo docker run --name forwarder deepcompute/logagg logagg forward --nsqtopic logagg --nsqchannel test --nsqd-tcp-address <nsq-server-ip-or-DNS>:4150 --target forwarder=logagg.forwarders.MongoDBForwarder:host=<mongoDB-server-ip-or-DNS>:port=27017:user=deadpool:password=chimichanga:db=logs:collection=cluster_logs_and_metrics
    ```
    - **NOTE**: Replace **<nsq-server-ip-or-DNS>** with the ip of `nsq` server
    - **NOTE**: Replace **<mongoDB-server-ip-or-DNS>** with the ip of `mongoDB` server eg.: **192.168.0.111**
    - **NOTE**: **--volume** argument is to mount local directory of log file into eg.: **192.168.0.111**
- You can check records in mongoDB 
    ```mongo
    $ mongo -u deadpool -p chimichanga
    ....
    ....
    > show dbs
    admin   0.000GB
    config  0.000GB
    local   0.000GB
    logs    0.003GB
    > use logs
    switched to db logs
    > show collections
    cluster_logs_and_metrics
    > db.cluster_logs_and_metrics.count()
    5219
    > db.cluster_logs_and_metrics.findOne()

    ```
---
## Advanced Usage

### Help command
- For `logagg`
    ```bash
    $ logagg --help
    ```
    ##### or
    ```
    $ sudo docker run deepcompute/logagg logagg --help
    ```
    - you should see something like:
    ```
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
                            Otherwise print json.
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
    $ logagg collect --help
    ```
    ##### or
    ```
    $ sudo docker run deepcompute/logagg logagg collect -h
    ```

- For `logagg forward`,
    ```
    $ logagg forward --help
    ```
    ##### or
    ```
    $ sudo docker run deepcompute/logagg logagg forward -h
    ```
### Python interpreter
- After installation of the logagg module through **pip**, we can perform operations in the python shell.


```python
$ python
>>> import logagg
>>> dir(logagg)
['LogCollector', 'LogForwarder', 'NSQSender', '__builtins__', '__doc__', '__file__', '__name__', '__package__', '__path__', 'collector', 'command', 'formatters', 'forwarder', 'forwarders', 'main', 'nsqsender', 'util']

>>> dir(logagg.formatters)
['RawLog', '__builtins__', '__doc__', '__file__', '__name__', '__package__', 'basescript', 'convert_str2int', 'datetime', 'django', 'docker_log_file_driver', 'elasticsearch', 'json', 'mongodb', 'nginx_access', 're']

>>> from pprint import pprint
>>> mongo_line = '2017-08-17T07:56:33.489+0200 I REPL     \[signalProcessingThread\] shutting down replication subsystems'


>>> pprint(logagg.formatters.mongodb(mongo_line))
{'data': {'component': 'REPL',
          'context': '[signalProcessingThread]',
          'message': 'shutting down replication subsystems',
          'severity': 'I',
          'timestamp': '2017-08-17T07:56:33.489+0200'},
 'timestamp': '2017-08-17T07:56:33.489+0200'} 
```

### How to check records at MongoDB?
- Connect to the mongo shell and perform queries:
```mongodb
> use database_name
> db.collection_name.find({'formatter': 'logagg.formatters.<handler_name>'})
```
- You can see the basic format of record like below:
```json
{
  "_id" : "20180301T065838_f7e042841d1d11e8bcf1000c2925b24d",
  "level" : "info",
  "timestamp" : "isoformat_time. Ex: 2017-08-01T07:32:24.183981Z",
  "data" : {},
  "raw" : "raw_log_line",
  "host" : "x.com",
  "formatter" : "logagg.formatters.basescript",
  "event" : "default_event",
  "file" : "/path/to/log/file",
  "type" : "log | metric"
}
```

- Arbitrary example to get the records for `nginx`:
```mongodb  
> use nginx
> db.cluster_logs_and_metrics.find({'handler': 'logagg.handlers.nginx_access', 'data.request_time' : {$gt: 0}}).count()
751438
> db.cluster_logs_and_metrics.find({'handler': 'logagg.handlers.nginx_access', 'data.request_time' : {$gt: 60}}).count()
181

```
### How to check metrics at InfluxDB?
- For metrics, connect to the InfluxDB shell and perform queries.
```influxdb
> use database_name
> show measurements
> select <field_key> from <measurement_name>
```
- Arbitrary example to get the metrics for a `measurement`:
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
```
---

### Types of handlers we support
| Formatter-name | Comments |
| -------- | -------- |
|   nginx_access   | See Configuration [here](https://github.com/deep-compute/logagg/issues/61)    |
|mongodb||
|basescript||
|docker_log_file_driver|See example [here](https://github.com/deep-compute/serverstats/issues/6)|
### Types of forwarders we support
| Forwarder-name | Sample command |
| -------- | -------- |
|MongoDBForwarder|`--target forwarder=logagg.forwarders.MongoDBForwarder:host=<mongoDB-server-ip>:port=<mongod-port-number>:user=<user-name>:password=<passwd>:db=<db-name>:collection=<collection name>`|
|InfluxDBForwarder|`--target forwarder=logagg.forwarders.InfluxDBForwarder:host=<influxDB-server-ip>:port=<influxd-port-number>:user=<user-name>:password=<passwd>:db=<db-name>:collection=nothing`|

**Rules to follow when using InfluxDBForwarder:**
1. Only logs with `"type":"metric"` go to InfluxDB
2. key-value pairs where value is a string type becomes a `tag` in InfluxDB database and values having numerical values are considered as `fields`. See [link](https://docs.influxdata.com/influxdb/v1.5/concepts/glossary/#field-key) for more information on tags and fields
3. Naming a key starting with single '_' in the beggining will force them as fields even though they are strings.
4. Naming a key starting with double '__' in the beggining will not be forwarded to InfluxDB

**Note:** For using multiple forwarders use the format ``--target <forwarder1> <forwarder2>`` and not ``--target <forwarder1> --target <forwarder2>``

### How to create and use custom formatters for log files
#### Step 1: make a directory and append it's path to evironment variable $PYTHONPATH
```bash
$ echo $PYTHONPATH

$ mkdir customformatters
$ #Now append the path to $PYTHONPATH
$ export PYTHONPATH=$PYTHONPATH:/home/path/to/customformatters/

$ echo $PYTHONPATH
:/home/path/to/customformatters
```
#### Step 2: Create a another directory and put your formatter file(s) inside it.

```bash
$ cd customformatters/
$ mkdir myformatters
$ cd myformatters/
$ touch formatters.py
$ touch __init__.py
$ echo 'import formatters' >> __init__.py
$ #Now write your formatter functions inside the formatters.py file
```
#### Step 3: Write your formatter functions inside the formatters.py file

**Important:** 
1. Only **python standard modules** can be imported in formatters.py file
2. A formatter function should return a **dict()** `datatype`
3. The 'dict()' should only contain keys which are mentioned in the above [log structure](https://github.com/deep-compute/logagg#features).
4. Sample formatter functions:
    ```python
    import json 
    import re

    sample_log_line = '2018-02-07T06:37:00.297610Z [Some_event] [Info] [Hello_there]'

    def sample_formatter(log_line):
        log = re.sub('[\[+\]]', '',log_line).split(' ')
        timestamp = log[0]
        event = log[1]
        level = log[2]
        data = dict({'message': log[3]})

        return dict(timestamp = timestamp,
                     event = event,
                     level = level,
                     data = data,
                    )
     ```
  To see more examples, look [here](https://github.com/deep-compute/logagg/blob/master/logagg/formatters.py) 

5. Check if the custom handler works in `python interpreter` like for logagg.
    ```python
    >>> import myformatters
    >>> sample_log_line = '2018-02-07T06:37:00.297610Z [Some_event] [Info] [Hello_there]'
    >>> output = myformatters.formatters.sample_formatter(sample_log_line)
    >>> from pprint import pprint
    >>> pprint(output)
    {'data': {'message': 'Hello_there'},
     'event': 'Some_event',
     'level': 'Info',
     'timestamp': '2018-02-07T06:37:00.297610Z'}
    ```
6. Pseudo logagg collect commands:
    ```bash
    $ sudo logagg collect --file file=logfile.log:myformatters.formatters.sample_formatter --nsqtopic logagg --nsqd-http-address localhost:4151
    ```
    **or**
    docker run 
    ```bash
    $ sudo docker run --name collector --env PYTHONPATH=$PYTHONPATH --volume /var/log:/var/log deepcompute/logagg logagg collect --file file=logfile.log:myformatters.formatters.sample_formatter --nsqtopic logagg --nsqd-http-address <nsq-server-ip-or-DNS>:4151
    ```
---

### Debugging
You can store logagg collector/forwarder logs into files using [basescript](https://github.com/deep-compute/basescript) --log-file argument or [docker file log driver](https://github.com/supriyopaul/docker-file-log-driver)
```bash
$ sudo logagg --log-file /var/log/logagg/collector.log collect file=/var/log/serverstats/serverstats.log:formatter=logagg.formatters.basescript --nsqtopic logagg --nsqd-http-address <nsq-server-ip-or-DNS>:4151
```
**or**
docker run
    
```bash
$ sudo docker run --name collector --hostname $HOSTNAME --volume /var/log/:/var/log/ --restart unless-stopped --label formatter=logagg.formatters.basescript --log-driver file-log-driver --log-opt labels=formatter --log-opt fpath=/logagg/collector.log --log-opt max-size=100 deepcompute/logagg logagg collect --file file=/var/log/serverstats.log:formatter=logagg.formatters.basescript --nsqtopic serverstats --nsqd-http-address <nsq-server-ip-or-DNS>:4151
```
If there are multiple files being tracked by multiple collectors on multiple nodes, the collector information can be seen in "Heartbeat" topic of NSQ.
Every running collector sends a hearbeat to this topic (default interval = 30 seconds). The heartbeat format is as follows:
* `timestamp` : Timestamp of the recieved heartbeat.
* `heartbeat_number` : The heartbeat number since the collector started running.
* `host` : Hostname of the node on which the collector is running.
* `nsq_topic` : The nsq topic which the collector is using.
* `files_tracked` : list of files that are being tracked by the collector followed by the fomatter.

You can run the following command to see the information:
```bash
$ nsq_tail --topic=Heartbeat --channel=test --lookupd-http-address=<nsq-server-ip-or-DNS>:4161
```
## Build on logagg

You're more than welcome to hack on this:-)

```bash
$ git clone https://github.com/deep-compute/logagg
$ cd logagg
$ sudo python setup.py install
$ docker build -t logagg .
```
-------------------------------------------------------------------

