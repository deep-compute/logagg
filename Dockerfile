FROM ubuntu:16.04

RUN apt-get -y update

WORKDIR /logagg

ADD . /logagg

RUN apt-get install python-pip -y

RUN pip install .

RUN easy_install https://github.com/deep-compute/pygtail/tarball/master/#egg=pygtail-0.6.1

CMD ["logagg", "--log-level", "INFO collect", "~/Desktop/log_samples/access_new.log:logagg.handlers.nginx_access", "~/Desktop/log_samples/basescript_new.log:logagg.handlers.basescript", "~/Desktop/log_samples/not_a_file.log:logagg.handlers.django", "logs_and_metrics", "--nsqchannel", "test_channel", "--heartbeat-interval", "30", "--nsqd-http-address", "192.168.0.58:4151"]
