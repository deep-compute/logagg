FROM ubuntu:16.04

RUN apt-get -y update

WORKDIR /logagg
ADD . /logagg

RUN apt-get update
RUN apt-get install python-pip -y
RUN pip install .

RUN easy_install https://github.com/deep-compute/pygtail/tarball/master/#egg=pygtail-0.6.1

VOLUME ["/var/log"]

