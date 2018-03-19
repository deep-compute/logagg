from nsq.reader import Reader
from basescript import BaseScript
from logagg.collector import LogCollector
from logagg.forwarder import LogForwarder
from logagg.nsqsender import NSQSender
from logagg import util

class LogaggCommand(BaseScript):
    DESC = 'Logagg command line tool'

    def collect(self):
        nsq_sender = NSQSender(self.args.nsqd_http_address,
                               self.args.nsqtopic,
                               self.args.depth_limit_at_nsq,
                               self.log)
        collector = LogCollector(
            self.args.file,
            nsq_sender,
            self.args.heartbeat_interval,
            self.log)
        collector.start()

    def _parse_forwarder_target_arg(self, t):
        path, args = t.split(':', 1)
        path = path.split('=')[1]
        args = dict(a.split('=', 1) for a in args.split(':'))
        args['log'] = self.log
        return path, args

    def forward(self):
        targets = []
        for t in self.args.target:
            imp_path, args = self._parse_forwarder_target_arg(t)
            target_class = util.load_object(imp_path)
            target_obj = target_class(**args)
            targets.append(target_obj)

        nsq_receiver = Reader(self.args.nsqtopic,
                              self.args.nsqchannel,
                              nsqd_tcp_addresses=[self.args.nsqd_tcp_address])

        forwarder = LogForwarder(nsq_receiver,
                                 targets,
                                 self.log)
        forwarder.start()

    def define_subcommands(self, subcommands):
        super(LogaggCommand, self).define_subcommands(subcommands)

        collect_cmd = subcommands.add_parser('collect',
                                             help='Collects the logs from \
                                             different processes and sends to nsq')
        collect_cmd.set_defaults(func=self.collect)
        collect_cmd.add_argument('--file', nargs='+',
                                 help='Provide absolute path of logfile \
                                 including module name and function name,'
                                 'format: file=<filename>:formatter=<formatter function>,'
                                 'eg: file=/var/log/nginx/access.log:formatter=logagg.formatters.nginx_access')
        collect_cmd.add_argument('--nsqtopic',
                                 default='test_topic',
                                 help='Topic name to publish messages. Ex: logs_and_metrics')
        collect_cmd.add_argument('--nsqd-http-address',
                                 default='localhost:4151',
                                 help='nsqd http address where we send the messages')
        collect_cmd.add_argument('--depth-limit-at-nsq', type=int,
                                 default=10000000,
                                 help='To limit the depth at nsq topic')
        collect_cmd.add_argument('--heartbeat-interval',
                                 type=int, default=30,
                                 help='Time interval at which regular heartbeats to a nsqTopic "heartbeat" to know which hosts are running logagg')

        forward_cmd = subcommands.add_parser(
            'forward',
            help='Collects all the messages from nsq and pushes to storage engine')
        forward_cmd.set_defaults(func=self.forward)
        forward_cmd.add_argument(
            '--nsqtopic',
            help='NSQ topic name to read messages from. Ex: logs_and_metrics')
        forward_cmd.add_argument(
            '--nsqchannel',
            help='NSQ channel name to read messages from. Ex: logs_and_metrics')
        forward_cmd.add_argument('--nsqd-tcp-address',
                                 default='localhost:4150', help='nsqd tcp address where we get the messages')
        forward_cmd.add_argument('-t', '--target', nargs= '+',
                                 help='Target database and database details,'
                                 'format: "forwarder=<forwarder-classpath>:host=<hostname>:port=<port-number>:user=<user-name>:password=<password>:db=<database-name>:collection=<collection-name>",'
                                 'Ex: forwarder=logagg.forwarders.MongoDBForwarder:host=localhost:port=27017:user=some_user:password=xxxxx:db=logagg:collection=cluster_logs_and_metrics')

def main():
    LogaggCommand().start()


if __name__ == '__main__':
    main()
