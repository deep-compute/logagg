from basescript import BaseScript
from collect.collector import LogCollector
from forward.forwarder import LogForwarder

class LogaggCommand(BaseScript):
    DESC = 'Logagg command line tool'

    def collect(self):
        collector = LogCollector(
                        self.log,
                        self.args,
                        self.args.file,
                        self.args.nsqtopic,
                        self.args.nsqd_http_address)
        collector.start()

    def forward(self):
        forwarder = LogForwarder(self.log,self.args,self.args.nsqtopic,\
                        self.args.nsqchannel,self.args.nsqd_tcp_address,\
                        self.args.mongodb_server_url,self.args.mongodb_port,\
                        self.args.mongodb_user_name,self.args.mongodb_password,\
                        self.args.mongodb_database,self.args.mongodb_collection)
        forwarder.start()

    def define_subcommands(self, subcommands):
        super(LogaggCommand, self).define_subcommands(subcommands)

        collect_cmd = subcommands.add_parser('collect',
            help='Collects the logs from different processes and sends to nsq')
        collect_cmd.set_defaults(func=self.collect)
        collect_cmd.add_argument('file', nargs='+',
            help='Provide absolute path of logfile including module name and function name, '
                    'eg: /var/log/nginx/access.log:logagg.collect.handlers.nginx_access')
        collect_cmd.add_argument('nsqtopic', help='Topic name to publish messages. Ex: logs_and_metrics')
        collect_cmd.add_argument('--nsqd-http-address',
            default='localhost:4151', help='nsqd http address where we send the messages')

        forward_cmd = subcommands.add_parser('forward', help='Collects all the messages from nsq and pushes to storage engine')
        forward_cmd.set_defaults(func=self.forward)
        forward_cmd.add_argument('--nsqtopic', help='NSQ topic name to read messages from. Ex: logs_and_metrics')
        forward_cmd.add_argument('--nsqchannel', help='channel of nsqd')
        forward_cmd.add_argument('--nsqd-tcp-address',
            default='localhost:4150', help='nsqd tcp address where we get the messages')

        forward_cmd.add_argument('--mongodb-server-url', help='DNS of the server where mongo is running')
        forward_cmd.add_argument('--mongodb-port', help='port where mongo is running')
        forward_cmd.add_argument('--mongodb-user-name', help='username of mongodb')
        forward_cmd.add_argument('--mongodb-password', help='password to authenticate mongodb')
        forward_cmd.add_argument('--mongodb-database', help='database to store logs')
        forward_cmd.add_argument('--mongodb-collection', help='collection to store logs')

def main():
    LogaggCommand().start()

if __name__ == '__main__':
    main()
