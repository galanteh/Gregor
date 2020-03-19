import os, sys, logging, errno, datetime, argparse, time, faker, inspect
from utils import PathUtils
from logging import handlers
from configparser import ConfigParser
from time import sleep
from KafkaTask import KafkaTask

__version__ = '0.0.2'

class Gregor:

    logger = None

    def __init__(self):
        self.tasks = []
        self.log_console = True
        self.read_configuration('gregor.cfg')
        self.logger.info('Welcome to Gregor, a program that produces fake data into Apache Kafka for demo purposes.')

    @classmethod
    def get_fake_providers(cls):
        """Get a list of all available provider methods."""
        fake = faker.Factory.create()
        fakes = []
        providers_list = fake.get_providers()
        for provider in providers_list:
            provider_list = inspect.getmembers(provider, predicate=inspect.ismethod)
            for method_name, method_value in provider_list:
                if not method_name.startswith('_'):
                    fakes.append(method_name)
        return fakes

    def get_config_logger(self, logfile, level=logging.INFO, max_bytes=5242880, log_console=False):
        """
        Setup the logger of the receiver.
        :param logfile: filename of the log
        :param level: level of verbose
        :param max_bytes: max bytes of the log file
        :param log_console: if the log should also printed on the console
        :return:
        """
        PathUtils().ensure_has_paths(logfile, is_filename=True)
        self.logger = logging.getLogger('GREGOR')
        formatter = logging.Formatter(u'%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger.setLevel(level)
        # Console
        if log_console:
            ch = logging.StreamHandler(sys.stdout)
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)
        # File
        fh = handlers.RotatingFileHandler(logfile, maxBytes=max_bytes, backupCount=100, encoding='utf8')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

    def read_configuration(self, cfg_file):
        """
        Read the configuration from the argument file
        :param cfgfile: filename
        :return: None
        """
        try:
            path_utils = PathUtils()
            config = ConfigParser()
            confirmation = config.read(cfg_file)
            if len(confirmation) == 0:
                sys.exit(u'Configuration file not found. Please execute with the configuration file next to the executable file.')
            _logfile = config.has_option('Logs','Filename') and config.get('Logs','Filename') or './log/application.log'
            _logfile_max_bytes = int(config.has_option('Logs','MaxBytes') and config.get('Logs','MaxBytes') or 5242880)
            log_level = config.has_option('Logs','Level') and config.get('Logs','Level') or 'INFO'
            log_console_option = config.has_option('Logs','Console') and config.get('Logs','Console') or 'FALSE'
            self.log_console = log_console_option.strip().upper() == 'TRUE'
            if log_level == 'INFO':
                log_level = logging.INFO
            if log_level == 'DEBUG':
                log_level = logging.DEBUG
            if log_level == 'WARNING':
                log_level = logging.WARNING
            if log_level == 'CRITICAL':
                log_level = logging.CRITICAL
            if log_level == 'ERROR':
                log_level = logging.ERROR
            if log_level == 'FATAL':
                log_level = logging.FATAL
            self.get_config_logger(_logfile, log_level, _logfile_max_bytes, self.log_console)
            num_task = int(config.has_option('Task', 'Number') and config.get('Task', 'Number') or 0)
            for index in range(0, num_task):
                try:
                    name = config.has_option('Task', str(index)) and config.get('Task', str(index)) or None
                    if name is not None:
                        brokers_str = config.has_option(name, 'Brokers') and config.get(name, 'Brokers') or ''
                        brokers = brokers_str.split(',')
                        topic = config.has_option(name, 'Topic') and config.get(name, 'Topic') or ''
                        retries = int(config.has_option(name, 'Retries') and config.get(name, 'Retries') or 0)
                        partitions = config.has_option(name, 'Partitions') and config.get(name, 'Partitions') or [0]
                        key = config.has_option(name, 'Key') and config.get(name, 'Key') or ''
                        value = config.has_option(name, 'Value') and config.get(name, 'Value') or ''
                        locale = config.has_option(name, 'Locale') and config.get(name, 'Locale') or 'En_US'
                        pause_milliseconds = config.has_option(name, 'Pause_milliseconds') and config.get(name, 'Pause_milliseconds') or 0
                        kafka_task = KafkaTask(name, locale, brokers, topic, retries, partitions, key, value, pause_milliseconds)
                        self.tasks.append(kafka_task)
                except Exception as e:
                    self.logger.error(u'Error found at Kafka task listed at {0} on the configuration file!'.format(index))
                    pass
        except Exception as e:
            if self.logger is not None:
                self.logger.error(u'ReadConfiguration Exception {0}.'.format(e))
            sys.exit(u'ReadConfiguration Exception {0}.'.format(e))

    def get_sample_data(self):
        len_tasks = len(self.tasks)
        if len_tasks > 1:
            self.logger.info(u'Getting sample data from {0} tasks'.format(len_tasks))
        else:
            self.logger.info(u'Getting sample data from {0} task'.format(len_tasks))
        for task in self.tasks:
            message = u'{0}: {1} - {2}'.format(task.name, task.get_key(), task.get_value())
            if not self.log_console:
                print(message)
            else:
                self.logger.info(message)

    def start(self):
        len_tasks = len(self.tasks)
        if len_tasks > 1:
            self.logger.info(u'Starting {0} tasks'.format(len_tasks))
        else:
            self.logger.info(u'Starting {0} task'.format(len_tasks))
        for task in self.tasks:
            task.start()
        self.logger.info(u'If you want to exit, just break the loop '
                         u'with Control-C or just kill the program')

if __name__ == "__main__":
    description = 'Gregor is a program that produces fake data into Apache Kafka for demo purposes.'
    prog = 'gregor.exe'
    epilog = 'Developed by Hernan J. Galante <hernan_galante@hotmail.com>. License under Apache License 2.0'
    parser = argparse.ArgumentParser(description=description, prog=prog, epilog=epilog)
    parser.add_argument('--version', action='version', version='%(prog)s {version}'.format(version=__version__))
    parser.add_argument('-l', '--list-fakes', dest='list_fakes', action='store_true',
                        help='Show a list of all available fake generator properties.')
    parser.add_argument('-d', '--show-data', dest='show_data', action='store_true',
                        help='Show sample of data that should go to Kafka without send it. This option is for debbuging purposes.')
    parser.add_argument('-s', '--start', dest='start', action='store_true',
                        help='Start pushing fake data into the Kafka topic set in the configuration file.')
    args = parser.parse_args()
    n_args = sum([1 for a in vars(args).values() if a])
    if n_args == 0:
        parser.print_help()
        sys.exit(0)
    if args.list_fakes:
        fakes = Gregor.get_fake_providers()
        print(" \n".join(sorted(list(set(fakes)))))
        sys.exit(0)
    if args.show_data:
        producer = Gregor()
        producer.get_sample_data()
        sys.exit(0)
    if args.start:
        producer = Gregor()
        try:
            while True:
                producer.start()
                time.sleep(5)
        except KeyboardInterrupt:
            sys.exit(0)

