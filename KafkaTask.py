import logging, faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from json import dumps
from string import Template
from utils import TemplateUtil

class KafkaTask:

    def __init__(self,
                 name=None,
                 locale='En_US',
                 brokers=None,
                 topic='',
                 retries=1,
                 partitions=None,
                 key=None,
                 value=None,
                 pause_milliseconds=5):
        self.logger = logging.getLogger('GREGOR')
        self._producer = None
        self._faker = None
        self.name = name
        self.locale = locale
        if brokers is None:
            self.brokers = ['localhost:9092']
        else:
            self.brokers = brokers
        self.topic = topic
        self.retries = retries
        if partitions is None:
            self.partitions = [0]
        else:
            self.partitions = partitions
        self.key = key
        self.value = value
        self.pause_milliseconds = pause_milliseconds

    def get_producer(self):
        if self._producer is None:
            self._producer = KafkaProducer(bootstrap_servers=self.brokers, retries=self.retries)
            return self._producer
        else:
            return self._producer

    def get_faker(self):
        if self._faker is None:
            self._faker = faker.Factory.create(self.locale)
        return self._faker

    def get_substitute_on_template(self, template_string):
        d = {}
        keys = TemplateUtil.get_variable_key_from_template(template_string)
        for provider_name in keys:
            d[provider_name] = self.get_faker_data(provider_name)
        return template_string.safe_substitute(d)

    def get_key(self):
        return self.get_substitute_on_template(Template(self.key))

    def get_value(self):
        return self.get_substitute_on_template(Template(self.value))

    def get_faker_data(self, provider_name):
        faker = self.get_faker()
        method = None
        try:
            method = getattr(faker, provider_name)
        except AttributeError:
            self.logger.error(u'Faker `{}` does not implement `{}`'.format(faker.__class__.__name__, provider_name))
        return method()

    def start(self):
        try:
            kp = self.get_producer()
            message_key = self.get_key()
            message_value = self.get_value()
            self.logger.info(u'Sending Key: {0} Value: {1} to {2} on topic {3}'.format(message_key,
                                                                                       message_value,
                                                                                       self.brokers,

                                                                                       self.topic))
            # value_serializer=lambda m: dumps(m).encode('utf-8')
            ack = kp.send(self.topic, key=dumps(message_key).encode('utf-8'), value=dumps(message_value).encode('utf-8') )
            metadata = ack.get()
            self.logger.info(u'Topic: {0} Partition: {1}'.format(metadata.topic,metadata.partition))
        except NoBrokersAvailable as e:
            self.logger.error(u'Brokers {0} are not reachable or available. Please check connectivity'.format(
                self.brokers))
        except Exception as e:
            self.logger.error(u'Error has been raised on the start of task {0}'.format(self.name))
