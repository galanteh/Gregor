import logging, faker, json
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from string import Template
from utils import TemplateUtil
from random import randint

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
            client_id = 'Gregor-Producer-{0}'.format(randint(0, 10))
            if self.name is not None:
                client_id = self.name

            self._producer = KafkaProducer(client_id=client_id, bootstrap_servers=self.brokers, retries=self.retries)
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
        json_string = self.get_substitute_on_template(Template(self.key))
        try:
            json = json.loads(json_string) 
        except Exception as e:
            json = {}
            json['key'] = json_string
        return json

    def get_value(self):
        json_string = self.get_substitute_on_template(Template(self.value))
        return json.loads(json_string)

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
            key = json.dumps(message_key).encode('utf-8')
            value = json.dumps(message_value).encode('utf-8')
            ack = kp.send(self.topic, key=key, value=value )
            metadata = ack.get()
            self.logger.info(u'Topic: {0} Partition: {1}'.format(metadata.topic,metadata.partition))
        except NoBrokersAvailable as e:
            self.logger.error(u'Brokers {0} are not reachable or available. Please check connectivity'.format(
                self.brokers))
        except Exception as e:
            self.logger.error(u'Error has been raised on the start of task {0}'.format(self.name))
            self.logger.error(u'Error: {0}'.format(e))

