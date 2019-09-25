import os, re, errno, time, logging
from urllib.parse import urljoin
from urllib.parse import urlsplit
from urllib.parse import urlparse
from os.path import splitext
from string import Template

class TemplateUtil:

    @classmethod
    def get_variable_key_from_template(cls, template_string):
        return [s[1] or s[2] for s in Template.pattern.findall(template_string.template) if s[1] or s[2]]


class PathUtils:

    def __init__(self):
        self.logger = logging.getLogger('FastProducer')

    def ensure_has_paths(self, new_path, is_filename=True):
        """
        Private method - Ensure that the path in the config file exits or create them.
        :return:
        """
        if is_filename:
            if not os.path.exists(os.path.dirname(new_path)):
                try:
                    if self.logger is not None:
                        self.logger.info(u'Creating path {0}'.format(new_path))
                    os.makedirs(os.path.dirname(new_path))
                except OSError as exception:  # Guard against race condition
                    if exception.errno != errno.EEXIST:
                        raise exception
        else:
            if not os.path.isdir(new_path):
                if self.logger is not None:
                    self.logger.info(u'Creating path {0}'.format(new_path))
                os.makedirs(new_path)

    def touch(self, fname):
        try:
            open(fname, 'a').close()
        except Exception as e:
            self.logger.error(e)
