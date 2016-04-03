import abc
import yaml


class DbConn(object):
    __metaclass__ = abc.ABCMeta

    @classmethod
    def load(cls, path):
        data = yaml.load(open(path))
        return cls(
            host=data.get('host'),
            port=data.get('port'),
            dbname=data.get('dbname'),
            user=data.get('user'),
            password=data.get('password'))

    @abc.abstractmethod
    def connect(self, input):  # pragma: no cover
        return
