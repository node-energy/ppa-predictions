import abc


class AbstractDataStore(abc.ABC):
    def save_file(self, file):
        raise NotImplementedError()


class EmailDataStore(AbstractDataStore):
    def save_file(self, file):
        pass
