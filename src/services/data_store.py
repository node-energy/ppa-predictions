import abc


class AbstractDataStore(abc.ABC):
    def save_file(self, file_name, buffer):
        raise NotImplementedError()


class EmailDataStore(AbstractDataStore):
    def save_file(self, file_name, buffer):
        pass


class LocalDataStore(AbstractDataStore):
    def save_file(self, file_name, buffer):
        with open(f"{file_name}.csv", "wb") as f:
            f.write(buffer)
