import abc


class AbstractLoadDataRetriever(abc.ABC):
    def get_data(self, malo: str):
        raise NotImplementedError()


class APILoadDataRetriever(AbstractLoadDataRetriever):
    def get_data(self, malo: str):
        pass


class OptinodeDataRetriever(AbstractLoadDataRetriever):
    def get_data(self, malo: str):
        pass
