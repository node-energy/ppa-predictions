from __future__ import annotations
import abc
from src.persistence import repository


class AbstractUnitOfWork(abc.ABC):
    components: repository.AbstractRepository
    customers: repository.AbstractRepository
    historic_load_profiles: repository.AbstractHistoriyLoadProfileRepository
    locations: repository.AbstractRepository
    predictions: repository.AbstractRepository

    def __enter__(self) -> AbstractUnitOfWork:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.rollback()

    def commit(self):
        self._commit()

    def collect_new_events(self):  # TODO better solution
        for obj in self.customers.seen:
            while obj.events:
                yield obj.events.pop(0)

    @abc.abstractmethod
    def _commit(self):
        raise NotImplementedError

    @abc.abstractmethod
    def rollback(self):
        raise NotImplementedError


class MemoryUnitOfWork(AbstractUnitOfWork):
    def __init__(self):
        self.customers = repository.MemoryRepository([])
        self.components = repository.MemoryRepository([])
        self.historic_load_profiles = repository.HistoricLoadProfileMemoryRepository([])
        self.locations = repository.MemoryRepository([])
        self.committed = False

    def _commit(self):
        self.committed = True

    def rollback(self):
        pass
