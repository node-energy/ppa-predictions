from __future__ import annotations
import abc
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session
from src.config import settings
from src.prognosis.persistence import repository
from src.prognosis.persistence.sqlalchemy import Location


class AbstractUnitOfWork(abc.ABC):
    historic_load_data: repository.AbstractRepository
    locations: repository.AbstractRepository[Location]

    def __enter__(self) -> AbstractUnitOfWork:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.rollback()

    def commit(self):
        self._commit()

    def collect_new_events(self):  # TODO better solution
        for obj in self.locations.seen:
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
        self.locations = repository.GenericMemoryRepository[Location]({})
        self.committed = False

    def _commit(self):
        self.committed = True

    def rollback(self):
        pass


# DEFAULT_SESSION_FACTORY = sessionmaker(
#     bind=create_engine("postgresql://localhost:5672", isolation_level="REPEATABLE READ")
# )

DEFAULT_SESSION_FACTORY = {}


class SqlAlchemyUnitOfWork(AbstractUnitOfWork):
    def __init__(self, session_factory=None):
        if session_factory is None:
            session_factory = sessionmaker(
                bind=create_engine(
                    settings.db_connection_string
                )  # , isolation_level="REPEATABLE READ")
            )

        self.session_factory = session_factory

    def __enter__(self):
        self.session = self.session_factory()  # type: Session
        self.locations = repository.LocationRepository(self.session, Location)
        return super().__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)
        self.session.close()

    def _commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()
