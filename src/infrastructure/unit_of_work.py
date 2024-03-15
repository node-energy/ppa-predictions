from __future__ import annotations
import abc
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session
from src.persistence import repository
from src.persistence.sqlalchemy import Project


class AbstractUnitOfWork(abc.ABC):
    projects: repository.AbstractRepository
    components: repository.AbstractRepository
    customers: repository.AbstractRepository
    companies: repository.AbstractRepository
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
        return None
        for obj in self.projects.seen:
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
        self.projects = repository.GenericMemoryRepository({})
        self.customers = repository.GenericMemoryRepository({})
        self.companies = repository.GenericMemoryRepository({})
        self.components = repository.GenericMemoryRepository({})
        self.historic_load_profiles = repository.HistoricLoadProfileMemoryRepository({})
        self.locations = repository.GenericMemoryRepository({})
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
                bind=create_engine("postgresql://predict:predict@localhost:5432/predict", isolation_level="REPEATABLE READ")
            )

        self.session_factory = session_factory

    def __enter__(self):
        self.session = self.session_factory()  # type: Session
        self.projects = repository.ProjectRepository(self.session, Project)
        return super().__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)
        self.session.close()

    def _commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()
