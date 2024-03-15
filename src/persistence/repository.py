from abc import ABC, abstractmethod
from typing import Any, List, Type
from src.domain import model
from sqlalchemy.orm import Session
from src.persistence.sqlalchemy import Project as DBProject, Base as DBBase


class AbstractRepository[T](ABC):
    def __init__(self):
        self.seen = set()

    def add(self, obj: T) -> T:
        self.seen.add(obj)
        return self._add(obj)

    def get(self, id: Any) -> T:
        obj = self._get(id)
        if obj:
            self.seen.add(obj)
        return obj

    def get_all(self, **filters) -> List[T]:
        objs = self._get_all()
        for obj in objs:
            self.seen.add(obj)
        return objs

    def update(self, obj: T) -> T:
        self.seen.add(obj)
        return self._update(obj)

    def delete(self, id: Any) -> None:
        self._delete(id)

    @abstractmethod
    def _add(self, obj: T) -> T:
        raise NotImplementedError

    @abstractmethod
    def _get(self, id: Any) -> T:
        raise NotImplementedError

    @abstractmethod
    def _get_all(self, **filters) -> List[T]:
        raise NotImplementedError

    @abstractmethod
    def _update(self, obj: T) -> T:
        raise NotImplementedError

    @abstractmethod
    def _delete(self, id: Any) -> None:
        raise NotImplementedError


class GenericMemoryRepository[T](AbstractRepository):
    def __init__(self, objs: dict[Any, T]):
        super().__init__()
        self._objs: dict[Any, T] = objs

    def _add(self, obj: T) -> T:
        if (id := str(obj.id)) not in self._objs:
            self._objs[id] = obj
        return obj

    def _get(self, id: Any) -> T:
        return self._objs[id]

    def _get_all(self, **filters) -> List[T]:  # TODO CQRS?
        return list(self._objs.values())

    def _update(self, obj: T) -> T:
        id = str(obj.id)
        self._objs[id] = obj
        return obj

    def _delete(self, id: Any) -> None:
        return self._objs.pop(id)


class GenericSqlAlchemyRepository[T](AbstractRepository, ABC):
    def __init__(self, session: Session, db_cls: Type[DBBase]) -> None:
        super().__init__()
        self._session = session
        self._db_cls = db_cls

    def _add(self, obj: T) -> T:
        db_obj = self.domain_to_db(obj)
        self._session.add(db_obj)
        self._session.flush()
        self._session.refresh(db_obj)
        return self.db_to_domain(db_obj)

    def _get(self, id: Any) -> T:
        db_obj = self._session.query(self._db_cls).filter_by(id=id).first()
        if db_obj is not None:
            return self.db_to_domain(db_obj)
        return None

    def _get_all(self, **filters) -> List[T]:
        results = []
        for db_obj in self._session.query(self._db_cls).all():
            results.append(self.db_to_domain(db_obj))
        return results

    def _update(self, obj: T) -> T:
        db_obj = self.domain_to_db(obj)
        o = self._session.merge(db_obj, load=True)
        self._session.flush()
        self._session.refresh(o)
        return self.db_to_domain(o)

    def _delete(self, id: Any) -> None:
        obj = self._get(id)
        if obj is not None:
            self._session.delete(obj)
            self._session.flush()

    @abstractmethod
    def db_to_domain(self, db_obj: DBBase) -> T:
        raise NotImplementedError

    @abstractmethod
    def domain_to_db(self, domain_obj: T) -> DBBase:
        raise NotImplementedError


class ProjectRepositoryBase(AbstractRepository[model.Project], ABC):
    pass


class ProjectRepository(
    GenericSqlAlchemyRepository[model.Project], ProjectRepositoryBase
):
    def db_to_domain(self, db_obj: DBProject) -> model.Project:
        return model.Project(id=db_obj.id, name=db_obj.name)

    def domain_to_db(self, domain_obj: model.Project) -> DBProject:
        return DBProject(id=domain_obj.id, name=domain_obj.name)


class AbstractHistoriyLoadProfileRepository(AbstractRepository):
    def get_by_component_ref(self, component_ref):
        historic_load_profile = self._get_by_component_ref(component_ref)
        if historic_load_profile:
            self.seen.add(historic_load_profile)
        return historic_load_profile

    def _get_by_component_ref(self, component_ref):
        raise NotImplementedError


class HistoricLoadProfileMemoryRepository(
    AbstractHistoriyLoadProfileRepository, GenericMemoryRepository
):
    def _get_by_component_ref(self, component_ref):
        return next(
            h for h in self._objs.values() if str(h.component.id) == component_ref
        )
