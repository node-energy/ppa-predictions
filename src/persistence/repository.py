import io
import pandas as pd
from abc import ABC, abstractmethod
from typing import Any, List, Type, TypeVar, Generic
from src.domain import model
from sqlalchemy.orm import Session
from src.persistence.sqlalchemy import UUIDBase as DBBase
from src.persistence.sqlalchemy import (
    Location as DBLocation,
    Component as DBComponent,
    HistoricLoadData as DBHistoricLoadData,
    Prediction as DBPrediction,
)


T = TypeVar('T')


class AbstractRepository(ABC, Generic[T]):
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


class GenericMemoryRepository(AbstractRepository, Generic[T]):
    def __init__(self, objs: dict[Any, T]):
        super().__init__()
        self._objs: dict[Any, T] = objs

    def _add(self, obj: T) -> T:
        if (id := obj.id) not in self._objs:
            self._objs[id] = obj
        return obj

    def _get(self, id: Any) -> T:
        return self._objs[id]

    def _get_all(self, **filters) -> List[T]:  # TODO CQRS?
        return list(self._objs.values())

    def _update(self, obj: T) -> T:
        id = obj.id
        self._objs[id] = obj
        return obj

    def _delete(self, id: Any) -> None:
        return self._objs.pop(id)


class GenericSqlAlchemyRepository(AbstractRepository, ABC, Generic[T]):
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
        o = self._session.merge(db_obj)
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


class LocationRepositoryBase(AbstractRepository[model.Location], ABC):
    pass


class LocationRepository(
    GenericSqlAlchemyRepository[model.Location],
    AbstractRepository[model.Location],  # LocationRepositoryBase
):
    def db_to_domain(self, db_obj: DBLocation) -> model.Location:
        def historic_load_data_to_domain(
            db_hld: DBHistoricLoadData,
        ) -> model.HistoricLoadData:
            if db_hld is None:
                return None
            f = io.BytesIO(db_hld.dataframe)
            return model.HistoricLoadData(id=db_hld.id, df=pd.read_pickle(f))

        def component_to_domain(db_component: DBComponent) -> model.Component:
            if db_component is None:
                return None
            if db_component.type == "consumer":
                return model.Consumer(
                    id=db_component.id,
                    malo=db_component.malo,
                    historic_load_data=historic_load_data_to_domain(
                        db_component.historic_load_data
                    ),
                )
            else:
                return model.Producer(
                    id=db_component.id,
                    malo=db_component.malo,
                    historic_load_data=historic_load_data_to_domain(
                        db_component.historic_load_data
                    ),
                )

        def prediction_to_domain(db_prediction: DBPrediction) -> model.Prediction:
            if db_prediction is None:
                return None
            f = io.BytesIO(db_prediction.dataframe)
            return model.Prediction(
                id=db_prediction.id, type=db_prediction.type, df=pd.read_pickle(f)
            )

        state = model.State(db_obj.state)
        return model.Location(
            id=db_obj.id,
            state=state,
            alias=db_obj.alias,
            residual_short=component_to_domain(db_obj.residual_short),
            residual_long=component_to_domain(db_obj.residual_long),
            producers=[component_to_domain(p) for p in db_obj.producers],
            predictions=[prediction_to_domain(p) for p in db_obj.predictions],
        )

    def domain_to_db(self, domain_obj: model.Location) -> DBLocation:
        def historic_load_data_to_db(hld: model.HistoricLoadData) -> DBHistoricLoadData:
            if hld is None:
                return None
            f = io.BytesIO()
            hld.df.to_pickle(f)
            f.seek(0)
            return DBHistoricLoadData(id=hld.id, dataframe=f.read())

        def component_to_db(component: model.Component) -> DBComponent:
            if component is None:
                return None
            type = "producer" if isinstance(component, model.Producer) else "consumer"
            return DBComponent(
                id=component.id,
                type=type,
                malo=component.malo,
                historic_load_data=historic_load_data_to_db(
                    component.historic_load_data
                ),
            )

        def prediction_to_db(prediction: model.Prediction) -> DBPrediction:
            if prediction is None:
                return None
            f = io.BytesIO()
            prediction.df.to_pickle(f)
            f.seek(0)
            return DBPrediction(
                id=prediction.id, type=prediction.type, dataframe=f.read()
            )

        residual_short_db = component_to_db(domain_obj.residual_short)
        residual_short_db.residual_short_location_id = domain_obj.id
        return DBLocation(
            id=domain_obj.id,
            state=domain_obj.state.value,
            alias=domain_obj.alias,
            residual_short=component_to_db(domain_obj.residual_short),
            residual_long=component_to_db(domain_obj.residual_long),
            producers=[component_to_db(p) for p in domain_obj.producers],
            predictions=[prediction_to_db(p) for p in domain_obj.predictions],
        )
