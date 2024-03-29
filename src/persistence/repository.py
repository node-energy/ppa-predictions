import abc


class AbstractRepository(abc.ABC):
    def __init__(self):
        self.seen = set()

    def add(self, obj):
        self._add(obj)
        self.seen.add(obj)

    def get(self, ref):
        obj = self._get(ref)
        if obj:
            self.seen.add(obj)
        return obj

    def get_all(self):
        objs = self._get_all()
        for obj in objs:
            self.seen.add(obj)
        return objs

    @abc.abstractmethod
    def _add(self, obj):
        raise NotImplementedError

    @abc.abstractmethod
    def _get(self, ref):
        raise NotImplementedError

    @abc.abstractmethod
    def _get_all(self):
        raise NotImplementedError


class MemoryRepository(AbstractRepository):
    def __init__(self, objs):
        super().__init__()
        self._objs = set(objs)

    def _add(self, obj):
        self._objs.add(obj)

    def _get(self, ref):
        return next((o for o in self._objs if str(o.ref) == ref), None)

    def _get_all(self):  # TODO CQRS?
        return self._objs


class AbstractHistoriyLoadProfileRepository(AbstractRepository):
    def get_by_component_ref(self, component_ref):
        historic_load_profile = self._get_by_component_ref(component_ref)
        if historic_load_profile:
            self.seen.add(historic_load_profile)
        return historic_load_profile

    def _get_by_component_ref(self, component_ref):
        raise NotImplementedError


class HistoricLoadProfileMemoryRepository(AbstractHistoriyLoadProfileRepository, MemoryRepository):
    def _get_by_component_ref(self, component_ref):
        return next(h for h in self._objs if str(h.component.ref) == component_ref)
