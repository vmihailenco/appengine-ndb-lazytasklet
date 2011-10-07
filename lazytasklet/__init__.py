from functools import wraps

from ndb import tasklets, model, query


class AutoKeyProperty(model.KeyProperty):
    def _comparison(self, op, value):
        if value is not None:
            value = self._validate(value)
        return query.FilterNode(
            self._name, op, self._datastore_type(value.key))

    def _validate(self, value):
        if isinstance(value, KeyFutureValueHolder):
            return value
        elif isinstance(value, model.Key):
            return KeyFutureValueHolder(value)
        elif isinstance(value, model.Model):
            return KeyFutureValueHolder(value)
        else:
            assert False

    def _db_set_value(self, v, unused_p, value):
        super(AutoKeyProperty, self)._db_set_value(v, unused_p, value.key)


class KeyFutureValueHolder(object):
    _key = None
    _future = None
    _value = None

    def __init__(self, value):
        if isinstance(value, model.Key):
            self.key = value
        elif isinstance(value, tasklets.Future):
            self._future = value
        else:
            self.value = value

    def _get_key(self):
        if not self._key and self.value:
            if isinstance(self.value, list):
                self._key = [v.key for v in self.value]
            else:
                self._key = self.value.key
        return self._key

    def _set_key(self, value):
        self._key = value
        self._future = None
        self._value = None

    key = property(_get_key, _set_key)

    def _get_value(self):
        if not self._value and self.future:
            if isinstance(self.future, list):
                self.value = [f.get_result() for f in self.future]
            else:
                self.value = self.future.get_result()
        return self._value

    def _set_value(self, value):
        self._value = value
        self._key = None
        self._future = None

    value = property(_get_value, _set_value)

    @property
    def future(self):
        if not self._future and self._key:
            if isinstance(self.key, list):
                self._future = model.get_multi_async(self.key)
            else:
                self._future = self.key.get_async()
        return self._future


def lazytasklet(func):
    def _create_kfv_holder(value):
        if isinstance(value, KeyFutureValueHolder):
            return value
        else:
            return KeyFutureValueHolder(value)

    @tasklets.tasklet
    @wraps(func)
    def inner(*args, **kwargs):
        gen = func(*args, **kwargs)
        futures = []
        res = None
        try:
            value = gen.send(None)
            while True:
                if isinstance(value, list):
                    holders = [_create_kfv_holder(v) for v in value]
                    futures.append([h.future for h in holders])
                    value = gen.send(holders)
                else:
                    holder = _create_kfv_holder(value)
                    futures.append(holder.future)
                    value = gen.send(holder)
        except tasklets.Return, res:
            pass
        yield futures

        if res:
            raise res
    return inner
