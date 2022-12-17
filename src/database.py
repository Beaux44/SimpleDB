from __future__ import annotations

from typing import Optional, Mapping, Any
import json
import aiofiles
import asyncio
from queries import assemble_query

__all__ = ["SimpleDB"]

try:
    import bson
except ModuleNotFoundError:
    pass

class CollectionNotFound(Exception):
    pass


class AlreadyExistingCollection(Exception):
    pass


class SimpleDB(object):
    def __init__(self, file_name: str, storage_type: str="json", debug: bool=True):
        if storage_type not in {"bson", "json"}:
            raise ValueError(f"storage_type must be 'json' or 'bson' got {storage_type!r}")

        self._flock = asyncio.Lock()
        self._loaded = False
        self._debug = debug
        self._db = {}
        self.file_name = file_name
        self.read_mode = {'json': 'r', 'bson': 'rb'}[storage_type]
        self.write_mode = {'json': 'w+', 'bson': 'wb+'}[storage_type]
        self.serialization_module = {'json': json, 'bson': bson}[storage_type]

    async def load(self):
        if loaded:
            return

        async with self._flock:
            async with aiofiles.open(self.file_name, mode=self.read_mode) as f:
                self._db = self.serialization_module.loads(await f.read())
                for k, v in self._db.items():
                    self._db[k] = SimpleCollection(k, v)
                loaded = True

    async def save(self):
        async with self._flock:
            async with aiofiles.open(self.file_name, mode=self.write_mode) as f:
                await f.write(self.serialization_module.dumps(self._db))

    def sync_load(self):
        if loaded:
            return

        with open(self.file_name, mode=self.read_mode) as f:
            self._db = self.serialization_module.loads(f.read())
            for k, v in self._db.items():
                self._db[k] = SimpleCollection(k, v)
            loaded = True

    def sync_save(self):
        with open(self.file_name, mode=self.write_mode) as f:
            f.write(self.serialization_module.dumps(self._db))

    def create_collection(self, name: str):
        if name in self._db:
            raise AlreadyExistingCollection(f"Cannot create collection {name!r}")

        self._db[name] = SimpleCollection(name)

    def list_collection_names(self):
        return [*self._db.values()]

    def __getattr__(self, attr: str) -> SimpleCollection:
        if attr in self._db:
            return self._db[attr]

        raise CollectionNotFound(f"Invalid collection {attr!r}")


class SimpleCollection(list):
    def __init__(self, name: str, _collection: Optional[list]=None):
        super().__init__(_collection or [])
        self.name = name

    def __repr__(self):
        return f"SimpleCollection({self.name!r})"

    def count_documents(self, query):
        query = assemble_query(query)
        return sum(query(i) for i in self)


async def main():
    db = SimpleDB("file.json")
        

if __name__ == '__main__':
    d = SimpleDocument({"team": "A", "score": 22})
    blah = SimpleDB("file.json")
    blah.create_collection("name")
    blah.name += [{"team": "A", "score": 24}, {"team": "B", "score": 28}, {"team": "C", "score": 32}, {"team": "D", "score": 14}, {"team": "E", "score": 18}]
    print(blah.name.count_documents({"score": {"$gt": 20}}))

