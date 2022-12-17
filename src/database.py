from __future__ import annotations

from typing import Optional, Mapping, Any
import json
import aiofiles
import asyncio
from queries import assemble_query
from results import *

__all__ = ["SimpleDB", "SimpleCollection"]

import bson

class CollectionNotFound(Exception):
    pass


class AlreadyExistingCollection(Exception):
    pass


class SimpleDB(object):
    def __init__(self, file_name: str, storage_type: str="bson", debug: bool=True):
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
        if self._loaded:
            return

        async with self._flock:
            async with aiofiles.open(self.file_name, mode=self.read_mode) as f:
                self._db = self.serialization_module.loads(await f.read())
                for k, v in self._db.items():
                    self._db[k] = SimpleCollection(k, v)
                    for doc in self._db[k]:
                        doc['_id'] = bson.ObjectId(doc['_id'])
                self._loaded = True

    async def save(self):
        async with self._flock:
            async with aiofiles.open(self.file_name, mode=self.write_mode) as f:
                await f.write(self.serialization_module.dumps(self._db))

    def sync_load(self):
        if self._loaded:
            return

        with open(self.file_name, mode=self.read_mode) as f:
            self._db = self.serialization_module.loads(f.read())
            for k, v in self._db.items():
                self._db[k] = SimpleCollection(k, v)
                for doc in self._db[k]:
                    doc['_id'] = bson.ObjectId(doc['_id'])
            self._loaded = True

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

    def delete_one(self, query):
        query = assemble_query(query)
        to_delete = None
        for i in self:
            if query(i):
                to_delete = i
                break
        if to_delete is not None:
            self.remove(to_delete)
            return DeleteResult(1)

        return DeleteResult(0)
    
    def delete_many(self, query):
        query = assemble_query(query)
        to_delete = filter(query, self)

        for i in to_delete:
            self.remove(i)

        return DeleteResult(len(to_delete))

    def find_one(self, query):
        query = assemble_query(query)
        return next(filter(query, self), None)

    def find_many(self, query):
        query = assemble_query(query)
        return filter(query, self)

    def insert_one(self, document):
        if not isinstance(document, dict):
            raise TypeError(f"document must be 'dict' got {type(document).__name__!r}")

        if document.get('_id') is None:
            document['_id'] = bson.ObjectId()

        self += [document]
        return InsertOneResult(document['_id'])
    
    def insert_many(self, documents):
        inserted_ids = []
        for doc in documents:
            if doc.get('_id') is None:
                doc['_id'] = bson.ObjectId()
                inserted_ids += [doc['_id']]
        self += documents
        return InsertManyResult(inserted_ids)

    def replace_one(self, query, document, upsert=False):
        query = assemble_query(query)
        to_replace = self.find_one(query)
        if to_replace is not None:
            self.remove(to_replace)
            self.insert_one(document)
            return UpdateResult(1, 1)
        elif upsert:
            insert_result = self.insert_one(document)
            return UpdateResult(0, 0, insert_result.inserted_id) # This may not be the same output, testing required
        else:
            return UpdateResult(0, 0)


def main():
    blah = SimpleDB("file.json")
    blah.sync_load()
    name = blah.name
    # blah.create_collection("name")
    # name.insert_one({"team": "A", "score": 24})
    # name.insert_many([{"team": "B", "score": 28}, {"team": "C", "score": 32}, {"team": "D", "score": 14}, {"team": "E", "score": 18}])
    # blah.sync_save()
    print([*name.find_many({"score": {"$gt": 20, "$lt": 30}})])
    print(name.count_documents({"score": {"$gt": 20}}))


