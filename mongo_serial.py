# Copyright 2021 Matheus Xavier
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0


from bson import ObjectId
from pymongo import MongoClient


class CounterNotFoundException(Exception):
    pass


class MongoSequence:
    def __init__(self, mongo_client: MongoClient, db: str, counter_name: str, colle: str = "counters",
                 allow_create: bool = False, interval: int = 1, start: int = 1):
        self.db = mongo_client[db]
        self.colle = self.db[colle]
        count_doc = self.colle.find_one({"counter_name": counter_name})
        if not count_doc and allow_create:
            count_doc = self.colle.insert_one({
                "counter_name": counter_name,
                "interval": interval,
                "sequence_value": start
            }).inserted_id
            count_doc = self.colle.find_one({"_id": count_doc})
        elif not count_doc:
            raise CounterNotFoundException("Consider allowing the creation of the counter")

        # speed up by caching the id
        self.counter_id = ObjectId(count_doc["_id"])

    def __next__(self):
        curr = self.colle.find_one({"_id": self.counter_id})
        ret = curr["sequence_value"]
        curr["sequence_value"] += curr["interval"]
        self.colle.replace_one({"_id": self.counter_id}, curr)
        return ret

    def next(self):
        return self.__next__()

    def alter_counter(self, value: int, interval: int = 1):
        curr = self.colle.find_one({"_id": self.counter_id})
        ret = curr["sequence_value"]
        curr["sequence_value"] = value
        curr["interval"] = interval
        self.colle.replace_one({"_id": self.counter_id}, curr)
        return ret