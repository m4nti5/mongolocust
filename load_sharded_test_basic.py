from locust import between

from mongo_user import MongoUser, mongodb_task
from settings import DEFAULTS
from pymongo.write_concern import WriteConcern
from pprint import pprint

import pymongo
import random

# number of cache entries for queries
IDS_TO_CACHE = 10000
migrating = False

class MongoSampleUser(MongoUser):
    """
    Generic sample mongodb workload generator
    """
    # no delays between operations
    wait_time = between(0.0, 0.0)

    def __init__(self, environment):
        super().__init__(environment)
        self.cached_ids = []
        self.migrating = False

    def generate_new_document(self):
        """
        Generate a new sample document
        """
        document = {
            'id': random.randint(-1000000, 1000000),
            'first_name': self.faker.first_name(),
            'last_name': self.faker.last_name(),
            'address': self.faker.street_address(),
            'city': self.faker.city(),
            'total_assets': self.faker.pydecimal(min_value=100, max_value=1000, right_digits=2)
        }
        return document

    def on_start(self):
        """
        Executed every time a new test is started - place init code here
        """
        self.collection, self.collection_secondary = self.ensure_sharded_collection(DEFAULTS['COLLECTION_NAME'], {'id': pymongo.HASHED})
        self.cached_ids = []

    @mongodb_task(weight=int(DEFAULTS['INSERT_WEIGHT']))
    def insert_single_document(self):
        document = self.generate_new_document()
        if len(self.cached_ids) < IDS_TO_CACHE:
            self.cached_ids.append(document['id'])
        else:
            self.cached_ids[random.randint(0, len(self.cached_ids) - 1)] = document['id']

        self.collection.with_options(write_concern=WriteConcern(w='majority')).insert_one(document)

    @mongodb_task(weight=int(DEFAULTS['FIND_WEIGHT']))
    def find_document(self):
        # find a random document using an index
        if not self.cached_ids:
            return

        self.collection.find_one({'id': self.cached_ids[random.randint(0, len(self.cached_ids) - 1)]})

    @mongodb_task(weight=int(DEFAULTS['UPDATE_WEIGHT']))
    def update_document(self):
        if not self.cached_ids:
            return

        # update a random document using an index
        self.collection.update_one({'id': self.cached_ids[random.randint(0, len(self.cached_ids) - 1)]}, {'$set': {'updated': True}}, upsert=False)

    @mongodb_task(weight=int(DEFAULTS['BULK_INSERT_WEIGHT']), batch_size=int(DEFAULTS['DOCS_PER_BATCH']))
    def insert_documents_bulk(self):
        self.collection.with_options(write_concern=WriteConcern(w='majority')).insert_many(
            [self.generate_new_document() for _ in
             range(int(DEFAULTS['DOCS_PER_BATCH']))])

    @mongodb_task(weight=int(DEFAULTS['MIGRATION_WEIGHT']))
    def migrate_chunk(self):
        global migrating
        if migrating:
            return
        migrating = True
        nss = DEFAULTS['DB_NAME'] + '.' + DEFAULTS['COLLECTION_NAME']
        # Get the collection UUID
        collection = self.config.get_collection('collections').find_one({'_id': nss})
        # Get a random chunk
        chunks = []
        for c in self.config.get_collection('chunks').find({'uuid': collection['uuid']}):
            chunks.append(c)

        chunk = chunks[random.randint(0, len(chunks) - 1)]

        # Get a different shard
        shards = []
        for s in self.config.get_collection('shards').find({'shard': {'$ne': chunk['shard']}}):
            shards.append(s)

        new_shard = shards[random.randint(0, len(shards) - 1)]

        # Move it to a different shard
        self.admin.command({'moveChunk': nss, 'find': chunk['min'], 'to': new_shard['_id']})
        migrating = False
