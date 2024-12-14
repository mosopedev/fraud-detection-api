import os
from couchbase.cluster import Cluster, timedelta
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import CouchbaseException
from app.models.transaction import Transaction
import json

class CouchbaseService:
    def __init__(self, bucket_name):
        options = ClusterOptions(PasswordAuthenticator(os.getenv('COUCHBASE_USER'), os.getenv('COUCHBASE_PASSWORD')))
        options.apply_profile('wan_development')
        
        self.cluster = Cluster(
            os.getenv('COUCHBASE_CLUSTER'),
            options
        )
        self.cluster.wait_until_ready(timedelta(seconds=5))
        self.bucket = self.cluster.bucket(bucket_name)
        self.collection = self.bucket.default_collection()

    def insert_transaction(self, transaction):
        try:
            print("\ncouchbase serv ",transaction['transaction_id'])
            self.collection.upsert(transaction['transaction_id'], transaction)
            return True
        except CouchbaseException as e:
            print(f"Error inserting transaction: {e}")
            return False

    def get_all_transactions(self):
        try:
            query = f"SELECT * FROM `{self.bucket.name}`;"
            rows = self.cluster.query(query)
            return [Transaction.from_dict(row[self.bucket.name]) for row in rows]
        except CouchbaseException as e:
            print(f"Error fetching transactions: {e}")
            return []
