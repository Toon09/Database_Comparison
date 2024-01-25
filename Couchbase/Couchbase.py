from couchbase.cluster import Cluster, ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import BucketNotFoundException

from Couchbase import insertData

class Couchbase():
    def __init__(self, URI:str = "couchbase://localhost", username:str = "root", password:str = "rooring") -> None:
        self.cluster = Cluster.connect(URI, ClusterOptions(PasswordAuthenticator(username, password)))
        self.bucket_name = 'HBI_datalake'
        self.db = None
        self.shoreline = None
        self.dataGetter = None

    def get_db(self):
        if self.db is None:
            self.db = self.cluster.bucket(self.bucket_name)
            self.shoreline = self.db.default_collection()
            self.dataGetter = insertData(db=self.db, shoreline=self.shoreline)
        return self.db

    def deleteDatabase(self):
        self.cluster.buckets().drop_bucket(self.bucket_name)
        self.db = None
        self.shoreline = None
        self.dataGetter = None

    def createDatabase(self):
        self.cluster.buckets().create_bucket({
          "name": self.bucket_name,
          "ram_quota_mb": 100,
          "bucket_type": "couchbase"
        })
        self.get_db()
        self.addIndexes()

    def printAllIndexes(self):
        for index in self.get_db().query_indexes().get_all_indexes():
            print(index)

    def addIndexes(self):
        self.get_db().query_indexes().create_index(self.bucket_name, 'index_name', fields=["attributes.organisation", "metadata.time"])

    def insertData(self, directory):
        self.dataGetter.storeDirectory(directory)
