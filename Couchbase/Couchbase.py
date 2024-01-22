from couchbase.cluster import Cluster, ClusterOptions
from couchbase_core.n1ql import N1QLQuery
from couchbase.auth import PasswordAuthenticator

from Arango import insertData
import time

class Couchbase():
    def __init__(self, URI:str = "couchbase://localhost", username:str = "username", password:str = "password") -> None:
        cluster = Cluster(URI, ClusterOptions(PasswordAuthenticator(username, password)))
        self.db = cluster.bucket('HBI_datalake') # connection to the specific database
        self.shoreline = self.db.default_collection() # connection to the main collection of the database where most data will be

        self.dataGetter = insertData(db=self.db, shoreline=self.shoreline) # all methods for inserting data are managed here


    def deleteDatabase(self):
        self.cluster.buckets().drop_bucket('HBI_datalake')

    def createDatabase(self):
        self.cluster.buckets().create_bucket('HBI_datalake')
        self.db = self.cluster.bucket('HBI_datalake')
        self.shoreline = self.db.default_collection()
        

    def printAllIndexes(self):
        for index in self.db.query_indexes().get_all_indexes():
            print(index)

    def addIndexes(self):
        self.db.query_indexes().create_index('HBI_datalake', 'index_name', fields=["attributes.organisation", "metadata.time"])

    def insertData(self, directory):
        start_time = time.time()
        self.dataGetter.storeDirectory(directory)

        return (time.time() - start_time)
    
