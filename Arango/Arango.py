from arango import ArangoClient
from Arango import insertData
import time

class Arango():
    def __init__(self, URI:str = "http://localhost:8529") -> None:
        self.client = ArangoClient(hosts=URI)
        self.db = self.client.db('HBI_datalake') # connection to the specific database
        self.shoreline = self.db.collection('shoreline') # connection to the main collection of the database where most data will be

        self.dataGetter = insertData(db=self.db, shoreline=self.shoreline) # all methods for inserting data are managed here

    def printAllIndexes(self):
        for index in self.shoreline.indexes():
            print(index)

    def addIndexes(self):
        self.shoreline.add_hash_index(fields=["attributes.organisation", "metadata.time"], unique=False)

    def insertData(self, directory):
        start_time = time.time()
        out = self.dataGetter.storeDirectory(directory)

        return (time.time() - start_time)