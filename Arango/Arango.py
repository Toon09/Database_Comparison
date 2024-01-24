from arango import ArangoClient
from Arango.insertData import insertData
import time

class Arango():
    def __init__(self, URI:str = "http://localhost:8529") -> None:
        self.client = ArangoClient(hosts=URI)
        self.db = self.client.db('HBI_datalake', username='root', password='rooring') # connection to the specific database
        self.shoreline = self.db.collection('shoreline') # connection to the main collection of the database where most data will be

        self.dataGetter = insertData(db=self.db, shoreline=self.shoreline) # all methods for inserting data are managed here



    def deleteDatabase(self):
        sys_db = self.client.db('_system', username='root', password='rooring')
        if sys_db.has_database('HBI_datalake'):
            sys_db.delete_database('HBI_datalake')


    def createDatabase(self):
        sys_db = self.client.db('_system', username='root', password='rooring') ####### WHY DO THIS AGAIN AND AGAIN??
        if not sys_db.has_database('HBI_datalake'):
            sys_db.create_database('HBI_datalake')
        self.db = self.client.db('HBI_datalake', username='root', password='rooring')
        if not self.db.has_collection('shoreline'):
            self.shoreline = self.db.create_collection('shoreline')
        else:
            self.shoreline = self.db.collection('shoreline')
            
        self.addIndexes()


    def printAllIndexes(self):
        for index in self.shoreline.indexes():
            print(index)

    def addIndexes(self):
        self.shoreline.add_hash_index(fields=["attributes.organisation", "metadata.time"], unique=False)

    def insertData(self, directory):
        start_time = time.time()
        self.dataGetter.storeDirectory(directory)

        return (time.time() - start_time)