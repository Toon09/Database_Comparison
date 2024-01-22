from pymongo import MongoClient
from Mongo.insertData import insertData
import time

class Mongo():
    def __init__(self, URI:str = "mongodb://localhost:27017/") -> None:
        self.client = MongoClient(URI, compressors='zlib')
        self.db = self.client["HBI_datalake"] # connection to the specific database
        self.shoreline = self.db["shoreline"] # connection to the main collection of the database where most data will be

        self.dataGetter = insertData(db=self.db, shoreline=self.shoreline) # all methods for isnerting data are managed here


    def deleteDatabase(self):
        self.client.drop_database('HBI_datalake')

    def createDatabase(self):
        self.db = self.client["HBI_datalake"]
        self.shoreline = self.db["shoreline"]


    def printAllIndeces(self):
        for index in self.shoreline.list_indexes():
            print(index)

    def addIndexes(self):
        self.shoreline.create_index([("attributes.organisation", 1), ("metadata.time", 1)])
    
    def insertData(self, directory):
        start_time = time.time()
        self.dataGetter.storeDirectory(directory)
    
        return (time.time() - start_time)

