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

        self.addIndexes()


    def printAllIndexes(self):
        for index in self.shoreline.list_indexes():
            print(index)

    def addIndexes(self):
        self.shoreline.create_index([("attributes.organisation", 1), ("metadata.time", 1)])
    
    def insertData(self, directory):
        start_time = time.time()
        self.dataGetter.storeDirectory(directory)
    
        return (time.time() - start_time)


    def queryPayloadFields(self, organisation_id, start_date, end_date):
        # Convert datetime objects to ISO format strings
        start_date = start_date.isoformat()
        end_date = end_date.isoformat()

        cursor = self.shoreline.find(
            {"attributes.organisation": organisation_id, "metadata.time": {"$gte": start_date, "$lte": end_date}},
            {"payload_fields": 1}
        )

        return cursor


    def findUniqueModelIds(self):
        cursor = self.shoreline.distinct("model_id")
        return cursor
    

    def findUniqueDeviceIds(self):
        cursor = self.shoreline.distinct("device_id")
        return cursor


    def findUniqueOrganisationIds(self):
        cursor = self.shoreline.distinct("attributes.organisation")
        return cursor
    

    def queryByDeviceId(self, device_id):
        cursor = self.shoreline.find(
            {"device_id": device_id},
            {"payload_fields": 1}
        )

        return cursor
