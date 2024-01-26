from pymongo import MongoClient
from Mongo.insertData import insertData

class Mongo():
    def __init__(self, URI:str = "mongodb://localhost:27017/", comp = 'snappy') -> None:
        self.client = MongoClient(URI, compressors=comp) # zstd more compress but slower
        # requires running npm install @mongodb-js/zstd to have zstd
        self.db = self.client["HBI_datalake"] # connection to the specific database
        self.shoreline = self.db["shoreline"] # connection to the main collection of the database where most data will be

        self.dataGetter = insertData(db=self.db, shoreline=self.shoreline) # all methods for isnerting data are managed here

        result = self.client.admin.command('isMaster')

        # Check the compression algorithms
        if 'compression' in result:
            print('Compression algorithms supported by the server:', result['compression'])
        else:
            print('The server does not support any compression algorithms.')


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
        self.dataGetter.storeDirectory(directory)


    def size(self):
        stats = self.db.command("dbstats")
        return stats['dataSize'] 



    ############ queries


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
