from arango import ArangoClient
from Arango.insertData import insertData
import os

class Arango():
    def __init__(self, URI:str = "http://localhost:8529") -> None:
        self.client = ArangoClient(hosts=URI)
        self.sys_db = self.client.db('_system', username='root', password='rooring') # connection to the _system database
        self.db = self.client.db('HBI_datalake', username='root', password='rooring') # connection to the specific database
        self.shoreline = self.db.collection('shoreline') # connection to the main collection of the database where most data will be

        self.dataGetter = insertData(db=self.db, shoreline=self.shoreline) # all methods for inserting data are managed here

    def deleteDatabase(self):
        if self.sys_db.has_database('HBI_datalake'):
            self.sys_db.delete_database('HBI_datalake')

    def createDatabase(self):
        if not self.sys_db.has_database('HBI_datalake'):
            self.sys_db.create_database('HBI_datalake')
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
        self.dataGetter.storeDirectory(directory)

    def size(self):
        total_size = 0

        for collection in self.db.collections():
            collection_ = self.db.collection(collection["name"])

            for doc in collection_.all():
                doc_size = len(str(doc))

                total_size += doc_size

        return total_size



    ######### QUERIES



    def queryPayloadFields(self, organisation_id, start_date, end_date):
        # Convert datetime objects to ISO strings
        start_date = start_date.isoformat()
        end_date = end_date.isoformat()

        cursor = self.db.aql.execute(
            'FOR doc IN shoreline '
            'FILTER doc.attributes.organisation == @organisation_id '
            '&& doc.metadata.time >= @start_date '
            '&& doc.metadata.time <= @end_date '
            'RETURN doc.payload_fields',
            bind_vars={'organisation_id': organisation_id, 'start_date': start_date, 'end_date': end_date}
        )

        return cursor
    

    def findUniqueModelIds(self):
        cursor = self.db.aql.execute(
            'FOR doc IN shoreline '
            'COLLECT app_id = doc.app_id WITH COUNT INTO count '
            'RETURN app_id'
        )

        return cursor
    
    def findUniqueDeviceIds(self):
        cursor = self.db.aql.execute(
            'FOR doc IN shoreline '
            'COLLECT device_id = doc.device_id WITH COUNT INTO count '
            'RETURN device_id'
        )

        return cursor
    

    def findUniqueOrganisationIds(self):
        cursor = self.db.aql.execute(
            'FOR doc IN shoreline '
            'COLLECT organisation_id = doc.attributes.organisation WITH COUNT INTO count '
            'RETURN organisation_id'
        )

        return cursor


    def queryByDeviceId(self, device_id):
        cursor = self.db.aql.execute(
            'FOR doc IN shoreline '
            'FILTER doc.device_id == @device_id '
            'RETURN doc.payload_fields',
            bind_vars={'device_id': device_id}
        )

        return cursor

