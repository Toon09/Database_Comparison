# ArangoDB
from arango import ArangoClient
import ijson
import os
from decimal import Decimal
from datetime import datetime

class insertData():
    def __init__(self, db, shoreline):
        self.db = db
        self.shoreline = shoreline

    # ArangoDB does not support the Decimal datatype, so it needs to be removed of all the data (replaced by float)
    # this function replaces all Decimal's by floats
    def checkForDecimal(self, obj):
        # when you see a list go thru every element and check if its decimal, then change
        # whenever you see a dictionary inside anything you recurse
        if isinstance(obj, dict):
            for key in obj:
                if isinstance(obj[key], Decimal):
                    obj[key] = float(obj[key])

                elif isinstance(obj[key], list):
                    for x in range(len(obj[key])):

                        if isinstance(obj[key][x], Decimal):
                            obj[key][x] = float(obj[key][x])

                        elif isinstance(obj[key][x], dict):
                            self.checkForDecimal(obj[key][x])

                elif isinstance(obj[key], dict):
                    self.checkForDecimal(obj[key])
    
        elif isinstance(obj, Decimal):
            obj = float(obj)

    # file must be a json and in the same format as
    def storeFile(self, dataFile):
        
        with open(dataFile, "r") as file:
            jsonF = ijson.items(file, 'item')
            
            for obj in jsonF:
                # exhaustively check all fields to check if there's a single decimal
                self.checkForDecimal(obj) # Decimal type breaks db, so replaced by float
                self.changeTimeFormat(obj) # time as string doesn't allow for good searches, so replaced by datetime (from micro-s to micro-s)
                self.shoreline.insert(obj)

    # stores all json files inside a directory
    def storeDirectory(self, directoryPath:str):
        for filename in os.listdir(directoryPath):
            if filename.endswith(".json"):
                self.storeFile(f"{directoryPath}/{filename}")
                


    # changes the time from a string to a datetime type which is better used for ranges, etc
    def changeTimeFormat(self, doc):
        if "metadata" in doc:
            if not isinstance(doc["metadata"]["time"], datetime):
                time = doc["metadata"]["time"][:-1] # adds the 0's missing
                if len(time) < 27-1:
                    time = time + "0"*(27-len(doc["metadata"]["time"])) 
                elif len(time) > 27-1:
                    time = time[:27-len(doc["metadata"]["time"])] 

                newTime = datetime.strptime(time + "Z", "%Y-%m-%dT%H:%M:%S.%fZ")
                doc["metadata"]["time"] = newTime.isoformat() # Convert datetime to string

        else: 
            newTime = datetime.strptime(doc["created_at"][:-6], "%Y-%m-%dT%H:%M:%S.%f")
            doc["created_at"] = newTime.isoformat() # Convert datetime to string

