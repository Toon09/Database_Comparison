from Mongo.Mongo import Mongo
from Arango.Arango import Arango
from Couchbase.Couchbase import Couchbase

test_directory = "C:/Users/raul/OneDrive - HBI Bisscheroux/Documents/data"


mong = Mongo()
mong.createDatabase()

arang = Arango()
arang.createDatabase()

#couch = Couchbase()
#couch.createDatabase()


#print(f"mong: {mong.insertData(test_directory)}")
#print(f"arang: {arang.insertData(test_directory)}")

print()

mong.printAllIndexes()
print()
arang.printAllIndexes()


#arang.deleteDatabase()
#mong.deleteDatabase()


