from Mongo.Mongo import Mongo
from Arango.Arango import Arango
from Couchbase.Couchbase import Couchbase

import time

test_directory = "C:/Users/raul/OneDrive - HBI Bisscheroux/Documents/data"


mong = Mongo()
mong.createDatabase()

arang = Arango()
#arang.createDatabase()

#couch = Couchbase()
#couch.createDatabase()


#print(f"mong: {mong.insertData(test_directory)}")
#print(f"arang: {arang.insertData(test_directory)}")

print()


s = time.time()
cur = arang.findUniqueAppIds()
print(f"exec: {time.time()-s}\n")

for x in cur:
    print(x)

print()

s = time.time()
cur = arang.findUniqueOrganisationIds()
print(f"exec: {time.time()-s}\n")

for x in cur:
    print(x)





#arang.deleteDatabase()
#mong.deleteDatabase()


