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

print()

s = time.time()
cur = arang.findUniqueDeviceIds()
print(f"exec: {time.time()-s}\n")


print()

s = time.time()
cur = arang.queryByDeviceId("fe8940ab-9ffe-4c16-ba59-b3235e2ae776#1eda41b6-78af-4eff-aaa4-88f928a33537#6c170ee5-5481-42ff-bf0d-26a36743b043#70b3d543800026d4")
print(f"exec: {time.time()-s}\n")


#arang.deleteDatabase()
#mong.deleteDatabase()


