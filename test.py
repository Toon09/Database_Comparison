from Mongo.Mongo import Mongo
from Arango.Arango import Arango
from Couchbase.Couchbase import Couchbase

test_directory = "C:/Users/raul/OneDrive - HBI Bisscheroux/Documents/data"


mong = Mongo()
mong.createDatabase()

arang = Arango()
arang.createDatabase()

couch = Couchbase()
couch.createDatabase()


arang.deleteDatabase()
mong.deleteDatabase()


