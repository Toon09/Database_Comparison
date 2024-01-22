from Mongo.Mongo import Mongo
from Arango.Arango import Arango

test_directory = "C:/Users/raul/OneDrive - HBI Bisscheroux/Documents/data"


mong = Mongo()
mong.createDatabase()

arang = Arango()
mong.createDatabase()




#arang.deleteDatabase()
#mong.deleteDatabase()


