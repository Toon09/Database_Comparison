
"""
import all 3 classes

inside loop

get data +++

create db's +++

add indeces +++

do data insertion and measure time it took for each of the db's
append time measure in files of each folder corresponding to time measures

do size test
append size ratio and size measure (compression rate, actual size of data) to file of size

do read test
append in corresponding file of each


delete db's +++


"""


from Mongo.Mongo import Mongo
from Arango.Arango import Arango
from Couchbase.Couchbase import Couchbase


import os
import csv
import json
import ijson
import time
import random
from decimal import Decimal


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def select_json_files(directory, M):
    # Get all JSON files in the directory and subdirectories
    json_files = [os.path.join(root, file) 
                  for root, dirs, files in os.walk(directory) 
                  for file in files if file.endswith('.json')]

    # Shuffle the list of files to ensure random selection
    random.shuffle(json_files)

    selected_files = []
    total_size = 0

    # Select files until total size exceeds M
    for file in json_files:
        file_size = os.path.getsize(file)
        if total_size + file_size <= M:
            total_size += file_size
            selected_files.append(file)

    # Load the selected JSON files into an ijson object
    ijson_objects = []
    for file in selected_files:
        with open(file, 'r') as f:
            ijson_objects.extend(next(ijson.items(f, '')))

    # Save the ijson object to a new JSON file
    with open('0_exp_data/data.json', 'w') as outfile:
        json.dump(ijson_objects, outfile, cls=DecimalEncoder)

    return selected_files



def writeCSV(array, file_path):
    with open(file_path, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(array)


def size_dir(path): # ="C:/ProgramData/ArangoDB/engine-rocksdb/journals/"
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)

    return total_size


def rowAVG(file_path, row=-1):
    with open(file_path, 'r') as file:
        line = file.readlines()
        line = line[row]
        
    values = line.split(',')
    return sum(float(value) for value in values) / len(values)
    


data_directory = "C:/Users/raul/OneDrive - HBI Bisscheroux/Documents/werk/report_phase1/data_report/"

N = 1_000 # number of data samples to get
min_size = 62_950_000 # min size (in bytes) of how much data is to be saved for each data sample of the exp, 15% of total data rigth now
random.seed(8774) # for reproducibility

# inputting data is linear, thus  min_size * 0.4 is approx the time for mongoDB of input
# to get ebtter approx do a large avg of time/min_size and use as "velocity"

for i in range(N):
    mong = Mongo()
    #arang = Arango()

    mong.deleteDatabase()
    #arang.deleteDatabase()

    # databases are created with indexes
    mong.createDatabase()
    #arang.createDatabase()


    # generating the data
    select_json_files(data_directory, min_size)
    selected_data = "C:/Users/raul/OneDrive - HBI Bisscheroux/Documents/Dev/Database_Comparison/0_exp_data"

    # insert data speed
    s = time.time()
    mong.insertData(selected_data)
    writeCSV( [time.time()-s], "Mongo/data_zstd/insert.csv" )

    s = time.time()
    #arang.insertData(selected_data)
    #writeCSV( [time.time()-s], "Arango/data/insert.csv" )

    # space efficiency ########################################
    data_size = size_dir(selected_data)

    #print(arang.size(), ", ", arang.size()/data_size)
    print(mong.size(), ", ", mong.size()/data_size ) # size of mongo db data
    print(data_size) # size of input data

    writeCSV( [mong.size()/data_size], "Mongo/data_zstd/space.csv" )
    #writeCSV( [arang.size()/data_size], "Arango/data/space.csv" )
    

    # query efficiency
    s = time.time()
    mong.findUniqueModelIds()
    writeCSV( [time.time()-s], "Mongo/data_zstd/readModelID.csv" )

    s = time.time()
    #arang.findUniqueModelIds()
    #writeCSV( [time.time()-s], "Arango/data/readModelID.csv" )

    s = time.time()
    mong.findUniqueOrganisationIds()
    writeCSV( [time.time()-s], "Mongo/data_zstd/readOrgID.csv" )

    s = time.time()
    #arang.findUniqueOrganisationIds()
    #writeCSV( [time.time()-s], "Arango/data/readOrgID.csv" )


    s = time.time()
    cur = mong.findUniqueDeviceIds()
    writeCSV( [time.time()-s], "Mongo/data_zstd/readDeviceID.csv" )

    temp = []
    for x in cur:
        if x == None:
            continue
        s = time.time()
        mong.queryByDeviceId(x)
        temp.append(time.time()-s)

    writeCSV( temp, "Mongo/data_zstd/readDevicesFields.csv" )
    writeCSV( [sum(temp)/len(temp)], "Mongo/data_zstd/readDevicesFieldsAVG.csv" )


    s = time.time()
    #cur = arang.findUniqueDeviceIds()
    #writeCSV( [time.time()-s], "Arango/data/readDeviceID.csv" )

    temp = []
    for x in cur:
        if x == None:
            continue
        s = time.time()
        #arang.queryByDeviceId(x)
        temp.append(time.time()-s)

    #writeCSV( temp, "Arango/data/readDevicesFields.csv" )
    #writeCSV( [sum(temp)/len(temp)], "Arango/data/readDevicesFieldsAVG.csv" )


    # delete to empty and be able to get next data sample without any relations
    mong.deleteDatabase()
    #arang.deleteDatabase()

    del mong
    #del arang

    print(f"round {i+1} done!\n")
