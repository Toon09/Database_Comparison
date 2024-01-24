
"""
import all 3 classes

inside loop

get data +++

create db's

add indeces

do data insertion and measure time it took for each of the db's
append time measure in files of each folder corresponding to time measures

do size test
append size ratio and size measure (compression rate, actual size of data) to file of size

do read test
append in corresponding file of each


delete db's


"""


from Mongo.Mongo import Mongo
from Arango.Arango import Arango
from Couchbase.Couchbase import Couchbase


import os
import random
import json
import ijson
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




data_directory = "C:/Users/raul/OneDrive - HBI Bisscheroux/Documents/werk/report_phase1/data_report/"

N = 1_000 # number of data samples to get
min_size = 12_000_000 # min size (in bytes) of how much data is to be saved for each data sample of the exp


for i in range(N):
    mong = Mongo()
    arang = Arango()

    # databases are created with indexes
    mong.createDatabase()
    arang.createDatabase()


    # generating the data
    select_json_files(data_directory, min_size)

    # write code to append data in the corresponding datafolder


    # delete to empty and be able to get next data sample without any relations
    mong.deleteDatabase()
    arang.deleteDatabase()

    del mong
    del arang
