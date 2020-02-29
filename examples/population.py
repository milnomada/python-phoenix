#!/usr/bin/env python

import phoenixdb
import csv
import time
import math
import os

cluster_host = "192.168.99.100"
base_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')

with phoenixdb.connect('http://{}:8765/'.format(cluster_host), autocommit=True) as connection:
    with connection.cursor() as cursor:
        start = time.time()
        cursor.execute("DROP TABLE IF EXISTS population")
        cursor.execute("CREATE TABLE population (\
          id INTEGER PRIMARY KEY,\
          country_name VARCHAR,\
          country_code VARCHAR,\
          year INTEGER,\
          size INTEGER)")

        f = open("{}/datasets/population.csv".format(base_path))
        di = csv.DictReader(f, ('country_name', 'country_code', 'year', 'size'))
        id = 0
        for line in di:
          if line['size'] == 'Value':  # Skip Header line
            print(line)
            continue

          try:
            year = int(line['year'])
            size = int(math.floor(float(line['size'])))
            cursor.executemany("UPSERT INTO population VALUES (?, ?, ?, ?, ?)", [
              [
                id,
                line['country_name'],
                line['country_code'],
                year,
                size
              ]
            ])
          except TypeError as e:
            print("Exception {}".format(e))
          except ValueError as e:
            print("ValueError at csv line {}: {}".format(id + 1, e))

          id += 1

        print("insert elapsed {}s".format(time.time() - start))

        start = time.time()
        cursor.execute("SELECT * FROM population ORDER BY id")
        for row in cursor:
          print(row)

        print("show all elapsed {}s".format(time.time() - start))
