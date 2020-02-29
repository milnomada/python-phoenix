#!/usr/bin/env python

import phoenixdb
import csv
import time
import math

with phoenixdb.connect('http://192.168.99.100:8765/', autocommit=True) as connection:
    with connection.cursor() as cursor:
        start = time.time()
        cursor.execute("DROP TABLE IF EXISTS airports")
        cursor.execute("CREATE TABLE airports (\
          id INTEGER PRIMARY KEY,\
          ident VARCHAR,\
          airport_type VARCHAR,\
          name VARCHAR,\
          elevation_ft INTEGER,\
          elevation_m INTEGER,\
          continent VARCHAR,\
          iso_country VARCHAR,\
          iso_region VARCHAR,\
          municipality VARCHAR,\
          gps_code VARCHAR,\
          iata_code VARCHAR,\
          local_code VARCHAR,\
          lat DECIMAL,\
          lng DECIMAL)")

        f = open("/usr/local/www/hadoop/python-phoenixdb/airport-codes.csv")
        di = csv.DictReader(f, ('ident', 'airport_type', 'name', 'elevation_ft', 'continent',
                                'iso_country', 'iso_region', 'municipality', 'gps_code',
                                'iata_code', 'local_code', 'coordinates'))
        id = 0
        for line in di:
          if line['ident'] == 'ident':  # Header line
            print(line)
            continue

          try:
            coor = line['coordinates'].replace(',', '').split(" ")
            lat = float(coor[0])
            lng = float(coor[1])
            feet = int(line['elevation_ft'])
            meters = int(math.floor(feet * 0.3048))

            cursor.executemany("UPSERT INTO airports VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", [
              [
                id,
                line['ident'],
                line['airport_type'],
                line['name'],
                feet,
                meters,
                line['continent'],
                line['iso_country'],
                line['iso_region'],
                line['municipality'],
                line['gps_code'],
                line['iata_code'],
                line['local_code'],
                lat,
                lng
              ]
            ])
          except TypeError as e:
            print("Exception {}".format(e))
          except ValueError as e:
            print("ValueError at csv line {}: {}".format(id + 1, e))

          id += 1

        print("insert elapsed {}s".format(time.time() - start))

        start = time.time()
        cursor.execute("SELECT * FROM airports ORDER BY id")
        for row in cursor:
          print(row)

        print("show all elapsed {}s".format(time.time() - start))
