#! /usr/bin/env python3

import os
import sys
import csv
import requests
import traceback
import pygtfs
import database
from tempfile import TemporaryFile
from glob import glob
from geopy.distance import distance

CSV_URL="https://bit.ly/catalogs-csv"

# This adds some extra custom tables to the pygtfs tables. See database module
# for details on how this is done
database.ducktype_environment(pygtfs)

def get_gtfs_sources(outputdir="data", countries=None, active=True, force=False):
    r = requests.get(CSV_URL)
    r.encoding = 'utf-8'

    with TemporaryFile(mode="w+") as csvfile, open(os.path.join(outputdir, "agency_list"), "w") as listfile:
        # write contents to a tempfile
        csvfile.write(r.text)

        csvfile.seek(0)

        reader = csv.DictReader(csvfile)
        for row in reader:
            if row['status'] not in ['', 'active']:
                print(f"Skipping {row['provider']}: inactive")
                continue
            if countries is not None and \
                row['location.country_code'] not in countries:
                print(f"Skipping {row['provider']}: outside requested countries")
                continue
            if row['urls.authentication_type'] not in ['', '0']:
                print(f"Skipping {row['provider']}: requires authentication")
                continue
            print(f"{row['provider']}: {row['urls.direct_download']}")

            filename = os.path.join(outputdir, f"{row['mdb_source_id']}.zip")
            listfile.write(f"{row['mdb_source_id']}, \"{row['provider']}\"\n")


            try:
                data = requests.get(row['urls.direct_download'])
            except Exception:
                print(f"Couldn't download {row['provider']}:")
                traceback.print_exc()
            

            mode = "xb"
            if force:
                mode = "wb"
            try:
                with open(filename, mode) as f:
                    for chunk in data.iter_content(chunk_size=128):
                        f.write(chunk)
            except OSError as e:
                if e.errno == 17:
                    print("Already downloaded")
                else:
                    raise e

def import_to_db(location, datadir="data", databasefile="merged.sqlite", 
        boxes="boxes.sqlite", verbose=True):

    files = glob(os.path.join(datadir, "*.zip"))
    files.sort()
    sched = pygtfs.Schedule(databasefile)
    for zipfile in files:
        print(f"Importing {zipfile}")
        try:
            pygtfs.append_feed(sched, zipfile)
        except Exception:
            traceback.print_exc()
    
    count = 0
    for stop in sched.stops:
        box = latlon_to_box(float(stop.stop_lat), float(stop.stop_lon))

        sched.session.add(database.BoxStation(
            stop_id=stop.stop_id, box_id=box))
        count += 1

        if count % 5000 == 0:
            sched.session.flush()
            print(".", end="")
    sched.session.flush()
    sched.session.commit()


def latlon_to_box(latitude:float, longitude:float) -> int:
    latpart = int((latitude + 90) * 10 )
    lonpart = int((longitude + 180) * 10)
    return latpart * 10000 + lonpart

def get_neighbor_boxes(box:int):
    lat = int(box / 10000)
    lon = int(box % 10000)

    mods = [
        (0, 0),
        (1, 0),
        (0, 1),
        (-1, 0),
        (0, -1),
        (1, 1),
        (-1, -1),
        (1, -1),
        (-1, 1),
    ]

    return [ ((lat + x) % 1800 ) * 10000 + ((lon + y) % 3600 ) for x, y in mods ]

def get_neighbor_stops(sched, stop):
    box = latlon_to_box(stop.stop_lat, stop.stop_lon)
    boxes = get_neighbor_boxes(box)

    results = []
    for res in sched.boxstations_query.where(database.BoxStation.box_id.in_(boxes)):
        results += sched.stops_by_id(res.stop_id)
    
    return results

def explore(databasefile="merged.sqlite"):
    sched = pygtfs.Schedule(databasefile)
    for row in sched.stops_query.where(pygtfs.gtfs_entities.Stop.stop_name.contains("Stuttgart")):
        #if "Singen" in row.stop_name:
        print(f"{row.stop_id}: {row.stop_name}: {row.parent_station} ({row.stop_lat}, {row.stop_lon})")
    print("Done")
    from datetime import datetime
    start=datetime.now()

    # root = sched.stops[0]

    # for stop in sched.stops:
    #     distance((root.stop_lat, root.stop_lon), (stop.stop_lat, stop.stop_lon))

    # end = datetime.now()
    # runtime = end-start
    # print(f"Runtime: {runtime} seconds for {len(sched.stops)} stops")
    # print(f"or {runtime/len(sched.stops)} seconds per stop (remember this is n^2!)")

    print(sched.stops[0])
    for stop in get_neighbor_stops(sched, sched.stops[0]):
        print(f"{stop}: {distance((sched.stops[0].stop_lat, sched.stops[0].stop_lon), (stop.stop_lat, stop.stop_lon)).km}")
    import pdb; pdb.set_trace()

if __name__ == "__main__":
    if sys.argv[1] == "download":
        get_gtfs_sources(countries=["DE"])
    elif sys.argv[1] == "import":
        import_to_db("Germany")
    elif sys.argv[1] == "explore":
        explore()