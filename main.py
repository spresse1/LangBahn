#! /usr/bin/env python3

import os
import sys
import csv
import requests
import traceback
import pygtfs
import database
import queuelib
import pickle
import argparse
import datetime
import sqlalchemy
from tempfile import TemporaryFile
from glob import glob
from geopy.distance import distance
from sqlalchemy import and_


CSV_URL="https://bit.ly/catalogs-csv"

# This adds some extra custom tables to the pygtfs tables. See database module
# for details on how this is done
database.ducktype_environment(pygtfs)

class PriorityQueue():
    def __init__(self, dirname='queues/queue-dir'):
        qfactory = lambda priority: queuelib.FifoDiskQueue(f'{dirname}-%s' % priority)
        self.pq = queuelib.PriorityQueue(qfactory)

    def push(self, priority, object):
        self.pq.push(pickle.dumps(object, priority))
    
    def pop(self):
        return pickle.loads(self.pq.pop())

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
        verbose=True):

    files = glob(os.path.join(datadir, "*.zip"))
    files.sort()
    sched = pygtfs.Schedule(databasefile)
    for zipfile in files:
        print(f"Importing {zipfile}")
        try:
            pygtfs.append_feed(sched, zipfile)
        except Exception:
            traceback.print_exc()

def calculate(databasefile="merged.sqlite"):
    sched = pygtfs.Schedule(databasefile)

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

    # Start code for calculating distances and times.
    Stop = pygtfs.gtfs_entities.Stop
    StopTime = pygtfs.gtfs_entities.StopTime

    stoptimes = sched.stops_query.add_entity(StopTime).join(
        StopTime,
        and_(
            StopTime.feed_id == Stop.feed_id,
            StopTime.stop_id == Stop.stop_id
        )
    ).order_by(StopTime.feed_id, StopTime.trip_id, StopTime.stop_sequence)

    # Trip-level variables
    currentfeed = None
    currenttrip = None
    starttime = None
    cumdistance = 0
    previousstop = None
    count = 0

    for time in stoptimes:
        # Check if this starts a new trip, store if so
        if time[1].feed_id != currentfeed or time[1].trip_id != currenttrip:
            if currenttrip is not None:
                endtime = previousstop[1].arrival_time
                # if endtime is None:
                #     endtime = previousstop[1].departure_time
                # Only record if this was an actual trip
                print(f"feed: {currentfeed}, trip: {currenttrip}, time: {endtime}, {starttime}, {endtime-starttime}, distance: {cumdistance}")
                sched.session.add(database.TripData(
                    trip_id=currenttrip,
                    time=endtime-starttime,
                    distance=cumdistance
                ))
            
            # Reset stored state for next trip
            currentfeed = time[1].feed_id
            currenttrip = time[1].trip_id
            starttime = time[1].departure_time
            cumdistance = 0
            previousstop = None
        
        # Calculate distance from previous stop, ignoring if this is the first stop
        # Do this calculation first because we want this distance if we are at the end of a line
        if previousstop is not None:
            pass
            newlatlon = (time[0].stop_lat, time[0].stop_lon)
            oldlatlon = (previousstop[0].stop_lat, previousstop[0].stop_lon)
            cumdistance += distance(oldlatlon, newlatlon).km
            
        previousstop = time
        count += 1

        if count % 5000 == 0:
            sched.session.flush()
            print(".", end="")

    print(f"feed: {currentfeed}, trip: {currenttrip}, time: {endtime}, {starttime}, {endtime-starttime}, distance: {cumdistance}")
    sched.session.add(database.TripData(
        trip_id=currenttrip,
        time=endtime-starttime,
        distance=cumdistance
    ))

    # Put the last items in the database
    sched.session.flush()
    sched.session.commit()

        #print(f"feed: {time[1].feed_id}, trip: {time[1].trip_id}, stop: {time[1].stop_sequence}, departs: {time[1].departure_time} from {time[0].stop_name} ({time[0].stop_lat},{time[0].stop_lon}), distance: {cumdistance} in {time[1].arrival_time-starttime}")


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

def find_transfers(stop, start_time, end_time):
    """
    Returns a list of departing trains at the requested station in the requested
    time window.
    """
    print(stop, start_time, end_time)

def find_trip(databasefile="merged.sqlite"):
    sched = pygtfs.Schedule(databasefile)
    
    parser = argparse.ArgumentParser(
        description="LangBahn finds the longest possible train trip in a certain period of time",
    )
    parser.add_argument("start_time", action="store")
    parser.add_argument("end_time", action="store")
    parser.add_argument("start_stop", action="store")
    parser.add_argument("trip_time", action="store")
    args = parser.parse_args(sys.argv[2:])

    # Parse times into useful things
    start_time = datetime.datetime.fromisoformat(args.start_time)
    end_time = datetime.datetime.fromisoformat(args.end_time)

    # Find the station by name
    try:
        start_stop = sched.stops_query.where(pygtfs.gtfs_entities.Stop.stop_name == args.start_stop).one()
    except sqlalchemy.exc.NoResultFound:
        print(f"Unable to find a stop named {args.start_stop}!")
        return

    trip_time = datetime.timedelta(hours=int(args.trip_time))

    trips = find_transfers(start_stop, start_time, end_time)
    

def explore(databasefile="merged.sqlite"):
    sched = pygtfs.Schedule(databasefile)
    for row in sched.stops_query.where(pygtfs.gtfs_entities.Stop.stop_name.contains("Stuttgart")):
        #if "Singen" in row.stop_name:
        print(f"{row.stop_id}: {row.stop_name}: {row.parent_station} ({row.stop_lat}, {row.stop_lon})")
    print("Done")
    from datetime import datetime
    start=datetime.now()

    import pdb; pdb.set_trace()

if __name__ == "__main__":
    if sys.argv[1] == "download":
        get_gtfs_sources(countries=["DE"])
    elif sys.argv[1] == "import":
        import_to_db("Germany")
    elif sys.argv[1] == "calculate":
        calculate()
    elif sys.argv[1] == "find_trip":
        find_trip()
    elif sys.argv[1] == "explore":
        explore()