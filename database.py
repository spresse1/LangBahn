from sqlalchemy import Column, ForeignKey, ForeignKeyConstraint, and_
from sqlalchemy.types import (Unicode, Integer, Float, Boolean, Date, Interval,
                              Numeric)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, validates, synonym

import pygtfs

# Get the pygtfs SQLalchemy base to add out tables to
# This is an awful hack.
Base = pygtfs.schedule.Base

class BoxStation(Base):
    __tablename__ = '_boxstation'
    _plural_name_ = 'boxstations'
    stop_id = Column(Unicode, ForeignKey('stops.stop_id'))
    box_id = Column(Integer, index=True)
    boxstation_id = Column(Integer, primary_key=True)
    id = synonym('boxstation_id')

    def __repr__(self):
        return '<BoxStation %s: %s in %d>' % (self.box_id, self.stop_id, self.box_id)

class TripData(Base):
    __tablename__ = "_tripdata"
    _plural_name_ = "tripdata"
    trip_id = Column(Unicode, ForeignKey('trips.trip_id'), index=True)
    time = Column(Interval)
    distance = Column(Float)
    tripdata_id = Column(Integer, primary_key=True)
    id = synonym('tripdata_id')

    def __repr__(self):
        return '<TripData %s: %s in %d>' % (self.trip_id, self.distance, self.time)

modules = [ BoxStation, TripData ]

def ducktype_environment(pygtfs):
    # Awful hack part 2: add custom tables to the pygtfs sched object:
    for entity in modules:
        pygtfs.gtfs_entities.gtfs_all.append(entity)
        entity_doc = "A list of :py:class:`pygtfs.gtfs_entities.{0}` objects".format(entity.__name__)
        entity_raw_doc = ("A :py:class:`sqlalchemy.orm.Query` object to fetch "
                        ":py:class:`pygtfs.gtfs_entities.{0}` objects"
                        .format(entity.__name__))
        entity_by_id_doc = "A list of :py:class:`pygtfs.gtfs_entities.{0}` objects with matching id".format(entity.__name__)
        setattr(pygtfs.Schedule, entity._plural_name_, pygtfs.schedule._meta_query_all(entity, entity_doc))
        setattr(pygtfs.Schedule, entity._plural_name_ + "_query",
                pygtfs.schedule._meta_query_raw(entity, entity_raw_doc))
        if hasattr(entity, 'id'):
            setattr(pygtfs.Schedule, entity._plural_name_ + "_by_id", pygtfs.schedule._meta_query_by_id(entity, entity_by_id_doc))    