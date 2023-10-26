from DbConnector import DbConnector
from typing import Union, Any
import logging
from datetime import datetime

class User(dict):
    KEYS = ('_id', 'has_labels', 'activities')
    def __init__(self,
                 id : str,
                 has_labels : bool,
                 activities : list = []) -> None:
        super().__setitem__('_id', id)
        super().__setitem__('has_labels', has_labels)
        super().__setitem__('activities', activities)
        
    def __setitem__(self, key : str, item : Any) -> None:
        if key not in User.KEYS:
            raise KeyError(f"The key {key} is not defined.")
        super().__setitem__(key, item)

class ActivityDenorm(dict):
    KEYS = ('_id', 'user')
    def __setitem__(self, key : str, item : Any) -> None:
        if key not in ActivityDenorm.KEYS:
            raise KeyError(f"The key {key} is not defined.")
        super().__setitem__(key, item)

class Activity(dict):
    KEYS = ('_id', 'user', 'transportation_mode', 'start_date_time', 'end_date_time', 'trackpoints')
    DENORM_KEYS = ('_id', 'user')
    def __init__(self,
                 id : int,
                 user : str,
                 transportation_mode : str = None,
                 start_date_time : datetime = None,
                 end_date_time : datetime = None,
                 trackpoints : list = []) -> None:
        super().__setitem__('_id', id)
        super().__setitem__('user', user)
        super().__setitem__('transportation_mode', transportation_mode)
        super().__setitem__('start_date_time', start_date_time)
        super().__setitem__('end_date_time', end_date_time)
        super().__setitem__('trackpoints', trackpoints)
        
    def __setitem__(self, key : str, item : Any) -> None:
        if key not in Activity.KEYS:
            raise KeyError(f"The key {key} is not defined.")
        super().__setitem__(key, item)

    def denorm(self) -> ActivityDenorm:
        return {key : super().__getitem__(key) for key in Activity.DENORM_KEYS}

class TrackPoint(dict):
    KEYS = ('_id', 'lat', 'lon', 'altitude', 'date_days', 'date_time', 'activity')
    def __init__(self,
                 id : int,
                 lat : float,
                 lon : float,
                 altitude : int,
                 date_days : float,
                 date_time : datetime,
                 activity : ActivityDenorm) -> None:
        super().__setitem__('_id', id)
        super().__setitem__('lat', lat)
        super().__setitem__('lon', lon)
        super().__setitem__('altitude', altitude)
        super().__setitem__('date_days', date_days)
        super().__setitem__('date_time', date_time)
        super().__setitem__('activity', activity)
        
    def __setitem__(self, key : str, item : Any) -> None:
        if key not in TrackPoint.KEYS:
            raise KeyError(f"The key {key} is not defined.")
        super().__setitem__(key, item)

class Database:
    def __init__(self) -> None:
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
    
    def create_collection(self, name : str) -> bool:
        try:
            collection = self.db.create_collection(name)
        except Exception as e:
            logging.critical(f'An error occured in create_collection() -> \n{e}')
            return False
        logging.info(f'Created a new collection: {collection}')
        return True
    
    def insert_documents(self, collection : str, data : Union[Union[Activity, User, TrackPoint], list[Union[Activity, User, TrackPoint]]]) -> bool:
        try:
            col = self.db[collection]
            if isinstance(data, dict):
                col.insert_one(data)
            else:
                col.insert_many(data)
        except Exception as e:
            logging.critical(f'An error occured in insert_documents() -> \n{e}')
            return False
        logging.info(f'Inserted data into: {collection}')
        return True
    
    def insert_user(self, user : Union[User, list[User]]) -> bool:
        return self.insert_documents('User', user)
    
    def insert_activities(self, activities : Union[Activity, list[Activity]]) -> bool:
        return self.insert_documents('Activity', activities)
    
    def insert_trackpoints(self, trackpoints : Union[TrackPoint, list[TrackPoint]]) -> bool:
        return self.insert_documents('TrackPoint', trackpoints)
    
    def drop_collection(self, collection : str) -> bool:
        try:
            collection = self.db[collection]
            collection.drop()
        except Exception as e:
            logging.critical(f'An error occured in drop_collection() -> \n{e}')
            return False
        logging.info(f'Dropped {collection}!')
        return True
