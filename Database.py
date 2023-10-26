from DbConnector import DbConnector
from typing import Union, Any
import logging

class User(dict):
    KEYS = ('id', 'has_labels', 'activites')
    def __init__(self,
                 id : str,
                 has_labels : bool,
                 activites : list = []) -> None:
        self._dictionary = {'id': id,
                            'has_labels': has_labels,
                            'activities': activites}
        
    def __setitem__(self, key : str, item : Any) -> None:
        if key not in User.KEYS:
            raise KeyError(f"The key {key} is not defined.")
        self._dictionary[key] = item

    def __getitem__(self, key : str) -> Any:
        return self._dictionary[key]

class ActivityDenorm(dict):
    KEYS = ('id', 'user')
    def __setitem__(self, key : str, item : Any) -> None:
        if key not in User.KEYS:
            raise KeyError(f"The key {key} is not defined.")
        self._dictionary[key] = item

    def __getitem__(self, key : str) -> Any:
        return self._dictionary[key]

class Activity(dict):
    KEYS = ('id', 'user', 'transportation_mode', 'start_date_time', 'end_date_time', 'trackpoints')
    DENORM_KEYS = ('id', 'user')
    def __init__(self,
                 id : int,
                 user : str,
                 transportation_mode : str = None,
                 start_date_time : str = None,
                 end_date_time : str = None,
                 trackpoints : list = []) -> None:
        self._dictionary = {'id': id,
                            'user': user,
                            'transportation_mode': transportation_mode,
                            'start_date_time': start_date_time,
                            'end_date_time': end_date_time,
                            'trackpoints': trackpoints}
        
    def __setitem__(self, key : str, item : Any) -> None:
        if key not in User.KEYS:
            raise KeyError(f"The key {key} is not defined.")
        self._dictionary[key] = item

    def __getitem__(self, key : str) -> Any:
        return self._dictionary[key]
    
    def denorm(self) -> ActivityDenorm:
        return {key : self._dictionary[key] for key in Activity.DENORM_KEYS}

class TrackPoint(dict):
    KEYS = ('id', 'lat', 'lon', 'altitude', 'date_days', 'date_time', 'activity')
    def __init__(self,
                 id : int,
                 lat : float,
                 lon : float,
                 altitude : int,
                 date_days : float,
                 date_time : str,
                 activity : ActivityDenorm) -> None:
        self._dictionary = {'id': id,
                            'lat': lat,
                            'lon': lon,
                            'altitude': altitude,
                            'date_days': date_days,
                            'date_time': date_time,
                            'activity': activity}
        
    def __setitem__(self, key : str, item : Any) -> None:
        if key not in User.KEYS:
            raise KeyError(f"The key {key} is not defined.")
        self._dictionary[key] = item

    def __getitem__(self, key : str) -> Any:
        return self._dictionary[key]

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
            collection = self.db[collection]
            if isinstance(data, dict):
                collection.insert(data)
            else:
                collection.insert_many(data)
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
