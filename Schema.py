from datetime import datetime
from typing import Any

class User(dict):
    """
    A custom dictionary for the User collection schema. Ensures consistency when inserting User documents.
    """
    KEYS = ('_id', 'has_labels', 'activities')
    def __init__(self,
                 id : str,
                 has_labels : bool,
                 activities : list = []) -> None:
        super().__setitem__('_id', id)
        super().__setitem__('has_labels', has_labels)
        super().__setitem__('activities', activities)
        
    def __setitem__(self, key : str, item : Any) -> None:
        """
        Enforces restrictions on the key formatting of the User dictionary
        """
        if key not in User.KEYS:
            raise KeyError(f"The key {key} is not defined.")
        super().__setitem__(key, item)

class ActivityDenorm(dict):
    """
    A custom dictionary type for denormalized Activity reference for TrackPoints. Mostly for consistent typing.
    """
    KEYS = ('_id', 'user')
    def __setitem__(self, key : str, item : Any) -> None:
        """
        Enforces restrictions on the key formatting of the ActivityDenorm dictionary
        """
        if key not in ActivityDenorm.KEYS:
            raise KeyError(f"The key {key} is not defined.")
        super().__setitem__(key, item)

class Activity(dict):
    """
    A custom dictionary for the Activity collection schema. Ensures consistency when inserting Activity documents.
    """
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
        """
        Enforces restrictions on the key formatting of the Activity dictionary
        """
        if key not in Activity.KEYS:
            raise KeyError(f"The key {key} is not defined.")
        super().__setitem__(key, item)

    def denorm(self) -> ActivityDenorm:
        """
        Converts the Activity to its denormalized type
        :returns: The denormalized Activity
        """
        return {key : super().__getitem__(key) for key in Activity.DENORM_KEYS}

class TrackPoint(dict):
    """
    A custom dictionary for the TrackPoint collection schema. Ensures consistency when inserting TrackPoint documents.
    """
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
        """
        Enforces restrictions on the key formatting of the TrackPoint dictionary
        """
        if key not in TrackPoint.KEYS:
            raise KeyError(f"The key {key} is not defined.")
        super().__setitem__(key, item)
