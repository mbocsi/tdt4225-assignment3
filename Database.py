from DbConnector import DbConnector
from Schema import User, Activity, TrackPoint
from typing import Union
import logging

class Database:
    """
    A class for performing database operations (insertions, queries, collection creation, etc.)
    """
    def __init__(self) -> None:
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
    
    def create_collection(self, name : str) -> bool:
        """
        Creates a new collection
        :param name: The name of the new collection
        :returns: True if successful, False if not
        """
        try:
            collection = self.db.create_collection(name)
        except Exception as e:
            logging.critical(f'An error occured in create_collection() -> \n{e}')
            return False
        logging.info(f'Created a new collection: {collection}')
        return True
    
    def insert_documents(self, collection : str, data : Union[Union[Activity, User, TrackPoint], list[Union[Activity, User, TrackPoint]]]) -> bool:
        """
        Inserts documents into the database
        :param collection: The name of the collection for inserting
        :param data: The document(s) to be inserted into the collection
        :returns: True if successful, False if not
        """
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
        """
        Inserts a/multiple Users into the database
        :param user: The User document(s)
        :returns: True if successful, False if not
        """
        return self.insert_documents('User', user)
    
    def insert_activities(self, activities : Union[Activity, list[Activity]]) -> bool:
        """
        Inserts a/multiple Activities into the database
        :param activities: The Activity document(s)
        :returns: True if successful, False if not
        """
        return self.insert_documents('Activity', activities)
    
    def insert_trackpoints(self, trackpoints : Union[TrackPoint, list[TrackPoint]]) -> bool:
        """
        Inserts a/multiple TrackPoints into the database
        :param trackpoints: The TrackPoint document(s)
        :returns: True if successful, False if not
        """
        return self.insert_documents('TrackPoint', trackpoints)
    
    def drop_collection(self, collection : str) -> bool:
        """
        Drops a collection from the database
        :param collection: The name of the collection that will be dropped
        :returns: True if successful, False if not
        """
        try:
            col = self.db[collection]
            col.drop()
        except Exception as e:
            logging.critical(f'An error occured in drop_collection() -> \n{e}')
            return False
        logging.info(f'Dropped {collection}!')
        return True