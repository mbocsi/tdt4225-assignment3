from DbConnector import DbConnector
from typing import Union
import logging

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
    
    def insert_documents(self, collection : str, data : Union[dict, list[dict]]) -> bool:
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
    
    def drop_collection(self, collection : str) -> bool:
        try:
            collection = self.db[collection]
            collection.drop()
        except Exception as e:
            logging.critical(f'An error occured in drop_collection() -> \n{e}')
            return False
        logging.info(f'Dropped {collection}!')
        return True
