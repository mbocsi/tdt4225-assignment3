from DbConnector import DbConnector

class Database:
    def __init__(self) -> None:
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db