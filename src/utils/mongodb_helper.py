from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime

class MongoDBHelper:
    def __init__(self, conn_host, username, password , database_name, conn_port = 27017,):
        self.client = MongoClient(f"mongodb://{username}:{password}@{conn_host}:{conn_port}/")
        self.database = self.client[database_name]

    def get_collection(self, collection_name, create_if_not_exists = True):
        existing_collections = self.database.list_collection_names()

        if collection_name in existing_collections:
            return self.database[collection_name]
        
        else:
            if create_if_not_exists:
                
                self.database.create_collection(collection_name)

                return self.database[collection_name]
            
            return None
        
    def _read_document(self, documents):

        for document in documents:
            yield document


            
    def read_collection(self, collection_name, additional_filters = {}):
        collection = self.get_collection(collection_name)

        documents = collection.find(additional_filters)

        return [document for document in self._read_document(documents)] or None


    def insert_one(self, obj, collection_name):
        collection = self.get_collection(collection_name)
        result = collection.insert_one(obj)

        return result.inserted_id
    
    def update_document(self, collection_name, query_condition, update_data):

        try:
            # Get the collection
            collection = self.get_collection(collection_name)

            # Update a single document based on the query_condition
            result = collection.update_one(query_condition, {"$set": update_data})

            # Print the number of modified documents
            print(f"Matched {result.matched_count} document(s) and modified {result.modified_count} document(s).")
            
            return True
        
        except Exception as e:

            return (False,e)
        
    def delete_document(self,  collection_name, query_condition):
        try:
            collection = self.get_collection(collection_name)
            result = collection.delete_many(query_condition)

            return result.deleted_count
        except Exception as e:
            return (False, e)


    
    def close_connection(self):
        return self.client.close_connection()
