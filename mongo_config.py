from pymongo import MongoClient
from application_config import app_config 
def get_mongo_clients(): 
    src_client = MongoClient(app_config.DEV_URI) 
    dst_client = MongoClient(app_config.LOCAL_URI) 
    return src_client, dst_client