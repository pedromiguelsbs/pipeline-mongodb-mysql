from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import requests
import os

load_dotenv()

def connect_mongo(uri):
    client = MongoClient(uri, server_api=ServerApi('1'))
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
    return client

def create_and_connect_db(client, db_name):
    db = client[db_name]
    return db

def create_and_connect_collection(db, col_name):
    collection = db[col_name]
    return collection

def extract_api_data(url):
    response = requests.get(url)
    return response.json()

def insert_data(collection, data):
    collection.insert_many(data)

client = os.getenv("MONGODB_URI")
db = create_and_connect_db(client, "db_produtos")
collection = create_and_connect_collection(db, "produtos")

data = extract_api_data("https://labdados.com/produtos")
insert_data(collection, data)

client.close()