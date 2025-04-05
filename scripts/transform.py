from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import pandas as pd
import os

load_dotenv()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

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

def visualize_collection(collection):
    list_collection = []
    for doc in collection.find():
        list_collection.append(doc)
    return list_collection

def rename_column(collection, old_name, new_name):
    collection.update_many({}, {"$rename": {old_name:new_name}})

def  select_category(collection, category):
    lista = []
    query = {"Categoria do Produto": category}
    for doc in collection.find(query):
        lista.append(doc)
    return lista

def make_regex(collection, query):
    lista = []
    for doc in collection.find(query):
        lista.append(doc)
    return lista

def create_dataframe(list): 
    df = pd.DataFrame(list)
    return df

def format_date(df):
    df["Data da Compra"] = pd.to_datetime(df["Data da Compra"], format="%d/%m/%Y")
    df["Data da Compra"] = df["Data da Compra"].dt.strftime("%Y-%m-%d")

def save_csv(df, filename):
    path = os.path.join(DATA_DIR, filename)
    df.to_csv(path, index=False)

client = os.getenv("MONGODB_URI")
db = create_and_connect_db(client, "db_produtos")
collection = create_and_connect_collection(db, "produtos")

list_collection = visualize_collection(collection)
rename_column(collection, "lat", "Latitude")
rename_column(collection, "lon", "Longitude")

produtos_livros = select_category(collection, "livros")
produtos_a_partir_de_2021 = make_regex(collection, {"Data da Compra": {"$regex": "/202[1-9]"}})

df1 = create_dataframe(produtos_livros)
df2 = create_dataframe(produtos_a_partir_de_2021)

format_date(df1)
format_date(df2)

save_csv(df1, "tabela_livros.csv")
save_csv(df2, "tabela_produtos_2021_em_diante.csv")

client.close()