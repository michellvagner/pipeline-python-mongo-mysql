# %%
import yaml
import requests
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# %%
with open('./config.yaml', 'r', encoding='utf-8') as cfg:
    config = yaml.safe_load(cfg)

mongodb_server = config['mongodb_server']
mongodb_password = config['mongodb_password']
uri = config['uri'].replace("{mongodb_server}",mongodb_server).replace("{mongodb_password}",mongodb_password)
print(uri)

# %%
def connect_mongo(uri):
    client = MongoClient(uri, server_api=ServerApi('1'))

    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    return client

def create_connect_db(client, db_name):
    return client[db_name]

def create_connect_collection(db, col_name):
    return db[col_name]

def extract_api_data(url):
    return requests.get(url).json()

def insert_data(col, data):
    docs = col.insert_many(data)
    return len(docs)

if __name__ == "__main__":
    client = connect_mongo(uri)
    db = create_connect_db(client, "db_produtos")
    collection = create_connect_collection(db, "produtos")
    response = extract_api_data("https://labdados.com/produtos")
    docs = insert_data(collection, response)