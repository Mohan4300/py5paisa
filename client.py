from py5paisa import FivePaisaClient
from dotenv import load_dotenv
import os
import json
from pymongo import MongoClient
load_dotenv()

client = MongoClient(os.getenv("MONGODB_URL"))
db = client.py5paisa
stock_collection = db.stock_collection
cred = {
    "APP_NAME": os.getenv("APP_NAME"),
    "APP_SOURCE": os.getenv("APP_SOURCE"),
    "USER_ID": os.getenv("USER_ID"),
    "PASSWORD": os.getenv("PASSWORD"),
    "USER_KEY": os.getenv("USER_KEY"),
    "ENCRYPTION_KEY": os.getenv("ENCRYPTION_KEY")
}

py5paisa_client = FivePaisaClient(email=os.getenv("EMAIL"), passwd=os.getenv("FPAISA_PASSWORD"), dob=os.getenv("DOB"),
                                  cred=cred)

py5paisa_client.login()

df = client.historical_data('B', 'C', 500325, '60m', '2022-10-02', '2022-10-04')
print(df)

df = py5paisa_client.historical_data('B', 'C', 500112, '1d', '2021-04-21', '2022-10-04')
print(df.head())
print(df.to_csv("sbin.csv"))
print(stock_collection.insert_many(df.to_dict('records')))
print(type(df))

req_list = [
    {"Exch": "B", "ExchType": "C", "ScripCode": 500112},
]

req_data = py5paisa_client.Request_Feed('mf', 's', req_list)


def on_message(ws, message):
    formated_message = json.loads(message)
    stock_collection.insert_many(formated_message)


py5paisa_client.connect(req_data)
py5paisa_client.receive_data(on_message)
