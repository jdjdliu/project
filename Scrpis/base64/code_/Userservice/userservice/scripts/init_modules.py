import json

from sdk.db.mongo import MongoClient

with open("./modules.json", "r", encoding="UTF-8") as f:
    data = json.loads(f.read())


conn = MongoClient.get_collection("module_metadata")

conn.remove({})
conn.insert_many(data)
