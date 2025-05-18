import os
from dotenv import load_dotenv
from pymongo import MongoClient
from sentence_transformers import SentenceTransformer

load_dotenv()
client = MongoClient(os.getenv("MONGODB_URI"))
col    = client.slang_db.slang_terms
model  = SentenceTransformer("all-MiniLM-L6-v2")

samples = [
    {"term": "lit",      "year": 2015, "examples": ["This party is lit!"]},
    {"term": "on fleek", "year": 2016, "examples": ["Eyebrows on fleek."]}
]

for doc in samples:
    doc["embedding"] = model.encode(" ".join(doc["examples"])).tolist()
    col.insert_one(doc)

print("Seeded sample docs ✔")
