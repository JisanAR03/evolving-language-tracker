import os
from fastapi import FastAPI
from dotenv import load_dotenv
from pymongo import MongoClient
from sentence_transformers import SentenceTransformer

load_dotenv()
client = MongoClient(os.getenv("MONGODB_URI"))
col    = client.slang_db.slang_terms
model  = SentenceTransformer("all-MiniLM-L6-v2")

app = FastAPI(title="Evolving Language Tracker")

@app.get("/search")
async def search(term: str, k: int = 5):
    q_emb = model.encode(term).tolist()
    pipeline = [
        {
            "$search": {
                "vector": {
                    "path": "embedding",
                    "queryVector": q_emb,
                    "k": k
                }
            }
        },
        {"$limit": k}
    ]
    hits = list(col.aggregate(pipeline))
    return [
        {"term": h["term"], "year": h["year"], "examples": h["examples"]}
        for h in hits
    ]
