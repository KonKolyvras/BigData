# pip install fastapi uvicorn pymongo
# Run: uvicorn api:app --host 0.0.0.0 --port 8000 --reload

from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from pymongo import MongoClient
import os

app = FastAPI(title="Traffic Stats API", version="1.0.0")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
client = MongoClient(MONGO_URI)
db = client["traffic"]


@app.get("/")
def root():
    return RedirectResponse(url="/docs")


@app.get("/stats")
def get_all_stats(limit: int = 100):
    """Return the latest aggregated stats (time, link, vcount, vspeed)."""
    docs = list(db["stats"].find({}, {"_id": 0}).limit(limit))
    if not docs:
        raise HTTPException(status_code=404, detail="No stats found")
    return docs


@app.get("/stats/top5")
def get_top5_congested():
    """Return the top 5 most congested links by average vehicle count."""
    pipeline = [
        {"$group": {
            "_id": "$link",
            "avg_density": {"$avg": "$vcount"},
            "avg_speed":   {"$avg": "$vspeed"},
            "max_vehicles": {"$max": "$vcount"}
        }},
        {"$sort": {"avg_density": -1}},
        {"$limit": 5},
        {"$project": {"_id": 0, "link": "$_id",
                      "avg_density": 1, "avg_speed": 1, "max_vehicles": 1}}
    ]
    result = list(db["stats"].aggregate(pipeline))
    if not result:
        raise HTTPException(status_code=404, detail="No stats found")
    return result


@app.get("/stats/avg-speed")
def get_avg_speed_per_link():
    """Return average speed per link across all time steps, sorted descending."""
    pipeline = [
        {"$group": {
            "_id": "$link",
            "avg_speed":     {"$avg": "$vspeed"},
            "total_records": {"$sum": "$vcount"}
        }},
        {"$sort": {"avg_speed": -1}},
        {"$project": {"_id": 0, "link": "$_id",
                      "avg_speed": 1, "total_records": 1}}
    ]
    result = list(db["stats"].aggregate(pipeline))
    if not result:
        raise HTTPException(status_code=404, detail="No stats found")
    return result


@app.get("/raw")
def get_raw_data(limit: int = 50):
    """Return latest raw vehicle position records."""
    docs = list(db["raw_data"].find({}, {"_id": 0}).limit(limit))
    if not docs:
        raise HTTPException(status_code=404, detail="No raw data found")
    return docs
