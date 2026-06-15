# pip install fastapi uvicorn pymongo
# Run: python -m uvicorn api:app --host 0.0.0.0 --port 8000 --reload

from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from pymongo import MongoClient
import os

# Δημιουργία FastAPI εφαρμογής
app = FastAPI(title="Traffic Stats API", version="1.0.0")

# Σύνδεση στη MongoDB — διαβάζει MONGO_URI από environment variable, αλλιώς localhost
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
client = MongoClient(MONGO_URI)
db = client["traffic"]


# Root endpoint — ανακατευθύνει αυτόματα στο Swagger UI (/docs)
@app.get("/")
def root():
    return RedirectResponse(url="/docs")


# Επιστρέφει τα τελευταία aggregated stats από τη MongoDB (time, link, vcount, vspeed)
@app.get("/stats")
def get_all_stats(limit: int = 100):
    """Return the latest aggregated stats (time, link, vcount, vspeed)."""
    docs = list(db["stats"].find({}, {"_id": 0}).limit(limit))
    if not docs:
        raise HTTPException(status_code=404, detail="No stats found")
    return docs


# Επιστρέφει τις 5 πιο συμφορημένες ακμές με MongoDB aggregation pipeline
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
        {"$sort": {"avg_density": -1}},   # ταξινόμηση από πιο συμφορημένη
        {"$limit": 5},
        {"$project": {"_id": 0, "link": "$_id",
                      "avg_density": 1, "avg_speed": 1, "max_vehicles": 1}}
    ]
    result = list(db["stats"].aggregate(pipeline))
    if not result:
        raise HTTPException(status_code=404, detail="No stats found")
    return result


# Επιστρέφει μέση ταχύτητα ανά ακμή σε όλο το διάστημα εξομοίωσης
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


# Επιστρέφει τα τελευταία raw δεδομένα θέσης οχημάτων από τη MongoDB
@app.get("/raw")
def get_raw_data(limit: int = 50):
    """Return latest raw vehicle position records."""
    docs = list(db["raw_data"].find({}, {"_id": 0}).limit(limit))
    if not docs:
        raise HTTPException(status_code=404, detail="No raw data found")
    return docs
