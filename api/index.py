import re
import time
import logging
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# ⚠️ IMPORTANT: relative import fix
from .database import db
from .config import CORS_ORIGINS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

app = FastAPI(
    title="abhigyan OSINT DB API",
    version="1.0.0",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS if CORS_ORIGINS else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Helper ──
def clean_mobile(raw: str):
    digits = re.sub(r"[^\d]", "", raw)
    if not digits:
        return None

    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]
    elif len(digits) == 13 and digits.startswith("091"):
        digits = digits[3:]

    if len(digits) == 10 and digits[0] in "6789":
        return digits

    return None


# ── Routes ──

@app.get("/")
def root():
    return {
        "status": "online",
        "name": "Phantom OSINT API",
        "endpoints": {
            "lookup": "/api/lookup?number=9876543210",
            "stats": "/api/stats"
        }
    }


@app.get("/api/lookup")
async def lookup(number: str = Query(...)):
    mobile = clean_mobile(number)

    if not mobile:
        raise HTTPException(status_code=400, detail="Invalid number")

    t_start = time.perf_counter()

    # ⚠️ SAFE DB CALL (no startup connect)
    try:
        profile = await db.deep_search(mobile)
    except Exception as e:
        return {
            "success": False,
            "error": "DB error",
            "message": str(e)
        }

    elapsed = int((time.perf_counter() - t_start) * 1000)

    profile["response_time_ms"] = elapsed

    return JSONResponse(content=profile)


@app.get("/api/stats")
async def stats():
    try:
        row_count = await db.get_row_count()
        db_size = await db.get_db_size()
    except:
        return {
            "error": "DB not connected"
        }

    return {
        "total_records": row_count,
        "database_size": db_size
    }


# ✅ VERY IMPORTANT FOR VERCEL
handler = app
