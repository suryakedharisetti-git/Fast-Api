from fastapi import (
    FastAPI,
    HTTPException,
    UploadFile,
    File,
    Request,
    Form,
    Depends,
    Header,
    Query,
)
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from bson import ObjectId
from pydantic import BaseModel
import math
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from datetime import datetime

@app.on_event("startup")
def startup_db_client():
    global client, db
    client = MongoClient("mongodb://localhost:27017")
    db = client["school_db"]
    print(" Connected to MongoDB")