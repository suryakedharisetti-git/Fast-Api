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

app =FastAPI()

@app.on_event("startup")
def startup_db_client():
    global client, db
    client = MongoClient("mongodb://localhost:27017")
    db = client["school_db"]
    print(" Connected to MongoDB")


@app.on_event("shutdown")
def shutdown_db_client():
    global client
    if client:
        client.close()
        print(" MongoDB connection closed")


# ---------- MODELS ----------
class Student(BaseModel):
    student_id: int
    name: str
    age: int
    grade: str
    email: str


class Course(BaseModel):
    course_id: int
    course_name: str
    instructor: str


class Enrollment(BaseModel):
    student_id: int
    course_id: int

def fix_id(doc):
    if doc and "_id" in doc:
        doc["_id"] = str(doc["_id"])
    return doc


def clean_document(doc):
    if "_id" in doc:
        doc["_id"] = str(doc["_id"])
    for k, v in doc.items():
        if isinstance(v, float) and math.isnan(v):
            doc[k] = None
        elif isinstance(v, ObjectId):
            doc[k] = str(v)
    return doc


def get_next_student_id():
    last_student = db.students.find_one(sort=[("student_id", -1)])
    if last_student:
        return last_student["student_id"] + 1
    return 1


def get_next_course_id():
    last_course = db.courses.find_one(sort=[("course_id", -1)])
    if last_course:
        return last_course["course_id"] + 1
    return 1


# ---------- ROOT ----------
@app.get("/")
def read_root():
    return {"message": "MongoDB connected!"}


# ---------- STATIC FILES ----------
# app.mount("/static", StaticFiles(directory="static"), name="static")


# ---------- STUDENTS ----------
@app.post("/students")
def create_student(student: Student):
    student_dict = student.model_dump()
    result = db.students.insert_one(student_dict)
    return {"inserted_id": str(result.inserted_id)}


@app.get("/students")
def get_students():
    students = list(db.students.find())
    return [clean_document(s) for s in students]


@app.get("/students/search")
def search_students(name: str):
    students = list(db.students.find({"name": {"$regex": name, "$options": "i"}}))
    return [clean_document(s) for s in students]


@app.get("/students/paginated")
def paginated_students(page: int = Query(1, ge=1), limit: int = Query(10, ge=1)):
    skip = (page - 1) * limit
    students = list(db.students.find().skip(skip).limit(limit))
    return [clean_document(s) for s in students]


@app.get("/students/filter")
def filter_students(
    min_age: int = Query(0, ge=0, description="Minimum age filter"),
    sort: str = Query("asc", regex="^(asc|desc)$", description="Sort order (asc/desc)"),
):
    sort_order = 1 if sort == "asc" else -1
    students = list(
        db.students.find({"age": {"$gte": min_age}}).sort("age", sort_order)
    )
    return [clean_document(s) for s in students]

#  Put HTML endpoint ABOVE the dynamic {student_id}
@app.get("/students/html", response_class=HTMLResponse)
def students_html(request: Request):
    students = list(db.students.find())
    return templates.TemplateResponse(
        "students_form.html", {"request": request, "students": students}
    )


@app.get("/students/{student_id}")
def get_student(student_id: int):
    student = db.students.find_one({"student_id": student_id})
    if not student:
        raise HTTPException(status_code=404, detail="Student not found")
    return fix_id(student)


@app.put("/students/{student_id}")
def update_student(student_id: int, student: Student):
    update_data = student.model_dump(exclude_unset=True)
    result = db.students.update_one({"student_id": student_id}, {"$set": update_data})
    return {"updated_count": result.modified_count}


@app.delete("/students/{student_id}")
def delete_student(student_id: int):
    enrollment = db.enrollments.find_one({"student_id": student_id})
    if enrollment:
        return {
            "deleted_count": 0,
            "message": "Cannot delete student: already enrolled in a course",
        }
    result = db.students.delete_one({"student_id": student_id})
    return {"deleted_count": result.deleted_count}


# ---------- COURSES ----------
@app.post("/courses")
def create_course(course: Course):
    course_doc = course.model_dump()
    course_doc["course_id"] = get_next_course_id()
    result = db.courses.insert_one(course_doc)
    return {"inserted_id": course_doc["course_id"]}


@app.get("/courses")
def get_courses():
    courses = list(db.courses.find())
    return [fix_id(c) for c in courses]


@app.get("/courses/{course_id}/students")
def get_students_in_course(course_id: int):
    pipeline = [
        {"$match": {"course_id": course_id}},
        {
            "$lookup": {
                "from": "students",
                "localField": "student_id",
                "foreignField": "student_id",
                "as": "student_info",
            }
        },
        {"$unwind": "$student_info"},
        {"$replaceRoot": {"newRoot": "$student_info"}},
    ]
    students = list(db.enrollments.aggregate(pipeline))
    if not students:
        raise HTTPException(status_code=404, detail="No students found for this course")
    return [fix_id(s) for s in students]


# ---------- STATS ----------
@app.get("/stats/grades")
def get_grade_stats():
    pipeline = [{"$group": {"_id": "$grade", "count": {"$sum": 1}}}]
    results = list(db.students.aggregate(pipeline))
    return {r["_id"]: r["count"] for r in results if r["_id"]}


@app.get("/stats/top-courses")
def get_top_courses():
    pipeline = [
        {"$group": {"_id": "$course_id", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {
            "$lookup": {
                "from": "courses",
                "localField": "_id",
                "foreignField": "course_id",
                "as": "course_info",
            }
        },
        {"$unwind": "$course_info"},
        {
            "$project": {
                "_id": 0,
                "course_id": "$_id",
                "course_name": "$course_info.course_name",
                "enrollments": "$count",
            }
        },
    ]
    return list(db.enrollments.aggregate(pipeline))

@app.post("/enrollments")
def enroll_student(enrollment: Enrollment):
    if not db.students.find_one({"student_id": enrollment.student_id}):
        raise HTTPException(status_code=404, detail="Student not found")
    if not db.courses.find_one({"course_id": enrollment.course_id}):
        raise HTTPException(status_code=404, detail="Course not found")
    enrollment_doc = enrollment.model_dump()
    result = db.enrollments.insert_one(enrollment_doc)
    return {"inserted_id": str(result.inserted_id)}


@app.get("/enrollments")
def get_enrollments():
    enrollments = list(db.enrollments.find())
    return [fix_id(e) for e in enrollments]


# ---------- DATABASES ----------
@app.get("/databases")
def list_databases():
    return {"databases": client.list_database_names()}


# ---------- UPLOAD CSV ----------
@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    df = pd.read_csv(file.file)
    records = df.to_dict(orient="records")
    if records:
        db.students.insert_many(records)
    return {"inserted_count": len(records)}


# ---------- FORM ----------

@app.get("/form/student")
def student_form(request: Request):
    return templates.TemplateResponse("student_form.html", {"request": request})


@app.post("/form/student")
def submit_student(
    name: str = Form(...),
    age: int = Form(...),
    grade: str = Form(...),
    email: str = Form(...),
):
    student = {
        "student_id": get_next_student_id(),
        "name": name,
        "age": age,
        "grade": grade,
        "email": email,
    }
    db.students.insert_one(student)
    return RedirectResponse(url="/students", status_code=303)


def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API Key")


# Example secure route
@app.get("/secure/students", dependencies=[Depends(verify_api_key)])
def secure_get_students():
    students = list(db.students.find({}, {"_id": 0}))
    return students


@app.middleware("http")
async def log_requests(request: Request, call_next):
    log_entry = {
        "method": request.method,
        "path": request.url.path,
        "timestamp": datetime.utcnow(),
    }
    db.logs.insert_one(log_entry)  # save log in MongoDB

    response = await call_next(request)
    return response


@app.post("/set-name")
def set_name(name: str):
    response = HTMLResponse(content=f"Name set to {name}")
    response.set_cookie(key="username", value=name)
    return response


@app.get("/welcome")
def welcome(request: Request):
    username = request.cookies.get("username")
    if not username:
        return {"message": "Hello, guest! Please set your name first."}
    return {"message": f"Welcome back, {username}!"}


@app.exception_handler(DuplicateKeyError)
async def duplicate_key_handler(request: Request, exc: DuplicateKeyError):
    return JSONResponse(
        status_code=409,
        content={
            "error": "Duplicate key error. A record with this value already exists."
        },
    )
    