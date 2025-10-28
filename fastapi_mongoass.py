from fastapi import FastAPI, HTTPException, Depends, Request, Query, Form
from fastapi.responses import JSONResponse, StreamingResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.security.api_key import APIKeyHeader
from pymongo import MongoClient, errors
from pydantic import BaseModel
from bson import ObjectId
import pandas as pd
import io
from datetime import datetime


app = FastAPI()


client = MongoClient("mongodb://localhost:27017/")
db = client["fastapi_db"]

# Handle unique index creation safely
try:
    db.students.create_index("email", unique=True)
except errors.DuplicateKeyError:
    print("⚠️ Duplicate emails exist, index not created. Clean data before retrying.")


def clean_document(doc):
    """Convert ObjectId to string for JSON serialization"""
    doc["_id"] = str(doc["_id"])
    return doc


class Student(BaseModel):
    student_id: int
    name: str
    age: int
    grade: str
    email: str


class Course(BaseModel):
    course_id: int
    course_name: str


class Enrollment(BaseModel):
    student_id: int
    course_id: int


@app.post("/students")
def create_student(student: Student):
    try:
        result = db.students.insert_one(student.model_dump())
        return {"inserted_id": str(result.inserted_id)}
    except errors.DuplicateKeyError:
        return JSONResponse(status_code=409, content={"detail": "Email already exists"})


@app.get("/students")
def get_students():
    students = list(db.students.find())
    return [clean_document(s) for s in students]


@app.get("/students/paginated")
def paginated_students(page: int = Query(1, ge=1), limit: int = Query(10, ge=1)):
    skip = (page - 1) * limit
    students = list(db.students.find().skip(skip).limit(limit))
    return [clean_document(s) for s in students]


@app.get("/students/filter")
def filter_students(
    min_age: int = Query(0, ge=0, description="Minimum age filter"),
    sort: str = Query(
        "asc", pattern="^(asc|desc)$", description="Sort order (asc/desc)"
    ),
):
    sort_order = 1 if sort == "asc" else -1
    students = list(
        db.students.find({"age": {"$gte": min_age}}).sort("age", sort_order)
    )
    return [clean_document(s) for s in students]


@app.get("/students/{student_id}")
def get_student(student_id: int):
    student = db.students.find_one({"student_id": student_id})
    return clean_document(student) if student else {"detail": "Not found"}


@app.put("/students/{student_id}")
def update_student(student_id: int, student: Student):
    update_data = student.model_dump(exclude_unset=True)
    db.students.update_one({"student_id": student_id}, {"$set": update_data})
    return {"message": "Student updated"}


@app.delete("/students/{student_id}")
def delete_student(student_id: int):
    if db.enrollments.find_one({"student_id": student_id}):
        return {"detail": "Student is enrolled in a course"}
    db.students.delete_one({"student_id": student_id})
    return {"message": "Student deleted"}


@app.post("/courses")
def create_course(course: Course):
    result = db.courses.insert_one(course.model_dump())
    return {"inserted_id": str(result.inserted_id)}


@app.get("/courses")
def get_courses():
    return [clean_document(c) for c in db.courses.find()]


@app.post("/enrollments")
def enroll_student(enrollment: Enrollment):
    result = db.enrollments.insert_one(enrollment.model_dump())
    return {"inserted_id": str(result.inserted_id)}


@app.get("/enrollments")
def get_enrollments():
    return [clean_document(e) for e in db.enrollments.find()]


@app.get("/stats/grades")
def grade_stats():
    pipeline = [{"$group": {"_id": "$grade", "count": {"$sum": 1}}}]
    result = {doc["_id"]: doc["count"] for doc in db.students.aggregate(pipeline)}
    return result


@app.get("/stats/top-courses")
def top_courses():
    pipeline = [
        {"$group": {"_id": "$course_id", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]
    return list(db.enrollments.aggregate(pipeline))


@app.post("/upload-csv")
async def upload_csv(file: bytes):
    df = pd.read_csv(io.BytesIO(file))
    db.students.insert_many(df.to_dict(orient="records"))
    return {"message": "CSV uploaded successfully"}


@app.get("/students/export")
def export_students():
    students = list(db.students.find())
    df = pd.DataFrame(students)
    df["_id"] = df["_id"].astype(str)
    stream = io.StringIO()
    df.to_csv(stream, index=False)
    response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=students.csv"
    return response


templates = Jinja2Templates(directory="templates")


@app.get("/form/student", response_class=HTMLResponse)
def student_form(request: Request):
    return templates.TemplateResponse("student_form.html", {"request": request})


@app.post("/students/form")
def submit_form(
    name: str = Form(...),
    age: int = Form(...),
    grade: str = Form(...),
    email: str = Form(...),
):
    student = {
        "student_id": int(datetime.now().timestamp()),
        "name": name,
        "age": age,
        "grade": grade,
        "email": email,
    }
    db.students.insert_one(student)
    return {"message": "Student added from form"}


api_key_header = APIKeyHeader(name="x-api-key", auto_error=False)


def verify_api_key(x_api_key: str = Depends(api_key_header)):
    if x_api_key != "secret123":
        raise HTTPException(status_code=403, detail="Forbidden")
    return x_api_key


@app.get("/secure/data")
def secure_data(api_key: str = Depends(verify_api_key)):
    return {"message": "This is a secure route"}


@app.middleware("http")
async def log_requests(request: Request, call_next):
    db.logs.insert_one(
        {
            "method": request.method,
            "path": request.url.path,
            "timestamp": datetime.utcnow(),
        }
    )
    response = await call_next(request)
    return response


@app.get("/welcome")
def welcome(name: str = Query(None)):
    if not name:
        return {"message": "Hello, guest!"}
    return {"message": f"Welcome back, {name}!"}
