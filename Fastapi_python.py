from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import pandas as pd

app = FastAPI()

# Load CSVs
students_df = pd.read_csv("./students.csv")
courses_df = pd.read_csv("./courses.csv")
enrollments_df = pd.read_csv("./enrollments.csv")

@app.get("/")
def home():
    return {"message": "Welcome! Use /students, /courses, or /enrollments"}

#1
@app.get("/students")
def get_students():
    return students_df.to_dict()

@app.get("/courses")
def get_courses():
    return courses_df.to_dict()

@app.get("/enrollments")
def get_enrollments():
    return enrollments_df.to_dict()


#4
@app.get("/students_table", response_class=HTMLResponse)
def get_students_table():
    html_table = students_df.to_html(index=False)
    return f"""
    <html>
        <head>
            <title>Students Table</title>
        </head>
        <body>
            <h2>Students Data</h2>
            {html_table}
        </body>
    </html>
    """

#5
@app.get("/students_in_course/{course_id}", response_class=HTMLResponse)
def get_students_in_course(course_id: int):
    # Join enrollments with students
    merged_df = enrollments_df.merge(students_df, on="student_id")
    filtered_df = merged_df[merged_df["course_id"] == course_id]

    if filtered_df.empty:
        return f"<h3>No students found for course_id {course_id}</h3>"

    html_table = filtered_df[["student_id", "name", "age", "grade", "email"]].to_html(index=False)

    # Get course name for heading
    course_name = courses_df[courses_df["course_id"] == course_id]["course_name"].values
    course_display = course_name[0] if len(course_name) > 0 else f"Course {course_id}"

    return f"""
    <html>
        <head><title>Students in {course_display}</title></head>
        <body>
            <h2>Students enrolled in {course_display} (Course ID: {course_id})</h2>
            {html_table}
        </body>
    </html>
    """


#9
@app.get("/students_not_enrolled", response_class=HTMLResponse)
def get_students_not_enrolled():
    # Find student_ids in enrollments
    enrolled_ids = set(enrollments_df["student_id"].unique())

    # Filter students not in enrolled_ids
    not_enrolled_df = students_df[~students_df["student_id"].isin(enrolled_ids)]

    if not_enrolled_df.empty:
        return "<h3>All students are enrolled in at least one course.</h3>"

    html_table = not_enrolled_df.to_html(index=False)

    return f"""
    <html>
        <head><title>Students Not Enrolled</title></head>
        <body>
            <h2>Students not enrolled in any course</h2>
            {html_table}
        </body>
    </html>
    """

#10
@app.get("/students_courses", response_class=HTMLResponse)
def get_students_courses():
    merged_df = (
        enrollments_df
        .merge(students_df, on="student_id")   # Join with students
        .merge(courses_df, on="course_id")     # Join with courses
    )

    if merged_df.empty:
        return "<h3>No enrollments found.</h3>"

    # Keep only useful columns
    result_df = merged_df[["student_id", "name", "course_id", "course_name", "instructor"]]

    html_table = result_df.to_html(index=False)

    return f"""
    <html>
        <head><title>Students & Courses</title></head>
        <body>
            <h2>Students and Their Enrolled Courses</h2>
            {html_table}
        </body>
    </html>
    """

#11
@app.get("/search_student/{student_name}", response_class=HTMLResponse)
def search_student(student_name: str):
    # Case-insensitive search
    filtered_df = students_df[students_df["name"].str.contains(student_name, case=False, na=False)]

    if filtered_df.empty:
        return f"<h3>No student found with name containing '{student_name}'</h3>"

    html_table = filtered_df.to_html(index=False)

    return f"""
    <html>
        <head><title>Search Student</title></head>
        <body>
            <h2>Search results for: {student_name}</h2>
            {html_table}
        </body>
    </html>
    """

#13. Count students per course
@app.get("/students_per_course", response_class=HTMLResponse)
def students_per_course():
    # Count enrollments per course
    count_df = enrollments_df.groupby("course_id").size().reset_index(name="student_count")

    # Join with courses to get course names
    merged_df = count_df.merge(courses_df, on="course_id")

    # Sort in descending order
    sorted_df = merged_df.sort_values(by="student_count", ascending=False)

    html_table = sorted_df[["course_id", "course_name", "instructor", "student_count"]].to_html(index=False)

    return f"""
    <html>
        <head><title>Students per Course</title></head>
        <body>
            <h2>Number of Students Enrolled per Course (Descending)</h2>
            {html_table}
        </body>
    </html>
    """

#14
#14. Fetch students by course name
@app.get("/students_by_course/{course_name}", response_class=HTMLResponse)
def students_by_course(course_name: str):
    # Match course_id from course name (case-insensitive)
    course_match = courses_df[courses_df["course_name"].str.lower() == course_name.lower()]

    if course_match.empty:
        return f"<h3>No course found with name '{course_name}'</h3>"

    course_id = int(course_match.iloc[0]["course_id"])
    course_display = course_match.iloc[0]["course_name"]

    # Join enrollments with students
    merged_df = enrollments_df.merge(students_df, on="student_id")

    # Filter students in this course
    filtered_df = merged_df[merged_df["course_id"] == course_id]

    if filtered_df.empty:
        return f"<h3>No students enrolled in {course_display}</h3>"

    # Show student name + grade
    result_df = filtered_df[["student_id", "name", "grade"]]

    html_table = result_df.to_html(index=False)

    return f"""
    <html>
        <head><title>Students in {course_display}</title></head>
        <body>
            <h2>Students enrolled in {course_display}</h2>
            {html_table}
        </body>
    </html>
    """