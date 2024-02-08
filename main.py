import os
import aiohttp
import asyncio
from datetime import datetime, timedelta, timezone
from dateutil import parser
import json

import requests

import time

import re

from canvasapi import Canvas
from gradescope.scope import GSConnector  # Assuming this is correctly implemented

from concurrent.futures import ThreadPoolExecutor

# Load environment variables
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
CANVAS_API_URL = "https://bruinlearn.ucla.edu"
CANVAS_API_KEY = os.getenv("CANVAS_API_KEY")
CANVAS_ID = os.getenv("CANVAS_ID")
GS_EMAIL = os.getenv("GS_EMAIL")
GS_PASSWORD = os.getenv("GS_PASSWORD")
COURSES_DATABASE_ID = os.getenv("COURSES_DATABASE_ID")

ARCHIVE_LIMIT = int(os.getenv("ARCHIVE_LIMIT"))

# Initialize APIs
canvas = Canvas(CANVAS_API_URL, CANVAS_API_KEY)
gs_connector = GSConnector(GS_EMAIL, GS_PASSWORD)

accessible_courses_cache = []

notion_query_cache = {}

notion_headers_cache = None


async def log(message):
    print(f"{datetime.now().isoformat()}: {message}")


async def str_to_datetime(date_str):
    return parser.isoparse(date_str)


async def is_within_timeframe(due_date, days_after=ARCHIVE_LIMIT):
    if isinstance(due_date, str):
        due_date = await str_to_datetime(due_date)

    now = datetime.now(timezone.utc)
    return due_date > now or (now - due_date).days <= days_after


async def get_notion_headers():
    global notion_headers_cache
    if notion_headers_cache is None:
        notion_headers_cache = {
            "Authorization": f"Bearer {NOTION_TOKEN}",
            "Notion-Version": "2021-08-16",
            "Content-Type": "application/json",
        }
    return notion_headers_cache


def get_optimal_worker_count():
    lambda_memory = os.environ.get(
        "AWS_LAMBDA_FUNCTION_MEMORY_SIZE", 128
    )  # Default to 128 MB if not set
    # Simple heuristic: 1 worker per 256 MB of Lambda memory
    return max(1, int(lambda_memory) // 256)


def cache_accessible_courses():
    global accessible_courses_cache
    try:
        courses = canvas.get_courses()
        accessible_courses_cache = [
            course.id for course in courses if course.access_restricted_by_date == False
        ]
    except Exception as e:
        print(f"Error caching accessible courses: {e}")


def safe_get_course_name(course):
    """Safely get the name of a course, with enhanced error handling and logging."""
    try:
        return course.name
    except AttributeError:
        try:
            # Attempt to log a basic identifier using known safe attributes
            course_id = getattr(course, "id", "Unknown ID")
            print(f"Failed to access 'name' for course with ID: {course_id}")
        except Exception as e:
            # Fallback if even the 'id' attribute is inaccessible
            print(f"Exception when accessing course attributes: {e}")
        return "Unknown Course"


async def process_canvas_course(course, session):
    course_name = safe_get_course_name(course)  # Extract course name
    print(f"-> Processing course: {course_name}")
    try:
        assignments = course.get_assignments()
        for assignment in assignments:
            await process_assignment(
                assignment, course_name, course.id, session
            )  # Notice course.id is passed
    except Exception as e:
        print(f"Failed to process assignments for {course_name}: {e}")


# Make sure process_assignment is expecting course_name
async def process_assignment(assignment, course_name, course_id, session):
    assignment_name = (
        assignment.name.strip()
    )  # Trim whitespace from the assignment name
    due_date = assignment.due_at or "No Due Date"

    # Check if the assignment is within the right timeframe
    if not await is_within_timeframe(due_date):
        print(
            f"Canvas assignment {assignment_name} in {course_name} is not within the timeframe."
        )
        return  # Skip further processing for this assignment

    # Check if the assignment has been submitted
    submitted = await is_assignment_submitted_canvas(
        course_id, assignment.id, CANVAS_ID, session
    )

    # Check for existing assignment to avoid duplication
    existing_assignment = await find_existing_assignment(
        assignment_name, course_name, session
    )
    if existing_assignment:
        page_id = existing_assignment.get("id")
        print(f"Updating existing assignment in Notion: {assignment_name}")
        await create_or_update_notion_page(
            assignment_name, course_name, due_date, submitted, session, page_id=page_id
        )
    else:
        print(f"Creating new assignment in Notion: {assignment_name}")
        await create_or_update_notion_page(
            assignment_name, course_name, due_date, submitted, session
        )


async def process_canvas_courses():
    loop = asyncio.get_event_loop()
    courses = await loop.run_in_executor(None, canvas.get_courses)

    async with aiohttp.ClientSession() as session:
        tasks = [process_canvas_course(course, session) for course in courses]
        await asyncio.gather(*tasks)


async def process_gradescope_assignment(assignment, session):
    assignment_name = assignment.name
    course_name = assignment.course  # Directly use it as it's already a string
    due_date = assignment.close_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    submitted = is_assignment_submitted_gradescope(assignment)

    # First, check if the assignment is within the right timeframe
    if not await is_within_timeframe(due_date):
        print(
            f"Gradescope assignment {assignment_name} in {course_name} is not within the timeframe."
        )
        return  # Skip further processing for this assignment

    # Check for existing assignment to avoid duplication
    existing_assignment = await find_existing_assignment(
        assignment_name, course_name, session
    )
    if existing_assignment:
        print(
            f"Skipping creation of duplicate Gradescope assignment: {assignment_name}"
        )
        return

    print(
        f"Adding Gradescope assignment to Notion: {assignment_name} for {course_name}"
    )
    # If the assignment passes all checks, proceed to create or update the Notion page
    await create_or_update_notion_page(
        assignment_name, course_name, due_date, completed=submitted, session=session
    )


async def process_gradescope_assignments():
    async with aiohttp.ClientSession() as session:
        assignments = gs_connector.get_all_assignments()
        print(
            f"-- Successfully retrieved {len(assignments)} assignments from Gradescope"
        )
        await asyncio.gather(
            *(
                process_gradescope_assignment(assignment, session)
                for assignment in assignments
            )
        )


def is_assignment_submitted_gradescope(assignment):
    """Check if a Gradescope assignment is submitted based on its status."""
    status = assignment.status
    # Assuming 'submitted' and 'submitted-late' as indicators of submission
    return status in ["Submitted", "Submitted-Late"]


async def is_assignment_submitted_canvas(course_id, assignment_id, user_id, session):
    url = f"https://bruinlearn.ucla.edu/api/v1/courses/{course_id}/assignments/{assignment_id}/submissions/{user_id}"
    headers = {"Authorization": f"Bearer {CANVAS_API_KEY}"}

    async with session.get(url, headers=headers) as response:
        data = (
            await response.json()
        )  # Retrieve the response data immediately after the API call
        print(
            f"Canvas submission data for assignment {assignment_id}: {data}"
        )  # Log the response data

        if response.status == 200:
            # Assuming the response includes a 'workflow_state' key to check submission status
            return data.get("workflow_state") in ["submitted", "graded"]
        else:
            # Handle error or unexpected response
            print(f"Failed to check submission status: {response.status}")
            return False


async def mark_as_archived(page_id, session):
    """Marks a Notion page as 'Archived' by updating its status."""
    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {
        "properties": {
            "Status": {  # Ensure this matches your Notion setup
                "select": {"name": "Archived"}
            }
        }
    }
    headers = (
        await get_notion_headers()
    )  # Ensure this function correctly sets Authorization and Notion-Version headers

    async with session.patch(url, headers=headers, json=payload) as response:
        if response.status == 200:
            print(f"Assignment with ID {page_id} marked as Archived.")
        else:
            print(
                f"Failed to archive assignment {page_id}. Error: {await response.text()}"
            )


async def archive_old_assignments():
    """Archives assignments in Notion that are past their due date and defaults to 'Active' status if not set."""
    now = datetime.now(timezone.utc)
    assignments = (
        await query_notion_database()
    )  # Ensure this queries all relevant assignments

    async with aiohttp.ClientSession() as session:
        for assignment in assignments:
            properties = assignment.get("properties", {})
            due_date_info = properties.get("Date", {}).get("date")
            status_info = properties.get("Status", {}).get("select")
            assignment_id = assignment.get("id", "Unknown ID")

            if not due_date_info:
                print(f"Skipping assignment {assignment_id} due to missing 'Date'.")
                continue

            due_date_str = due_date_info.get("start")

            # Adjusted handling for None status_info
            status = (
                "Active" if status_info is None else status_info.get("name", "Active")
            )

            if due_date_str:
                due_date = await str_to_datetime(due_date_str)
                if (now - due_date).days > ARCHIVE_LIMIT and status != "Archived":
                    await mark_as_archived(assignment_id, session)
                elif status != "Archived":
                    # Ensure it's marked as 'Active' if not already set or status_info was None
                    await ensure_status_active(assignment_id, session, status)
            else:
                print(f"Assignment {assignment_id} does not have a valid due date.")


async def ensure_status_active(assignment_id, session, current_status):
    """Ensures the assignment status is set to 'Active' if it's not already set or is None."""
    if current_status != "Active":
        print(f"Setting assignment {assignment_id} status to 'Active'.")
        await update_assignment_status(assignment_id, "Active", session)
    else:
        print(f"Assignment {assignment_id} already set to 'Active'.")


async def update_assignment_status(assignment_id, status, session):
    """Updates the assignment's status in Notion."""
    url = f"https://api.notion.com/v1/pages/{assignment_id}"
    headers = await get_notion_headers()
    payload = {"properties": {"Status": {"select": {"name": status}}}}

    async with session.patch(url, headers=headers, json=payload) as response:
        if response.status == 200:
            print(f"Assignment {assignment_id} status updated to {status}.")
        else:
            print(
                f"Failed to update assignment {assignment_id} status. Error: {await response.text()}"
            )


def map_notion_result_to_data(result):
    """Maps Notion API result to a Python dictionary."""
    data_id = result["id"]
    properties = result["properties"]
    class_name = (
        properties.get("Class", {})
        .get("rich_text", [{}])[0]
        .get("text", {})
        .get("content", "N/A")
    )
    name = (
        properties.get("Name", {})
        .get("title", [{}])[0]
        .get("text", {})
        .get("content", "N/A")
    )
    date = properties.get("Date", {}).get("date", {}).get("start", "N/A")
    completed = properties.get("Completed", {}).get("checkbox", False)
    return {
        "class": class_name,
        "name": name,
        "date": date,
        "completed": completed,
        "data_id": data_id,
    }


async def query_notion_database(
    database_id=NOTION_DATABASE_ID, filter_name=None, class_id=None, session=None
):
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    headers = await get_notion_headers()
    json_body = {"filter": {"and": []}}

    if filter_name:
        json_body["filter"]["and"].append(
            {"property": "Name", "text": {"equals": filter_name}}
        )
    if class_id:
        json_body["filter"]["and"].append(
            {"property": "Class", "relation": {"contains": class_id}}
        )
    if not json_body["filter"]["and"]:
        del json_body["filter"]

    if session is None:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=json_body) as response:
                if response.status == 200:
                    results = await response.json()
                    return results.get("results", [])
                else:
                    print(
                        f"Failed to query Notion database. Status code: {response.status}, Response: {await response.text()}"
                    )
                    return []
    else:
        async with session.post(url, headers=headers, json=json_body) as response:
            if response.status == 200:
                results = await response.json()
                return results.get("results", [])
            else:
                print(
                    f"Failed to query Notion database. Status code: {response.status}, Response: {await response.text()}"
                )
                return []


async def find_course_by_code(class_code):
    url = f"https://api.notion.com/v1/databases/{COURSES_DATABASE_ID}/query"
    headers = await get_notion_headers()
    json_body = {"filter": {"property": "Code", "text": {"equals": class_code}}}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=json_body) as response:
            if response.status == 200:
                courses = await response.json()
                return (
                    courses.get("results", [])[0]["id"]
                    if courses.get("results", [])
                    else None
                )
            else:
                print(
                    f"Failed to query Notion database for class code {class_code}. Status: {response.status}, Response: {await response.text()}"
                )
                return None


async def create_or_update_notion_page(
    name, class_name, date, completed, session, page_id=None
):
    class_page_id = await find_class_page_id_by_class_code(class_name, session)
    if class_page_id is None:
        print(f"Class page ID not found for {class_name}")
        return

    status = (
        "Archived" if completed and not await is_within_timeframe(date) else "Active"
    )
    headers = await get_notion_headers()

    if page_id:
        # Update existing page
        url = f"https://api.notion.com/v1/pages/{page_id}"
        payload = {
            "properties": {
                "Name": {"title": [{"text": {"content": name}}]},
                "Class": {"relation": [{"id": class_page_id}]},
                "Date": {"date": {"start": date}},
                "Completed": {"checkbox": completed},
                "Status": {"select": {"name": status}},
            }
        }
        method = session.patch
    else:
        # Create new page
        url = "https://api.notion.com/v1/pages"
        payload = {
            "parent": {"database_id": NOTION_DATABASE_ID},
            "properties": {
                "Name": {"title": [{"text": {"content": name}}]},
                "Class": {"relation": [{"id": class_page_id}]},
                "Date": {"date": {"start": date}},
                "Completed": {"checkbox": completed},
                "Status": {"select": {"name": status}},
            },
        }
        method = session.post

    async with method(url, headers=headers, json=payload) as response:
        if response.status in [
            200,
            201,
            204,
        ]:  # Including 204 for successful PATCH requests
            action = "Updated" if page_id else "Created"
            print(f"{action} Notion page successfully for {name} with status {status}")
        else:
            action = "update" if page_id else "create"
            print(
                f"Failed to {action} Notion page for {name}: {response.status}, {await response.text()}"
            )


async def find_class_page_id_by_class_code(class_code, session):
    class_code = extract_course_code(class_code)
    print(f"Looking for class page with class code: {class_code}")
    url = f"https://api.notion.com/v1/databases/{COURSES_DATABASE_ID}/query"
    headers = await get_notion_headers()
    json_body = {
        "filter": {
            "property": "Code",  # Adjust this to match the exact property name for class codes in your database
            "text": {
                "contains": class_code  # You might need to adjust the filter condition (e.g., "equals") based on your data format
            },
        }
    }

    print(json_body)
    async with session.post(url, headers=headers, json=json_body) as response:
        if response.status == 200:
            results = await response.json()
            if results["results"]:
                # Assuming the first result is the correct one; consider adding more specific checks if necessary
                return results["results"][0]["id"]
            else:
                print(
                    f"No class page found for class code {class_code} in Notion database."
                )
        else:
            print(
                f"Failed to query Notion database for class code {class_code}. Status: {response.status}, Response: {await response.text()}"
            )
    return None


async def find_existing_assignment(name, class_code, session):
    class_page_id = await find_class_page_id_by_class_code(class_code, session)
    if not class_page_id:
        print(
            f"Could not find class page ID for {class_code}, skipping duplicate check."
        )
        return None

    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    headers = await get_notion_headers()
    json_body = {
        "filter": {
            "and": [
                {"property": "Name", "text": {"equals": name}},
                {"property": "Class", "relation": {"contains": class_page_id}},
            ]
        }
    }

    async with session.post(url, headers=headers, json=json_body) as response:
        if response.status == 200:
            results = await response.json()
            if results["results"]:
                print(f"Found existing assignment for {name}.")
                return results["results"][0]  # Return the first match
            else:
                print(f"No existing assignment found for {name}.")
        else:
            print(
                f"Failed to query Notion database. Status: {response.status}, Response: {await response.text()}"
            )
        return None


async def update_course_homework_relation(course_id, assignment_id):
    url = f"https://api.notion.com/v1/pages/{course_id}"
    headers = await get_notion_headers()
    payload = {"properties": {"Homework": {"relation": [{"id": assignment_id}]}}}
    async with aiohttp.ClientSession() as session:
        async with session.patch(url, headers=headers, json=payload) as response:
            if response.status == 200:
                print(
                    f"Successfully updated course with ID {course_id} to include assignment {assignment_id} in 'Homework'."
                )
            else:
                print(
                    f"Failed to update course's 'Homework' relation. Error: {await response.text()}"
                )


async def get_current_homework_relation(course_id):
    """Retrieves the current 'Homework' relation for a course."""
    url = f"https://api.notion.com/v1/pages/{course_id}"
    headers = await get_notion_headers()
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        current_relation_data = response.json()["properties"]["Homework"]["relation"]
        return current_relation_data
    else:
        print(
            f"Failed to retrieve current 'Homework' relation for course {course_id}. Error: {response.text}"
        )
        return []


def extract_course_code(course_name):
    """Extracts course code from a course name."""
    parts = course_name.split("-")
    return f"{parts[1]}-{parts[2]}" if len(parts) > 1 else course_name


async def main(event, context):
    start_time = time.time()
    print("-- Starting processing --")

    await process_canvas_courses()  # Assuming this is properly awaited and defined as async
    await process_gradescope_assignments()  # This should be defined as async and awaited

    await archive_old_assignments()  # This should be defined as async and awaited

    print(f"-> Process finished in {time.time() - start_time} seconds")

    return {"statusCode": 200, "body": json.dumps("Process completed successfully.")}


def lambda_handler(event, context):
    asyncio.run(main(event, context))


if __name__ == "__main__":
    asyncio.run(main({}, {}))
