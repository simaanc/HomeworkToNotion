import os
import asyncio
import aiohttp
import logging
import time
from datetime import datetime, timedelta, timezone
from dateutil import parser
import json
import re

import canvasapi
from canvasapi import Canvas
from gradescope.scope import GSConnector

from concurrent.futures import ThreadPoolExecutor

from async_lru import alru_cache

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables once
ENV_VARS = {
    'NOTION_TOKEN': os.getenv("NOTION_TOKEN"),
    'NOTION_DATABASE_ID': os.getenv("NOTION_DATABASE_ID"),
    'CANVAS_API_URL': "https://bruinlearn.ucla.edu",
    'CANVAS_API_KEY': os.getenv("CANVAS_API_KEY"),
    'CANVAS_ID': os.getenv("CANVAS_ID"),
    'GS_EMAIL': os.getenv("GS_EMAIL"),
    'GS_PASSWORD': os.getenv("GS_PASSWORD"),
    'COURSES_DATABASE_ID': os.getenv("COURSES_DATABASE_ID"),
    'ARCHIVE_LIMIT': int(os.getenv("ARCHIVE_LIMIT", 30)),  # Default to 30 days
}

# Initialize APIs
canvas = Canvas(ENV_VARS["CANVAS_API_URL"], ENV_VARS["CANVAS_API_KEY"])
gs_connector = GSConnector(ENV_VARS["GS_EMAIL"], ENV_VARS["GS_PASSWORD"])

# Load course title to class code mappings
try:
    with open("mappings.json", "r") as f:
        COURSE_TITLE_TO_CODE = json.load(f)
except FileNotFoundError:
    logger.error(
        "mappings.json file not found. Please create it in the script directory."
    )
    COURSE_TITLE_TO_CODE = {}

accessible_courses_cache = set()

notion_headers_cache = None

# Semaphore to limit concurrent requests
semaphore = asyncio.Semaphore(10)  # Limit to 10 concurrent requests


async def log(message: str):
    logger.info(message)


async def str_to_datetime(date_str: str) -> datetime:
    return parser.isoparse(date_str)


async def is_within_timeframe(due_date, days_after=None) -> bool:
    if days_after is None:
        days_after = ENV_VARS["ARCHIVE_LIMIT"]
    try:
        days_after = int(days_after)
    except ValueError:
        days_after = 30  # Default value if conversion fails

    now = datetime.now(timezone.utc)

    if due_date in [None, "No Due Date"]:
        # If no due date is provided, consider it as not within the timeframe
        return False

    if isinstance(due_date, str):
        try:
            due_date = await str_to_datetime(due_date)
        except ValueError:
            # If due_date string is invalid, consider it as not within the timeframe
            return False

    return due_date > now or (now - due_date).days <= days_after


async def get_notion_headers() -> dict:
    global notion_headers_cache
    if notion_headers_cache is None:
        notion_headers_cache = {
            "Authorization": f"Bearer {ENV_VARS['NOTION_TOKEN']}",
            "Notion-Version": "2021-08-16",
            "Content-Type": "application/json",
        }
    return notion_headers_cache


def get_optimal_worker_count() -> int:
    lambda_memory = os.environ.get(
        "AWS_LAMBDA_FUNCTION_MEMORY_SIZE", 128
    )  # Default to 128 MB if not set
    # Simple heuristic: 1 worker per 256 MB of Lambda memory
    return max(1, int(lambda_memory) // 256)


def cache_accessible_courses():
    global accessible_courses_cache
    try:
        courses = canvas.get_courses()
        accessible_courses_cache = {
            course.id for course in courses if course.access_restricted_by_date == False
        }
    except Exception as e:
        logger.error(f"Error caching accessible courses: {e}")


def safe_get_course_name(course) -> str:
    """Safely get the course code or name of a course."""
    try:
        course_id = getattr(course, "id", "Unknown ID")
        course_name = getattr(course, "name", None)
        course_code = getattr(course, "course_code", None)
        sis_course_id = getattr(course, "sis_course_id", None)

        logger.debug(f"course.id: {course_id}")
        logger.debug(f"course.name: {course_name}")
        logger.debug(f"course.course_code: {course_code}")
        logger.debug(f"course.sis_course_id: {sis_course_id}")

        if course_code and course_code != course_name:
            return course_code
        elif sis_course_id:
            return sis_course_id
        elif course_name:
            return course_name
        else:
            return "Unknown Course"
    except Exception as e:
        logger.error(
            f"Failed to access course attributes for course with ID: {course_id}: {e}"
        )
        return "Unknown Course"


async def process_canvas_course(course, session):
    async with semaphore:
        loop = asyncio.get_event_loop()
        try:
            # Attempt to access course attributes
            course_name = safe_get_course_name(course)
            if course_name == "Unknown Course":
                logger.debug(
                    f"Skipping course with ID: {course.id} due to unknown course name."
                )
                return  # Skip this course

            logger.info(f"-> Processing course: {course_name}")

            # Try fetching assignments for the course
            try:
                assignments = await loop.run_in_executor(None, course.get_assignments)
            except canvasapi.exceptions.Forbidden as e:
                logger.debug(
                    f"Skipping course '{course_name}' (ID: {course.id}) due to lack of permissions: {e}"
                )
                return  # Skip this course

            # Process each assignment
            tasks = [
                process_assignment(assignment, course_name, course.id, session)
                for assignment in assignments
            ]
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(
                f"Failed to process course '{course_name}' (ID: {course.id}): {e}"
            )


async def process_assignment(
    assignment, course_name: str, course_id: int, session: aiohttp.ClientSession
):
    """Processes a single assignment from Canvas."""
    assignment_name = assignment.name.strip()
    due_date = assignment.due_at  # Keep as None if missing

    # Check if the assignment is within the right timeframe
    if not await is_within_timeframe(due_date):
        return  # Skip further processing for this assignment

    # Check if the assignment has been submitted
    submitted = await is_assignment_submitted_canvas(
        course_id, assignment.id, ENV_VARS["CANVAS_ID"], session
    )

    # Check for existing assignment to avoid duplication
    existing_assignment = await find_existing_assignment(
        assignment_name, course_name, session
    )
    if existing_assignment:
        page_id = existing_assignment.get("id")
        logger.info(f"Updating existing assignment in Notion: {assignment_name}")
        await create_or_update_notion_page(
            assignment_name, course_name, due_date, submitted, session, page_id=page_id
        )
    else:
        logger.info(f"Creating new assignment in Notion: {assignment_name}")
        await create_or_update_notion_page(
            assignment_name, course_name, due_date, submitted, session
        )


async def process_canvas_courses(session: aiohttp.ClientSession):
    loop = asyncio.get_event_loop()
    courses = await loop.run_in_executor(None, canvas.get_courses)

    tasks = [process_canvas_course(course, session) for course in courses]
    await asyncio.gather(*tasks)


async def process_gradescope_assignment(assignment, session):
    async with semaphore:
        assignment_name = assignment.name
        course_name = assignment.course
        due_date = (
            assignment.close_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            if assignment.close_date
            else None
        )
        submitted = is_assignment_submitted_gradescope(assignment)

        # Check if the assignment is within the right timeframe
        if not await is_within_timeframe(due_date):
            return  # Skip further processing for this assignment

        # Check for existing assignment to avoid duplication
        existing_assignment = await find_existing_assignment(
            assignment_name, course_name, session
        )
        if existing_assignment:
            page_id = existing_assignment.get("id")
            logger.info(f"Updating existing Gradescope assignment: {assignment_name}")
            await create_or_update_notion_page(
                assignment_name,
                course_name,
                due_date,
                submitted,
                session,
                page_id=page_id,
            )
        else:
            logger.info(
                f"Adding Gradescope assignment to Notion: {assignment_name} for {course_name}"
            )
            await create_or_update_notion_page(
                assignment_name,
                course_name,
                due_date,
                completed=submitted,
                session=session,
            )


async def process_gradescope_assignments(session: aiohttp.ClientSession):
    assignments = gs_connector.get_all_assignments()
    logger.info(
        f"-- Successfully retrieved {len(assignments)} assignments from Gradescope"
    )
    tasks = [
        process_gradescope_assignment(assignment, session) for assignment in assignments
    ]
    await asyncio.gather(*tasks)


def is_assignment_submitted_gradescope(assignment) -> bool:
    """Check if a Gradescope assignment is submitted based on its status."""
    status = assignment.status
    # Assuming 'Submitted' and 'Submitted-Late' as indicators of submission
    return status in ["Submitted", "Submitted-Late"]


async def is_assignment_submitted_canvas(
    course_id, assignment_id, user_id, session
) -> bool:
    url = f"https://bruinlearn.ucla.edu/api/v1/courses/{course_id}/assignments/{assignment_id}/submissions/{user_id}"
    headers = {"Authorization": f"Bearer {ENV_VARS['CANVAS_API_KEY']}"}

    async with semaphore:
        async with session.get(url, headers=headers) as response:
            data = await response.json()
            logger.debug(
                f"Canvas submission data for assignment {assignment_id}: {data}"
            )

            if response.status == 200:
                # Assuming the response includes a 'workflow_state' key to check submission status
                return data.get("workflow_state") in ["submitted", "graded"]
            else:
                # Handle error or unexpected response
                logger.error(f"Failed to check submission status: {response.status}")
                return False


async def mark_as_archived(page_id, session):
    """Marks a Notion page as 'Archived' by updating its status."""
    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {"properties": {"Status": {"select": {"name": "Archived"}}}}

    response = await notion_api_request("PATCH", url, session, json=payload)
    if response is not None:
        logger.info(f"Assignment with ID {page_id} marked as Archived.")
    else:
        logger.error(f"Failed to archive assignment {page_id}.")


async def archive_old_assignments(session: aiohttp.ClientSession):
    """Archives assignments in Notion based on their due date and completion status."""
    now = datetime.now(timezone.utc)
    assignments = await query_notion_database(session=session)

    for assignment in assignments:
        properties = assignment.get("properties", {})
        due_date_info = properties.get("Date", {}).get("date")
        status_info = properties.get("Status", {}).get("select")
        completed = properties.get("Completed", {}).get("checkbox", False)
        assignment_id = assignment.get("id", "Unknown ID")

        status = status_info.get("name") if status_info else None

        if due_date_info and due_date_info.get("start"):
            due_date_str = due_date_info.get("start")
            due_date = await str_to_datetime(due_date_str)
            days_since_due = (now - due_date).days

            if days_since_due > ENV_VARS["ARCHIVE_LIMIT"]:
                if status != "Archived":
                    await mark_as_archived(assignment_id, session)
            else:
                if status != "Active":
                    await ensure_status_active(assignment_id, session, status)
        else:
            # No due date
            if completed:
                if status != "Archived":
                    logger.info(
                        f"Archiving completed assignment {assignment_id} with no due date."
                    )
                    await mark_as_archived(assignment_id, session)
            else:
                if status != "Active":
                    logger.info(
                        f"Setting assignment {assignment_id} with no due date to 'Active'."
                    )
                    await ensure_status_active(assignment_id, session, status)


async def ensure_status_active(assignment_id, session, current_status):
    """Ensures the assignment status is set to 'Active' if it's not already set or is None."""
    if current_status != "Active":
        logger.info(f"Setting assignment {assignment_id} status to 'Active'.")
        await update_assignment_status(assignment_id, "Active", session)
    else:
        logger.info(f"Assignment {assignment_id} already set to 'Active'.")


async def update_assignment_status(assignment_id, status, session):
    """Updates the assignment's status in Notion."""
    url = f"https://api.notion.com/v1/pages/{assignment_id}"
    payload = {"properties": {"Status": {"select": {"name": status}}}}

    response = await notion_api_request("PATCH", url, session, json=payload)
    if response is not None:
        logger.info(f"Assignment {assignment_id} status updated to {status}.")
    else:
        logger.error(f"Failed to update assignment {assignment_id} status.")


def map_notion_result_to_data(result) -> dict:
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
    database_id=ENV_VARS["NOTION_DATABASE_ID"],
    filter_name=None,
    class_id=None,
    session=None,
):
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
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

    results = await notion_api_request("POST", url, session, json=json_body)
    if results:
        return results.get("results", [])
    else:
        return []


async def find_course_by_code(class_code):
    url = f"https://api.notion.com/v1/databases/{ENV_VARS['COURSES_DATABASE_ID']}/query"
    json_body = {"filter": {"property": "Code", "text": {"equals": class_code}}}
    async with aiohttp.ClientSession() as session:
        results = await notion_api_request("POST", url, session, json=json_body)
        if results and results.get("results", []):
            return results["results"][0]["id"]
        else:
            logger.warning(f"Failed to find course with code {class_code}.")
            return None


async def create_or_update_notion_page(
    name, class_name, date, completed, session, page_id=None
):
    class_page_id = await find_class_page_id_by_class_code(class_name, session)
    if class_page_id is None:
        logger.warning(f"Class page ID not found for {class_name}")
        return

    # Determine the status based on completion and due date
    if date and date != "No Due Date":
        # Assignment has a due date
        within_timeframe = await is_within_timeframe(date)
        if completed and not within_timeframe:
            status = "Archived"
        else:
            status = "Active"
    else:
        # Assignment has no due date
        if completed:
            status = "Archived"
        else:
            status = "Active"

    properties = {
        "Name": {"title": [{"text": {"content": name}}]},
        "Class": {"relation": [{"id": class_page_id}]},
        "Completed": {"checkbox": completed},
        "Status": {"select": {"name": status}},
    }

    # Include 'Date' property only if a valid date is provided
    if date and date != "No Due Date":
        properties["Date"] = {"date": {"start": date}}

    if page_id:
        # Update existing page
        url = f"https://api.notion.com/v1/pages/{page_id}"
        payload = {"properties": properties}
        method = "PATCH"
    else:
        # Create new page
        url = "https://api.notion.com/v1/pages"
        payload = {
            "parent": {"database_id": ENV_VARS["NOTION_DATABASE_ID"]},
            "properties": properties,
        }
        method = "POST"

    response = await notion_api_request(method, url, session, json=payload)
    if response is not None:
        action = "Updated" if page_id else "Created"
        logger.info(
            f"{action} Notion page successfully for {name} with status {status}"
        )
    else:
        action = "update" if page_id else "create"
        logger.error(f"Failed to {action} Notion page for {name}.")


async def find_existing_assignment(name, class_code, session):
    class_page_id = await find_class_page_id_by_class_code(class_code, session)
    filters = [{"property": "Name", "text": {"equals": name}}]

    if class_page_id:
        filters.append({"property": "Class", "relation": {"contains": class_page_id}})
    else:
        logger.warning(
            f"Class page ID not found for {class_code}, proceeding without class filter."
        )

    url = f"https://api.notion.com/v1/databases/{ENV_VARS['NOTION_DATABASE_ID']}/query"
    json_body = {"filter": {"and": filters}}

    results = await notion_api_request("POST", url, session, json=json_body)
    if results and results.get("results", []):
        logger.info(f"Found existing assignment for {name}.")
        return results["results"][0]
    else:
        logger.info(f"No existing assignment found for {name}.")
        return None


async def update_course_homework_relation(course_id, assignment_id):
    url = f"https://api.notion.com/v1/pages/{course_id}"
    payload = {"properties": {"Homework": {"relation": [{"id": assignment_id}]}}}
    async with aiohttp.ClientSession() as session:
        response = await notion_api_request("PATCH", url, session, json=payload)
        if response is not None:
            logger.info(
                f"Successfully updated course with ID {course_id} to include assignment {assignment_id} in 'Homework'."
            )
        else:
            logger.error(f"Failed to update course's 'Homework' relation.")


async def get_current_homework_relation(course_id):
    """Retrieves the current 'Homework' relation for a course."""
    url = f"https://api.notion.com/v1/pages/{course_id}"
    async with aiohttp.ClientSession() as session:
        response = await notion_api_request("GET", url, session)
        if response:
            current_relation_data = response["properties"]["Homework"]["relation"]
            return current_relation_data
        else:
            logger.error(
                f"Failed to retrieve current 'Homework' relation for course {course_id}."
            )
            return []


def extract_course_code(course_name: str) -> str:
    """Extracts class code from a course name using regex or mappings."""
    if not course_name:
        return "Unknown"

    logger.debug(f"Original course name: '{course_name}'")

    # Normalize course name by stripping whitespace
    course_name_normalized = course_name.strip()

    # Attempt regex extraction first
    pattern = r"^[^-]+-([A-Z ]+-\d+)-[^-]+-.*"
    match = re.match(pattern, course_name_normalized)
    if match:
        class_code = match.group(1).strip()
        logger.debug(f"Extracted class code using regex: '{class_code}'")
        return class_code
    else:
        # Use mappings from mappings.json as a fallback
        class_code = COURSE_TITLE_TO_CODE.get(course_name_normalized)
        if class_code:
            logger.debug(f"Extracted class code from mappings.json: '{class_code}'")
            return class_code
        else:
            # Attempt to match patterns like 'EE149' or 'ECE 102'
            pattern_code = r"^[A-Z]+ ?\d+$"
            if re.match(pattern_code, course_name_normalized):
                logger.debug(
                    f"Using course name as class code: '{course_name_normalized}'"
                )
                return course_name_normalized
            else:
                logger.warning(f"Could not extract class code from: '{course_name}'")
                return course_name_normalized


@alru_cache(maxsize=None)
async def find_class_page_id_by_class_code(class_code, session):
    class_code = extract_course_code(class_code)
    logger.info(f"Looking for class page with class code: {class_code}")
    url = f"https://api.notion.com/v1/databases/{ENV_VARS['COURSES_DATABASE_ID']}/query"
    json_body = {
        "filter": {
            "property": "Code",
            "text": {"equals": class_code},
        }
    }
    results = await notion_api_request("POST", url, session, json=json_body)
    if results and results.get("results", []):
        return results["results"][0]["id"]
    else:
        logger.warning(
            f"No class page found for class code {class_code} in Notion database."
        )
    return None


async def notion_api_request(method, url, session, retries=3, **kwargs):
    """Helper function to make Notion API requests with retry logic."""
    for attempt in range(retries):
        try:
            headers = await get_notion_headers()
            async with session.request(
                method, url, headers=headers, **kwargs
            ) as response:
                if response.status in [200, 201, 204]:
                    if response.content_type == "application/json":
                        return await response.json()
                    else:
                        return None
                elif response.status == 429:
                    wait_time = 2**attempt
                    logger.warning(
                        f"Rate limit exceeded, retrying in {wait_time} seconds..."
                    )
                    await asyncio.sleep(wait_time)
                else:
                    text = await response.text()
                    logger.error(
                        f"Notion API request failed: {response.status}, {text}"
                    )
                    response.raise_for_status()
        except aiohttp.ClientResponseError as e:
            if e.status == 429:
                wait_time = 2**attempt
                logger.warning(
                    f"Rate limit exceeded, retrying in {wait_time} seconds..."
                )
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"ClientResponseError: {e}")
                raise
    logger.error(f"Failed to make request to {url} after {retries} retries.")
    return None


async def main(event, context):
    start_time = time.time()
    logger.info("-- Starting processing --")

    async with aiohttp.ClientSession() as session:
        await process_canvas_courses(session)
        await process_gradescope_assignments(session)
        await archive_old_assignments(session)

    logger.info(f"-> Process finished in {time.time() - start_time} seconds")

    return {"statusCode": 200, "body": json.dumps("Process completed successfully.")}


def lambda_handler(event, context):
    asyncio.run(main(event, context))


if __name__ == "__main__":
    asyncio.run(main({}, {}))
