## Overview
This script is designed to integrate various educational platforms (Canvas, Gradescope) with Notion to automate the process of managing assignments. It fetches assignments from Canvas and Gradescope, determines if they're within a specified timeframe, checks submission status, and updates or creates corresponding pages in a Notion database. Additionally, it archives old assignments in Notion based on their due dates. The script utilizes asynchronous programming for efficient network I/O operations and is structured to be deployed as an AWS Lambda function for scheduled execution.

## Features
- Fetch assignments from Canvas and Gradescope.
- Filter assignments based on due date.
- Check if assignments have been submitted.
- Create or update Notion pages for assignments.
- Archive old assignments in Notion.
- Designed to run as an AWS Lambda function.

## Requirements
- Python 3.8 or higher.
- `aiohttp` for asynchronous HTTP requests.
- `canvasapi` for interacting with the Canvas LMS API.
- A custom `GSConnector` module for Gradescope integration.
- `python-dateutil` for date parsing and manipulation.
- `requests` library for synchronous HTTP requests.
- Access to Notion API and relevant tokens.

## Setup
1. Install required Python packages:
```bash
pip install aiohttp canvasapi python-dateutil requests
```
2. Set up environment variables:
- `NOTION_TOKEN`: Notion Integration Token.
- `NOTION_DATABASE_ID`: ID of the Notion database for assignments.
- `CANVAS_API_URL`: URL of the Canvas LMS instance (e.g., `https://bruinlearn.ucla.edu`).
- `CANVAS_API_KEY`: Canvas API Key.
- `CANVAS_ID`: User's Canvas ID.
- `GS_EMAIL` & `GS_PASSWORD`: Credentials for Gradescope.
- `COURSES_DATABASE_ID`: ID of the Notion database for courses.
- `ARCHIVE_LIMIT`: Number of days after due date to archive assignments.

![alt text](/images/image.png)

3. Deploy the script to AWS Lambda:
- Ensure the lambda function is configured with the appropriate memory and timeout settings.
- Set up a trigger (e.g., CloudWatch Events) for periodic execution.

## Usage
When deployed, the script automatically:
- Fetches assignments from the specified Canvas and Gradescope courses.
- Checks each assignment's due date and submission status.
- Creates or updates Notion pages for assignments within the specified timeframe.
- Archives assignments in Notion that are past their due dates.

## Code Structure
- The script is organized into several asynchronous functions, each responsible for a specific part of the workflow (e.g., processing Canvas courses, processing Gradescope assignments, interacting with Notion).
- Use of global variables for caching data and environment configuration.
- Use of `asyncio` and `aiohttp` for efficient asynchronous operations.
- Functions for parsing dates and handling API interactions.

## Notes
- Ensure that your Notion database has the required properties (e.g., Name, Class, Date, Completed, Status) to store assignment information.
- Customize the `get_notion_headers`, `process_canvas_course`, `process_assignment`, and other functions as necessary to fit your specific requirements and data structure.
- The script assumes certain response structures from Canvas, Gradescope, and Notion APIs. Ensure compatibility with your versions of these platforms.

## Disclaimer
- This script is provided as-is, and customization may be necessary to work with your specific setup.
- Ensure you have the necessary permissions to access and modify data in Canvas, Gradescope, and Notion.
