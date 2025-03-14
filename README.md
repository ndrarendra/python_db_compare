# Web-enabled Database Comparison Tool

## Overview
This tool provides a web-based interface for comparing two MySQL/MariaDB databases. It checks schema differences, metadata variations, and row-level data inconsistencies. The tool generates reports in **Excel**, **Word**, and **log files** while also displaying a summary on a web page.

## Features
- **Schema Comparison:** Tables, columns, indexes, constraints, triggers, routines, and views.
- **Metadata Analysis:** Checks table storage engines, collation, and comments.
- **Row-Level Data Comparison:** Identifies missing and differing rows.
- **Exclusion and Inclusion Filters:** Specify tables to include/exclude via JSON.
- **Detailed Reports:** Generates logs, Excel, and Word reports.
- **Web-Based UI:** Upload JSON configurations or manually enter connection details.

## Installation

### Prerequisites
Ensure you have Python installed along with the required dependencies.

### Install Dependencies
```bash
pip install flask pymysql pandas xlsxwriter python-docx
python your_script.py
http://localhost:5000/
```


## Usage
### Web UI
Open http://localhost:5000/ in your browser.
Enter database connection details in JSON format or upload a JSON file.
Click Compare Databases.
Review the summary and download reports.
JSON Input Format
```json
{
  "dbA": { "host": "localhost", "user": "root", "password": "yourpassword", "db": "database_A", "port": 3306 },
  "dbB": { "host": "localhost", "user": "root", "password": "yourpassword", "db": "database_B", "port": 3306 },
  "allowed_objects": ["allowed.txt"],
  "exclude_tables": ["table3", "table4"],
  "verbose": false,
  "output_folder": null
}
```

## outputs
### Web-based Summary: Highlights key differences.
Log Files: Detailed logs in the output directory.
Excel Report: Comparison summary with color-coded differences.
Word Report: Structured document with schema and data differences.
API Endpoints
- **Homepage (GET /)**
Displays the web form for database comparison.

- **Compare Databases (POST /compare)**
Processes JSON input and performs database comparison.

- **Download Reports (GET /download?folder=xyz&file=abc.log)**
Allows downloading generated logs and reports.

- **Troubleshooting**
Ensure MySQL/MariaDB is running and accessible.
Verify JSON input format (use double quotes for keys/values).
Check Python dependencies are installed.

## License
This project is licensed under the MIT License.