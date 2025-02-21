"""
Database Comparison Tool
------------------------

This script compares two MySQL databases by checking both their schema and data.
It compares the following objects:
  - Tables and their columns
  - Indexes, constraints, views, triggers, and stored routines (procedures/functions)
  - Row-level data for tables that exist in both databases

For stored routines, triggers, and views, the tool "normalizes" the SQL definitions 
(by replacing the expected database name with a placeholder, converting to lower-case,
and removing extra whitespace) so that differences based solely on formatting or the 
database name are ignored.

After comparison, the tool produces:
  - Detailed logs (written to log files and optionally to the console)
  - A summary report exported to both an Excel file and a nicely formatted Word document

Usage:
  Run the script from the command line. You can optionally specify a list of objects (tables,
  views, procedures, etc.) to compare using the --tables argument or by providing a text file 
  (one object per line) with --table-file.
      python db_compare.py --table-file tables.txt --verbose
  If no object names are specified, all objects will be compared.

Adjust the connection parameters and database names as needed.
"""

import pymysql
import logging
import datetime
from collections import defaultdict
import pandas as pd
import argparse
from typing import Dict, List, Any, Set, Optional
import re
import zlib
import difflib
import os
import sys

# Optional: for exporting a pretty summary to Word (requires: pip install python-docx)
try:
    from docx import Document
    from docx.shared import Pt, Inches
    from docx.enum.table import WD_TABLE_ALIGNMENT
except ImportError:
    Document = None

#######################################
# Custom Formatter Classes for Readability
#######################################
class ReadableFormatter(logging.Formatter):
    """
    Custom formatter that inserts a blank line before messages that look like section headers.
    This improves readability in the log file.
    """
    def format(self, record):
        message = super().format(record)
        if record.msg.startswith(">>") or record.msg.startswith("=") or record.msg.startswith("----"):
            return "\n" + message
        return message

class ColoredFormatter(logging.Formatter):
    """
    Custom formatter for console logging that adds colors based on the log level.
    This makes it easier to identify warnings, errors, etc. in the console.
    """
    COLORS = {
        'DEBUG': '\033[94m',    # Blue
        'INFO': '\033[92m',     # Green
        'WARNING': '\033[93m',  # Yellow
        'ERROR': '\033[91m',    # Red
        'CRITICAL': '\033[95m'  # Magenta
    }
    RESET = '\033[0m'
    
    def format(self, record):
        color = self.COLORS.get(record.levelname, self.RESET)
        message = super().format(record)
        return f"{color}{message}{self.RESET}"

#######################################
# Global Variables (for logging context)
#######################################
global_dbA_name = ""
global_dbB_name = ""

#######################################
# Logger Setup Function
#######################################
def setup_logger(name: str, filename: str, verbose: bool = False) -> logging.Logger:
    """
    Create and configure a logger.
    Log messages are written to a file (always) and optionally to the console.
    This ensures that even errors (via logger.error) are written to the file.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # File handler: writes detailed logs including timestamp, log level, and function info.
    file_handler = logging.FileHandler(filename, mode='w', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_formatter = ReadableFormatter(
        '[%(asctime)s] [%(levelname)s] [%(name)s:%(funcName)s:%(lineno)d] %(message)s'
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    # Console handler: only added if verbose flag is True.
    if verbose:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = ColoredFormatter('[%(levelname)s] %(message)s')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
    
    # Prevent log messages from being propagated to the root logger.
    logger.propagate = False
    return logger

# Generate a timestamp string for log file names and exported reports.
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
schema_log_filename = f"schema_log_{timestamp}.log"
all_log_filename = f"all_log_{timestamp}.log"
summary_log_filename = f"summary_log_{timestamp}.log"

#######################################
# Database Connection Function
#######################################
def get_mysql_connection(host: str, user: str, password: str, db: str, port: int = 3306) -> pymysql.connections.Connection:
    """
    Connect to a MySQL database using the provided credentials.
    Returns the connection object.
    """
    return pymysql.connect(
        host=host,
        user=user,
        password=password,
        db=db,
        port=port,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.Cursor
    )

#######################################
# Schema Retrieval Functions
#######################################
def get_tables_and_columns(conn: pymysql.connections.Connection, db_name: str) -> Dict[str, List[Any]]:
    """
    Retrieve all tables and their column details from the specified database.
    Returns a dictionary where keys are table names and values are lists of column tuples.
    """
    query = f"""
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_TYPE
        FROM information_schema.columns
        WHERE table_schema = '{db_name}'
        ORDER BY TABLE_NAME, ORDINAL_POSITION;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
    schema_dict = defaultdict(list)
    for table_name, col_name, data_type, is_nullable, col_default, col_type in rows:
        schema_dict[table_name].append((col_name, data_type, is_nullable, col_default, col_type))
    return schema_dict

def get_indexes(conn: pymysql.connections.Connection, db_name: str) -> Dict[Any, List[Any]]:
    """
    Retrieve index definitions for each table in the specified database.
    Returns a dictionary with keys as (table_name, index_name) and values as lists of index details.
    """
    query = f"""
      SELECT TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, NON_UNIQUE, INDEX_TYPE 
      FROM information_schema.statistics
      WHERE table_schema = '{db_name}'
      ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
    indexes = defaultdict(list)
    for table_name, index_name, seq, col_name, non_unique, index_type in rows:
        indexes[(table_name, index_name)].append((seq, col_name, non_unique, index_type))
    return indexes

def get_triggers(conn: pymysql.connections.Connection, db_name: str) -> Dict[str, Dict[str, Any]]:
    """
    Retrieve trigger definitions from the specified database.
    Returns a dictionary where keys are trigger names.
    """
    query = f"""
        SELECT TRIGGER_NAME, EVENT_MANIPULATION, EVENT_OBJECT_TABLE, ACTION_TIMING, ACTION_STATEMENT 
        FROM information_schema.triggers
        WHERE trigger_schema = '{db_name}';
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
    triggers = {}
    for trigger_name, event, table, timing, statement in rows:
        triggers[trigger_name] = {
            'event': event,
            'table': table,
            'timing': timing,
            'statement': statement
        }
    return triggers

def get_routines(conn: pymysql.connections.Connection, db_name: str) -> Dict[Any, Dict[str, Any]]:
    """
    Retrieve stored routines (procedures/functions) from the specified database.
    Returns a dictionary with keys as (routine_name, routine_type).
    """
    query = f"""
        SELECT ROUTINE_NAME, ROUTINE_TYPE, DATA_TYPE, DTD_IDENTIFIER, ROUTINE_DEFINITION, SQL_DATA_ACCESS
        FROM information_schema.routines
        WHERE routine_schema = '{db_name}';
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
    routines = {}
    for routine_name, routine_type, data_type, dtd_identifier, definition, sql_data_access in rows:
        routines[(routine_name, routine_type)] = {
            'data_type': data_type,
            'dtd_identifier': dtd_identifier,
            'definition': definition,
            'sql_data_access': sql_data_access
        }
    return routines

def get_constraints(conn: pymysql.connections.Connection, db_name: str) -> Dict[Any, Dict[str, Any]]:
    """
    Retrieve table constraints (primary keys, foreign keys, unique constraints) from the specified database.
    Returns a dictionary with keys as (table_name, constraint_name).
    """
    query = """
        SELECT
            tc.TABLE_NAME,
            tc.CONSTRAINT_NAME,
            tc.CONSTRAINT_TYPE,
            COALESCE(
                GROUP_CONCAT(DISTINCT kcu.COLUMN_NAME
                             ORDER BY kcu.ORDINAL_POSITION
                             SEPARATOR ', '),
                ''
            ) AS columns
        FROM information_schema.table_constraints tc
        LEFT JOIN information_schema.key_column_usage kcu
               ON tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
              AND tc.TABLE_NAME = kcu.TABLE_NAME
              AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
        WHERE tc.TABLE_SCHEMA = %s
        GROUP BY tc.TABLE_NAME, tc.CONSTRAINT_NAME, tc.CONSTRAINT_TYPE
    """
    constraints = {}
    with conn.cursor() as cursor:
        cursor.execute(query, (db_name,))
        rows = cursor.fetchall()
        for (table_name, constraint_name, constraint_type, columns) in rows:
            columns_list = columns.split(', ') if columns else []
            constraints[(table_name, constraint_name)] = {
                'type': constraint_type,
                'columns': columns_list
            }
    return constraints

def get_views(conn: pymysql.connections.Connection, db_name: str) -> Dict[str, str]:
    """
    Retrieve view definitions from the specified database.
    Returns a dictionary with view names as keys and their definitions as values.
    """
    query = f"""
        SELECT TABLE_NAME, VIEW_DEFINITION
        FROM information_schema.views
        WHERE table_schema = '{db_name}';
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
    views = {}
    for view_name, definition in rows:
        views[view_name] = definition
    return views

#######################################
# SQL Normalization & Helper Functions
#######################################
def check_db_name_in_definition(definition: str, expected_db_name: str) -> bool:
    """
    Check if all fully qualified table references in the SQL definition have the expected database name.
    Returns True if the expected database name is used (or not referenced), otherwise False.
    """
    if definition is None:
        return True
    expected = expected_db_name.replace("`", "").lower()
    pattern = re.compile(r'\b(?:from|join)\s+`([^`]+)`\s*\.\s*`([^`]+)`', flags=re.IGNORECASE)
    matches = pattern.findall(definition)
    if not matches:
        pattern_no_backticks = re.compile(r'\b(?:from|join)\s+([^\s`\.]+)\s*\.\s*([^\s`]+)', flags=re.IGNORECASE)
        matches = pattern_no_backticks.findall(definition)
    if not matches:
        return True
    for db_ref, table_ref in matches:
        if db_ref.lower() != expected:
            return False
    return True

def normalize_sql_definition(definition: str, db_name: str) -> str:
    """
    Normalize an SQL definition by:
      - Replacing the expected database name with the placeholder <DATABASE>
      - Converting the SQL to lower-case
      - Removing extra whitespace
    This helps in comparing SQL definitions without being affected by formatting differences.
    """
    if definition is None:
        return ''
    pattern = re.compile(r'`?' + re.escape(db_name) + r'`?\.', flags=re.IGNORECASE)
    normalized = pattern.sub(r'<DATABASE>.', definition)
    normalized = " ".join(normalized.lower().split())
    return normalized

def compute_table_checksum(
    conn: pymysql.connections.Connection,
    table_name: str,
    columns: List[str],
    logger: logging.Logger
) -> Any:
    """
    Compute a checksum for a table based on the provided list of column names.
    Uses a concatenation of column values and computes a CRC32 checksum.
    Returns the checksum value or None if there is an error.
    """
    if not columns:
        return None

    # Build a CONCAT expression for all columns, handling NULLs with COALESCE.
    col_concat = ",'#',".join([f"COALESCE(CAST(`{col}` AS CHAR), '')" for col in columns])
    query = f"SELECT BIT_XOR(CRC32(CONCAT_WS('#',{col_concat}))) FROM `{table_name}`"

    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()[0]
        return result
    except pymysql.err.OperationalError as e:
        logger.error(
            "Could not compute checksum for table '%s'. Likely an unknown column or other issue. Error: %s",
            table_name, e
        )
        return None
    except Exception as ex:
        logger.error("Unexpected error computing checksum for '%s': %s", table_name, ex)
        return None

def diff_location_summary(textA: str, textB: str, max_blocks: int = 30, max_lines_per_block: int = 10) -> str:
    """
    Generate a concise diff summary between two texts.
    Splits the texts into lines and uses difflib to identify differences.
    """
    linesA = textA.splitlines()
    linesB = textB.splitlines()
    matcher = difflib.SequenceMatcher(None, linesA, linesB)
    blocks = []
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == 'equal':
            continue
        block = []
        block.append(f"{tag.upper()} in A[{i1}:{i2}] vs B[{j1}:{j2}]:")
        block.append("A:")
        a_lines = linesA[i1:i2]
        if len(a_lines) > max_lines_per_block:
            block.extend(a_lines[:max_lines_per_block])
            block.append(f"... (and {len(a_lines) - max_lines_per_block} more lines)")
        else:
            block.extend(a_lines)
        block.append("B:")
        b_lines = linesB[j1:j2]
        if len(b_lines) > max_lines_per_block:
            block.extend(b_lines[:max_lines_per_block])
            block.append(f"... (and {len(b_lines) - max_lines_per_block} more lines)")
        else:
            block.extend(b_lines)
        blocks.append("\n".join(block))
    if not blocks:
        return "No differences."
    if len(blocks) > max_blocks:
        blocks = blocks[:max_blocks] + [f"... and {len(blocks) - max_blocks} more difference blocks"]
    return "\n\n".join(blocks)

def safe_decode(value: bytes) -> str:
    """
    Attempt to decode a bytes object using multiple common encodings.
    Falls back to replacing errors if necessary.
    """
    for enc in ("utf-8", "cp949", "euc-kr"):
        try:
            return value.decode(enc)
        except UnicodeDecodeError:
            continue
    return value.decode("utf-8", errors="replace")

def is_long_text_column(index: int, columns_info: List[Any]) -> bool:
    """
    Determine if the column at the given index is a large text column.
    This is useful to decide whether to use checksum for comparison.
    """
    col = columns_info[index]
    data_type = col[1].lower()
    if data_type in {"blob", "longblob"}:
        return True
    if data_type == "varchar":
        m = re.search(r'varchar\((\d+)\)', col[4])
        if m:
            size = int(m.group(1))
            return size > 10000
    return False

#######################################
# Comparison Functions (Schema & Data)
#######################################
def compare_constraints(
    logger: logging.Logger,
    constraintsA: Dict[Any, Dict[str, Any]],
    constraintsB: Dict[Any, Dict[str, Any]],
    dbA_name: str,
    dbB_name: str,
    summary: Dict[str, int]
) -> None:
    """
    Compare table constraints between two databases.
    Logs differences and updates the summary counts.
    """
    summary.setdefault('constraints_differ', 0)
    keysA = set(constraintsA.keys())
    keysB = set(constraintsB.keys())
    only_in_A = keysA - keysB
    only_in_B = keysB - keysA
    common = keysA & keysB

    logger.info("\n" + "="*50)
    logger.info(">> CONSTRAINT COMPARISON BETWEEN %s AND %s", dbA_name, dbB_name)
    logger.info("="*50)
    summary['constraints_only_in_A'] = len(only_in_A)
    summary['constraints_only_in_B'] = len(only_in_B)
    summary['constraints_in_both'] = len(common)

    if only_in_A:
        logger.info("Constraints only in %s: %s", dbA_name, sorted(only_in_A))
    if only_in_B:
        logger.info("Constraints only in %s: %s", dbB_name, sorted(only_in_B))

    for key in common:
        if constraintsA[key] != constraintsB[key]:
            table_name, constraint_name = key
            logger.info("Difference in constraint '%s' on table '%s':", constraint_name, table_name)
            logger.info("  %s definition: %s", dbA_name, constraintsA[key])
            logger.info("  %s definition: %s", dbB_name, constraintsB[key])
            summary['constraints_differ'] += 1

def compare_schemas(
    logger: logging.Logger,
    schemaA: Dict[str, Any],
    schemaB: Dict[str, Any],
    dbA_name: str,
    dbB_name: str,
    summary: Dict[str, int],
    table_summaries: Dict[str, Dict[str, Any]]
) -> None:
    """
    Compare tables and columns between two databases.
    Logs differences in tables and column definitions, and updates summary stats.
    """
    summary.setdefault('tables_only_in_A', 0)
    summary.setdefault('tables_only_in_B', 0)
    summary.setdefault('tables_in_both', 0)
    summary.setdefault('columns_compared', 0)
    summary.setdefault('columns_only_in_A', 0)
    summary.setdefault('columns_only_in_B', 0)

    tablesA: Set[str] = set(schemaA.keys())
    tablesB: Set[str] = set(schemaB.keys())
    only_in_A = tablesA - tablesB
    only_in_B = tablesB - tablesA
    in_both = tablesA & tablesB

    logger.info("\n" + "="*50)
    logger.info(">> SCHEMA COMPARISON BETWEEN %s AND %s", dbA_name, dbB_name)
    logger.info("="*50)
    summary['tables_only_in_A'] = len(only_in_A)
    summary['tables_only_in_B'] = len(only_in_B)
    summary['tables_in_both'] = len(in_both)

    if only_in_A:
        logger.info("Tables only in %s (%d): %s", dbA_name, len(only_in_A), sorted(only_in_A))
    else:
        logger.info("No tables unique to %s", dbA_name)
    for table_name in only_in_A:
        table_summaries[table_name] = {
            'row_count_A': 0,
            'row_count_B': 0,
            'col_count_A': len(schemaA[table_name]),
            'col_count_B': 0,
            'rows_only_in_A': 0,
            'rows_only_in_B': 0,
            'rows_mismatched': 0,
            'checksum_A': None,
            'checksum_B': None
        }

    if only_in_B:
        logger.info("Tables only in %s (%d): %s", dbB_name, len(only_in_B), sorted(only_in_B))
    else:
        logger.info("No tables unique to %s", dbB_name)
    for table_name in only_in_B:
        table_summaries[table_name] = {
            'row_count_A': 0,
            'row_count_B': 0,
            'col_count_A': 0,
            'col_count_B': len(schemaB[table_name]),
            'rows_only_in_A': 0,
            'rows_only_in_B': 0,
            'rows_mismatched': 0,
            'checksum_A': None,
            'checksum_B': None
        }

    if in_both:
        logger.info("Tables in both (%d): %s", len(in_both), sorted(in_both))
    else:
        logger.info("No common tables between %s and %s", dbA_name, dbB_name)

    for tbl in sorted(in_both):
        colsA = set(schemaA[tbl])
        colsB = set(schemaB[tbl])
        summary['columns_compared'] += max(len(colsA), len(colsB))
        logger.info("Table '%s': %d columns in %s vs. %d columns in %s", tbl, len(colsA), dbA_name, len(colsB), dbB_name)
        diffA = colsA - colsB
        diffB = colsB - colsA
        if diffA or diffB:
            if diffA:
                logger.info("  -> Columns in '%s' only in %s: %s", tbl, dbA_name, sorted(diffA))
                summary['columns_only_in_A'] += len(diffA)
            if diffB:
                logger.info("  -> Columns in '%s' only in %s: %s", tbl, dbB_name, sorted(diffB))
                summary['columns_only_in_B'] += len(diffB)
        else:
            logger.info("  -> Columns match for table '%s'", tbl)

def compare_indexes(logger: logging.Logger, indexesA: Dict[Any, List[Any]], indexesB: Dict[Any, List[Any]],
                    dbA_name: str, dbB_name: str, summary: Dict[str, int]) -> None:
    """
    Compare index definitions between two databases.
    Logs any differences and updates summary counts.
    """
    keysA = set(indexesA.keys())
    keysB = set(indexesB.keys())
    only_in_A = keysA - keysB
    only_in_B = keysB - keysA
    common = keysA & keysB

    logger.info("\n" + "="*50)
    logger.info(">> INDEX COMPARISON BETWEEN %s AND %s", dbA_name, dbB_name)
    logger.info("="*50)
    summary['indexes_only_in_A'] = len(only_in_A)
    summary['indexes_only_in_B'] = len(only_in_B)
    summary['indexes_in_both'] = len(common)

    if only_in_A:
        logger.info("Indexes only in %s: %s", dbA_name, sorted(only_in_A))
    if only_in_B:
        logger.info("Indexes only in %s: %s", dbB_name, sorted(only_in_B))
    for key in common:
        if indexesA[key] != indexesB[key]:
            table_name, index_name = key
            logger.info("Difference in index '%s' on table '%s':", index_name, table_name)
            logger.info("  %s: %s", dbA_name, indexesA[key])
            logger.info("  %s: %s", dbB_name, indexesB[key])
            summary['indexes_differ'] = summary.get('indexes_differ', 0) + 1

def compare_triggers(logger: logging.Logger, triggersA: Dict[str, Dict[str, Any]], triggersB: Dict[str, Dict[str, Any]],
                     dbA_name: str, dbB_name: str, summary: Dict[str, int]) -> None:
    """
    Compare trigger definitions between the two databases.
    Uses normalization to ignore formatting differences and verifies that the correct
    database name is referenced.
    """
    keysA = set(triggersA.keys())
    keysB = set(triggersB.keys())
    only_in_A = keysA - keysB
    only_in_B = keysB - keysA
    common = keysA & keysB

    logger.info("\n" + "="*50)
    logger.info(">> TRIGGER COMPARISON BETWEEN %s AND %s", dbA_name, dbB_name)
    logger.info("="*50)
    summary['triggers_only_in_A'] = len(only_in_A)
    summary['triggers_only_in_B'] = len(only_in_B)
    summary['triggers_in_both'] = len(common)

    if only_in_A:
        logger.info("Triggers only in %s: %s", dbA_name, sorted(only_in_A))
    if only_in_B:
        logger.info("Triggers only in %s: %s", dbB_name, sorted(only_in_B))
    for trig in common:
        stmtA = triggersA[trig].get('statement', '')
        stmtB = triggersB[trig].get('statement', '')
        validA = check_db_name_in_definition(stmtA, dbA_name)
        validB = check_db_name_in_definition(stmtB, dbB_name)
        if not validA or not validB:
            logger.info("Trigger '%s' references an unexpected database name.", trig)
            if not validA:
                logger.info("  %s: %s", dbA_name, stmtA)
            if not validB:
                logger.info("  %s: %s", dbB_name, stmtB)
            summary['triggers_differ'] = summary.get('triggers_differ', 0) + 1
            continue
        normA = normalize_sql_definition(stmtA, dbA_name)
        normB = normalize_sql_definition(stmtB, dbB_name)
        if normA != normB:
            logger.info("Logic difference in trigger '%s':", trig)
            logger.info("  %s normalized: %s", dbA_name, normA)
            logger.info("  %s normalized: %s", dbB_name, normB)
            summary['triggers_differ'] = summary.get('triggers_differ', 0) + 1

def compare_routines(logger: logging.Logger, routinesA: Dict[Any, Dict[str, Any]], routinesB: Dict[Any, Dict[str, Any]],
                     dbA_name: str, dbB_name: str, summary: Dict[str, int]) -> None:
    """
    Compare stored routines (procedures/functions) between two databases.
    Uses normalization and checks for proper database name references.
    """
    keysA = set(routinesA.keys())
    keysB = set(routinesB.keys())
    only_in_A = keysA - keysB
    only_in_B = keysB - keysA
    common = keysA & keysB

    logger.info("\n" + "="*50)
    logger.info(">> ROUTINE COMPARISON BETWEEN %s AND %s", dbA_name, dbB_name)
    logger.info("="*50)
    summary['routines_only_in_A'] = len(only_in_A)
    summary['routines_only_in_B'] = len(only_in_B)
    summary['routines_in_both'] = len(common)

    if only_in_A:
        logger.info("Routines only in %s: %s", dbA_name, sorted(only_in_A))
    if only_in_B:
        logger.info("Routines only in %s: %s", dbB_name, sorted(only_in_B))
    for key in common:
        routine_name, routine_type = key
        defA = routinesA[key].get('definition', '')
        defB = routinesB[key].get('definition', '')
        validA = check_db_name_in_definition(defA, dbA_name)
        validB = check_db_name_in_definition(defB, dbB_name)
        if not validA or not validB:
            logger.info("Routine '%s' [%s] references an unexpected database name.", routine_name, routine_type)
            if not validA:
                logger.info("  %s: %s", dbA_name, defA)
            if not validB:
                logger.info("  %s: %s", dbB_name, defB)
            summary['routines_differ'] = summary.get('routines_differ', 0) + 1
            continue
        normA = normalize_sql_definition(defA, dbA_name)
        normB = normalize_sql_definition(defB, dbB_name)
        if normA != normB:
            logger.info("Logic difference in routine '%s' [%s]:", routine_name, routine_type)
            logger.info("  %s normalized: %s", dbA_name, normA)
            logger.info("  %s normalized: %s", dbB_name, normB)
            summary['routines_differ'] = summary.get('routines_differ', 0) + 1

def compare_views(
    logger: logging.Logger,
    viewsA: Dict[str, str],
    viewsB: Dict[str, str],
    dbA_name: str,
    dbB_name: str,
    summary: Dict[str, int]
) -> None:
    """
    Compare view definitions between two databases.
    Normalizes SQL definitions and checks for database name references.
    """
    summary.setdefault('views_only_in_A', 0)
    summary.setdefault('views_only_in_B', 0)
    summary.setdefault('views_in_both', 0)
    summary.setdefault('views_differ', 0)

    keysA = set(viewsA.keys())
    keysB = set(viewsB.keys())
    only_in_A = keysA - keysB
    only_in_B = keysB - keysA
    common = keysA & keysB

    logger.info("\n" + "="*50)
    logger.info(">> VIEW COMPARISON BETWEEN %s AND %s", dbA_name, dbB_name)
    logger.info("="*50)
    summary['views_only_in_A'] = len(only_in_A)
    summary['views_only_in_B'] = len(only_in_B)
    summary['views_in_both'] = len(common)

    if only_in_A:
        logger.info("Views only in %s (%d): %s", dbA_name, len(only_in_A), sorted(only_in_A))
    else:
        logger.info("No views unique to %s.", dbA_name)
    if only_in_B:
        logger.info("Views only in %s (%d): %s", dbB_name, len(only_in_B), sorted(only_in_B))
    else:
        logger.info("No views unique to %s.", dbB_name)
    if common:
        logger.info("Views in both (%d): %s", len(common), sorted(common))
    else:
        logger.info("No common views between %s and %s.", dbA_name, dbB_name)

    for view in sorted(common):
        defA = viewsA[view]
        defB = viewsB[view]
        validA = check_db_name_in_definition(defA, dbA_name)
        validB = check_db_name_in_definition(defB, dbB_name)
        if not validA or not validB:
            normA = normalize_sql_definition(defA, dbA_name)
            normB = normalize_sql_definition(defB, dbB_name)
            if normA == normB:
                pattern = re.compile(r'\b(?:from|join)\s+`?([^`\s\.]+)`?\.', flags=re.IGNORECASE)
                matchA = pattern.search(defA)
                matchB = pattern.search(defB)
                actualA = matchA.group(1) if matchA else "Not specified"
                actualB = matchB.group(1) if matchB else "Not specified"
                logger.info("View '%s' logic is identical, but database names differ:", view)
                logger.info("  Actual database in first query: %s (expected: %s)", actualA, dbA_name)
                logger.info("  Actual database in second query: %s (expected: %s)", actualB, dbB_name)
            else:
                logger.info("View '%s' references unexpected database name(s), and logic differs.", view)
                if not validA:
                    logger.info("  Expected %s in view '%s', but got:\n%s", dbA_name, view, defA)
                if not validB:
                    logger.info("  Expected %s in view '%s', but got:\n%s", dbB_name, view, defB)
                summary['views_differ'] += 1
            continue
        normA = normalize_sql_definition(defA, dbA_name)
        normB = normalize_sql_definition(defB, dbB_name)
        if normA != normB:
            logger.info("Logic difference in view '%s':", view)
            logger.info("  %s normalized:\n%s", dbA_name, normA)
            logger.info("  %s normalized:\n%s", dbB_name, normB)
            summary['views_differ'] += 1

def get_primary_key_columns(conn: pymysql.connections.Connection, db_name: str, table_name: str) -> List[str]:
    """
    Retrieve primary key column names for a given table.
    This is required to order rows and compare data at the row level.
    """
    query = f"""
        SELECT COLUMN_NAME
        FROM information_schema.key_column_usage
        WHERE table_schema = '{db_name}'
          AND table_name = '{table_name}'
          AND constraint_name = 'PRIMARY'
        ORDER BY ordinal_position;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
    return [row[0] for row in rows]

def compare_table_data(
    logger: logging.Logger,
    connA: pymysql.connections.Connection,
    connB: pymysql.connections.Connection,
    dbA_name: str,
    dbB_name: str,
    table_name: str,
    pk_cols: List[str],
    summary: Dict[str, int],
    table_summaries: Dict[str, Dict[str, Any]],
    columns_info: List[Any] = None
) -> None:
    """
    Compare row-level data for a specific table based on its primary key(s).
    Retrieves rows from both databases, normalizes long text columns by computing CRC32,
    and logs any differences.
    """
    if not pk_cols:
        logger.info("Skipping data comparison for %s (no primary key).", table_name)
        summary['tables_skipped_no_pk'] = summary.get('tables_skipped_no_pk', 0) + 1
        return

    # Create the ORDER BY clause based on the primary key columns.
    pk_order = ", ".join([f"`{col}`" for col in pk_cols])
    cursorA = connA.cursor()
    cursorB = connB.cursor()

    # Ensure we are using the correct database and then fetch all rows ordered by the primary key.
    cursorA.execute(f"USE `{dbA_name}`")
    cursorA.execute(f"SELECT * FROM `{table_name}` ORDER BY {pk_order}")
    rowsA = cursorA.fetchall()

    cursorB.execute(f"USE `{dbB_name}`")
    cursorB.execute(f"SELECT * FROM `{table_name}` ORDER BY {pk_order}")
    rowsB = cursorB.fetchall()

    descA = cursorA.description
    col_names = [d[0] for d in descA]
    pk_indices = [col_names.index(pk) for pk in pk_cols]

    # Determine indices of long text columns to compare using checksum instead of full content.
    long_text_indices = []
    if columns_info:
        for i, _ in enumerate(columns_info):
            if is_long_text_column(i, columns_info):
                long_text_indices.append(i)

    def normalize_row(row: tuple) -> tuple:
        """
        Normalize a row by converting long text values to a CRC32 checksum.
        This speeds up the comparison of very large text/blob columns.
        """
        row_list = list(row)
        for i in long_text_indices:
            value = row_list[i]
            if value is not None:
                if isinstance(value, (bytes, bytearray)):
                    row_list[i] = zlib.crc32(value)
                elif isinstance(value, str):
                    row_list[i] = zlib.crc32(value.encode('utf-8'))
        return tuple(row_list)

    # Build dictionaries keyed by the primary key(s) for both databases.
    dataA_norm = {}
    dataB_norm = {}
    dataA_orig = {}
    dataB_orig = {}
    for row in rowsA:
        key = row[pk_indices[0]] if len(pk_indices) == 1 else tuple(row[i] for i in pk_indices)
        dataA_norm[key] = normalize_row(row)
        dataA_orig[key] = row

    for row in rowsB:
        key = row[pk_indices[0]] if len(pk_indices) == 1 else tuple(row[i] for i in pk_indices)
        dataB_norm[key] = normalize_row(row)
        dataB_orig[key] = row

    keysA = set(dataA_norm.keys())
    keysB = set(dataB_norm.keys())
    only_in_A = keysA - keysB
    only_in_B = keysB - keysA
    in_both = keysA & keysB

    # Log any rows that exist only in one of the databases.
    if only_in_A or only_in_B:
        logger.info("---- Data differences in table '%s' ----", table_name)
        summary['tables_with_data_differences'] = summary.get('tables_with_data_differences', 0) + 1
        if only_in_A:
            logger.info("Rows only in %s: %s", dbA_name, list(only_in_A))
            summary['rows_only_in_A'] = summary.get('rows_only_in_A', 0) + len(only_in_A)
            table_summaries[table_name]['rows_only_in_A'] += len(only_in_A)
        if only_in_B:
            logger.info("Rows only in %s: %s", dbB_name, list(only_in_B))
            summary['rows_only_in_B'] = summary.get('rows_only_in_B', 0) + len(only_in_B)
            table_summaries[table_name]['rows_only_in_B'] += len(only_in_B)

    # Compare rows that exist in both databases.
    for key in in_both:
        if dataA_norm[key] != dataB_norm[key]:
            logger.info("---- Data mismatch in table '%s' for PK=%s ----", table_name, key)
            rowA = dataA_orig[key]
            rowB = dataB_orig[key]
            for i, col_name in enumerate(col_names):
                valA = rowA[i]
                valB = rowB[i]
                if valA != valB:
                    if columns_info and is_long_text_column(i, columns_info):
                        strA = safe_decode(valA) if isinstance(valA, bytes) else str(valA)
                        strB = safe_decode(valB) if isinstance(valB, bytes) else str(valB)
                        diff_summary = diff_location_summary(strA, strB)
                        logger.info("Column '%s' differences (detailed):\n%s", col_name, diff_summary)
                    else:
                        logger.info("Column '%s' differs. %s: %s | %s: %s", col_name, dbA_name, valA, dbB_name, valB)
            summary['rows_mismatched'] = summary.get('rows_mismatched', 0) + 1
            table_summaries[table_name]['rows_mismatched'] += 1

    cursorA.close()
    cursorB.close()

def compare_all_tables_data(
    logger: logging.Logger,
    connA: pymysql.connections.Connection,
    connB: pymysql.connections.Connection,
    dbA_name: str,
    dbB_name: str,
    schemaA: Dict[str, List[Any]],
    schemaB: Dict[str, List[Any]],
    summary: Dict[str, int],
    table_summaries: Dict[str, Dict[str, Any]]
) -> None:
    """
    Iterate over all tables that exist in both databases and compare their row-level data.
    Also computes checksums for a quick table-level comparison.
    """
    summary.setdefault('total_tables_compared', 0)
    summary.setdefault('total_rows_compared', 0)

    tablesA: Set[str] = set(schemaA.keys())
    tablesB: Set[str] = set(schemaB.keys())
    common_tables: Set[str] = tablesA & tablesB

    logger.info("\n" + "="*50)
    logger.info(">> DATA COMPARISON BETWEEN %s AND %s", dbA_name, dbB_name)
    logger.info("="*50)

    summary['total_tables_compared'] = len(common_tables)

    for table_name in sorted(common_tables):
        try:
            # Retrieve row counts for the current table from both databases.
            with connA.cursor() as curA, connB.cursor() as curB:
                curA.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                countA = curA.fetchone()[0] if curA.rowcount > 0 else 0

                curB.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                countB = curB.fetchone()[0] if curB.rowcount > 0 else 0

            logger.info("Table '%s': %s=%d rows, %s=%d rows",
                        table_name, dbA_name, countA, dbB_name, countB)

            # Gather column information from database A.
            col_info_A = schemaA[table_name]  # list of tuples: (col_name, data_type, ...)

            # Determine the common columns between the two schemas.
            columnsA = {col[0] for col in col_info_A}
            columnsB = {col[0] for col in schemaB[table_name]}
            common_cols = list(columnsA & columnsB)

            # Compute checksums for a quick verification of table content.
            checksumA = compute_table_checksum(connA, table_name, common_cols, logger)
            checksumB = compute_table_checksum(connB, table_name, common_cols, logger)

            table_summaries[table_name] = {
                'row_count_A': countA,
                'row_count_B': countB,
                'col_count_A': len(col_info_A),
                'col_count_B': len(schemaB[table_name]),
                'rows_only_in_A': 0,
                'rows_only_in_B': 0,
                'rows_mismatched': 0,
                'checksum_A': checksumA,
                'checksum_B': checksumB
            }

            summary['total_rows_compared'] += countA + countB

            # Retrieve primary keys (from DB A) to compare rows accurately.
            pk_cols = get_primary_key_columns(connA, dbA_name, table_name)
            
            # Compare row-level data using the primary keys.
            compare_table_data(
                logger, connA, connB,
                dbA_name, dbB_name,
                table_name, pk_cols,
                summary, table_summaries,
                col_info_A  # Pass schema information to handle "long text" columns.
            )
        except Exception as e:
            # Log any error encountered during data comparison and record it.
            logger.error("Error comparing data for table '%s': %s", table_name, e)
            table_summaries[table_name] = {
                'row_count_A': 0,
                'row_count_B': 0,
                'col_count_A': 0,
                'col_count_B': 0,
                'rows_only_in_A': 0,
                'rows_only_in_B': 0,
                'rows_mismatched': 0,
                'checksum_A': None,
                'checksum_B': None,
                'error': str(e)
            }
            continue

#######################################
# Export Functions (Excel & Word)
#######################################
def export_table_summary_to_excel(table_summaries: Dict[str, Dict[str, Any]], filename: str, dbA_name: str, dbB_name: str) -> None:
    """
    Export a summary of each table's comparison (row counts, column counts, checksums, etc.)
    to an Excel file using basic styling.
    """
    data = []
    for table, summary_data in table_summaries.items():
        row = {
            'Table': table,
            f'col_count_{dbA_name}': summary_data.get('col_count_A', 0),
            f'col_count_{dbB_name}': summary_data.get('col_count_B', 0),
            f'row_count_{dbA_name}': summary_data.get('row_count_A', 0),
            f'row_count_{dbB_name}': summary_data.get('row_count_B', 0),
            f'rows_only_in_{dbA_name}': summary_data.get('rows_only_in_A', 0),
            f'rows_only_in_{dbB_name}': summary_data.get('rows_only_in_B', 0),
            'rows_mismatched': summary_data.get('rows_mismatched', 0),
            'col_mismatch': abs(summary_data.get('col_count_A', 0) - summary_data.get('col_count_B', 0)),
            f'checksum_{dbA_name}': summary_data.get('checksum_A'),
            f'checksum_{dbB_name}': summary_data.get('checksum_B')
        }
        data.append(row)
    df = pd.DataFrame(data)
    df.sort_values(by='Table', inplace=True)

    with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Summary')
        workbook = writer.book
        worksheet = writer.sheets['Summary']
        header_format = workbook.add_format({'bold': True, 'bg_color': '#DCE6F1', 'border': 1})
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)
        for i, col in enumerate(df.columns):
            col_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
            worksheet.set_column(i, i, col_len)

def export_summary_to_word(summary: Dict[str, int], table_summaries: Dict[str, Dict[str, Any]], filename: str, dbA_name: str, dbB_name: str) -> None:
    """
    Export an overall summary and a detailed table-by-table breakdown to a Word document.
    If python-docx is not installed, an error is logged and the export is skipped.
    """
    if Document is None:
        logging.error("python-docx is not installed. Skipping Word export.")
        return

    document = Document()

    # Set default font to Times New Roman.
    style = document.styles['Normal']
    font = style.font
    font.name = 'Times New Roman'
    font.size = Pt(11)

    document.add_heading('Database Comparison Summary', 0)

    document.add_heading('Overall Schema Comparison', level=1)
    schema_paragraph = document.add_paragraph()
    schema_paragraph.add_run(f"Tables only in {dbA_name}: {summary.get('tables_only_in_A', 0)}\n")
    schema_paragraph.add_run(f"Tables only in {dbB_name}: {summary.get('tables_only_in_B', 0)}\n")
    schema_paragraph.add_run(f"Tables in both DBs: {summary.get('tables_in_both', 0)}\n")
    schema_paragraph.add_run(f"Columns compared: {summary.get('columns_compared', 0)}\n")
    schema_paragraph.add_run(f"Columns only in {dbA_name}: {summary.get('columns_only_in_A', 0)}\n")
    schema_paragraph.add_run(f"Columns only in {dbB_name}: {summary.get('columns_only_in_B', 0)}\n")
    schema_paragraph.add_run(f"Indexes differing: {summary.get('indexes_differ', 0)}\n")
    schema_paragraph.add_run(f"Triggers differing: {summary.get('triggers_differ', 0)}\n")
    schema_paragraph.add_run(f"Routines differing: {summary.get('routines_differ', 0)}\n")
    schema_paragraph.add_run(f"Constraints differing: {summary.get('constraints_differ', 0)}\n")
    schema_paragraph.add_run(f"Views differing: {summary.get('views_differ', 0)}\n")

    document.add_heading('Data Comparison', level=1)
    data_paragraph = document.add_paragraph()
    data_paragraph.add_run(f"Total tables compared: {summary.get('total_tables_compared', 0)}\n")
    data_paragraph.add_run(f"Total rows compared: {summary.get('total_rows_compared', 0)}\n")
    data_paragraph.add_run(f"Tables skipped (no primary key): {summary.get('tables_skipped_no_pk', 0)}\n")
    data_paragraph.add_run(f"Tables with data differences: {summary.get('tables_with_data_differences', 0)}\n")
    data_paragraph.add_run(f"Rows only in {dbA_name}: {summary.get('rows_only_in_A', 0)}\n")
    data_paragraph.add_run(f"Rows only in {dbB_name}: {summary.get('rows_only_in_B', 0)}\n")
    data_paragraph.add_run(f"Rows mismatched: {summary.get('rows_mismatched', 0)}\n")

    document.add_heading('Table-wise Summary', level=1)
    num_cols = 11
    summary_table = document.add_table(rows=1, cols=num_cols)
    summary_table.style = 'Table Grid'
    summary_table.alignment = WD_TABLE_ALIGNMENT.CENTER
    hdr_cells = summary_table.rows[0].cells
    headers = [
        'Table',
        f'col_count_{dbA_name}',
        f'col_count_{dbB_name}',
        f'row_count_{dbA_name}',
        f'row_count_{dbB_name}',
        f'rows_only_in_{dbA_name}',
        f'rows_only_in_{dbB_name}',
        'rows_mismatched',
        'col_mismatch',
        f'checksum_{dbA_name}',
        f'checksum_{dbB_name}'
    ]
    for i, header in enumerate(headers):
        hdr_cells[i].text = header

    for tname, data in sorted(table_summaries.items()):
        row_cells = summary_table.add_row().cells
        row_cells[0].text = str(tname)
        row_cells[1].text = str(data.get('col_count_A', 0))
        row_cells[2].text = str(data.get('col_count_B', 0))
        row_cells[3].text = str(data.get('row_count_A', 0))
        row_cells[4].text = str(data.get('row_count_B', 0))
        row_cells[5].text = str(data.get('rows_only_in_A', 0))
        row_cells[6].text = str(data.get('rows_only_in_B', 0))
        row_cells[7].text = str(data.get('rows_mismatched', 0))
        row_cells[8].text = str(abs(data.get('col_count_A', 0) - data.get('col_count_B', 0)))
        row_cells[9].text = str(data.get('checksum_A'))
        row_cells[10].text = str(data.get('checksum_B'))

    for row in summary_table.rows:
        for cell in row.cells:
            for paragraph in cell.paragraphs:
                for run in paragraph.runs:
                    run.font.name = 'Times New Roman'
                    run.font.size = Pt(10)

    document.save(filename)

def generate_summary_log(summary_logger: logging.Logger, summary: Dict[str, int],
                         table_summaries: Dict[str, Dict[str, Any]]) -> None:
    """
    Log a comprehensive summary of the database comparison.
    This summary is written to a dedicated log file.
    """
    summary_logger.info("\n" + "="*50)
    summary_logger.info(">> DATABASE COMPARISON SUMMARY")
    summary_logger.info("="*50)
    summary_logger.info("Schema Comparison:")
    summary_logger.info("  - Tables only in DB A: %d", summary.get('tables_only_in_A', 0))
    summary_logger.info("  - Tables only in DB B: %d", summary.get('tables_only_in_B', 0))
    summary_logger.info("  - Tables in both DBs: %d", summary.get('tables_in_both', 0))
    summary_logger.info("  - Columns compared: %d", summary.get('columns_compared', 0))
    summary_logger.info("  - Columns only in DB A: %d", summary.get('columns_only_in_A', 0))
    summary_logger.info("  - Columns only in DB B: %d", summary.get('columns_only_in_B', 0))
    summary_logger.info("  - Indexes differing: %d", summary.get('indexes_differ', 0))
    summary_logger.info("  - Triggers differing: %d", summary.get('triggers_differ', 0))
    summary_logger.info("  - Routines differing: %d", summary.get('routines_differ', 0))
    summary_logger.info("  - Constraints differing: %d", summary.get('constraints_differ', 0))
    summary_logger.info("  - Views differing: %d", summary.get('views_differ', 0))
    summary_logger.info("\nData Comparison:")
    summary_logger.info("  - Total tables compared: %d", summary.get('total_tables_compared', 0))
    summary_logger.info("  - Total rows compared: %d", summary.get('total_rows_compared', 0))
    summary_logger.info("  - Tables skipped (no primary key): %d", summary.get('tables_skipped_no_pk', 0))
    summary_logger.info("  - Tables with data differences: %d", summary.get('tables_with_data_differences', 0))
    summary_logger.info("  - Rows only in DB A: %d", summary.get('rows_only_in_A', 0))
    summary_logger.info("  - Rows only in DB B: %d", summary.get('rows_only_in_B', 0))
    summary_logger.info("  - Rows mismatched: %d", summary.get('rows_mismatched', 0))
    summary_logger.info("="*50)
    summary_logger.info("\n--- Table-wise Summary ---")
    for table in sorted(table_summaries.keys()):
        data = table_summaries[table]
        summary_logger.info(
            "Table '%s': col_count_DB_A=%d, col_count_DB_B=%d, row_count_A=%d, row_count_B=%d, "
            "rows_only_in_A=%d, rows_only_in_B=%d, rows_mismatched=%d, col_mismatch=%d, "
            "checksum_A=%s, checksum_B=%s",
            table,
            data['col_count_A'],
            data['col_count_B'],
            data['row_count_A'],
            data['row_count_B'],
            data['rows_only_in_A'],
            data['rows_only_in_B'],
            data['rows_mismatched'],
            abs(data['col_count_A'] - data['col_count_B']),
            data.get('checksum_A'),
            data.get('checksum_B')
        )

#######################################
# Main Entry Point
#######################################
def main() -> None:
    """
    Main function:
      - Parses command-line arguments
      - Connects to both MySQL databases
      - Retrieves schema objects and optionally filters them
      - Compares schema and data between the two databases
      - Logs detailed comparison information
      - Exports summary reports to Excel and Word files
      - Prints a user-friendly summary to the console
    """
    global global_dbA_name, global_dbB_name

    parser = argparse.ArgumentParser(description="Database Comparison Tool")
    parser.add_argument('--tables', nargs='*', metavar='OBJECT', help='Specify tables/views/procedures to compare (optional)')
    parser.add_argument('--table-file', type=str, metavar='FILE', help='Text file containing object names (one per line)')
    parser.add_argument('--verbose', action='store_true', help='Print user-friendly log messages to console')
    args = parser.parse_args()

    allowed_objects: Optional[Set[str]] = None
    if args.table_file:
        if os.path.exists(args.table_file):
            with open(args.table_file, 'r', encoding='utf-8') as f:
                objects_from_file = {line.strip() for line in f if line.strip()}
            if objects_from_file:
                allowed_objects = objects_from_file
                print(f"Using objects from file {args.table_file}: {sorted(allowed_objects)}")
            else:
                print(f"Object file {args.table_file} is empty. Comparing all objects.")
        else:
            print(f"Object file {args.table_file} does not exist. Comparing all objects.")
    elif args.tables:
        allowed_objects = set(args.tables)
        print(f"Using objects from command-line argument: {sorted(allowed_objects)}")
    else:
        print("No specific objects provided. Comparing all objects.")

    # Initialize summary dictionaries for schema and data differences.
    summary: Dict[str, int] = {
        'tables_only_in_A': 0, 'tables_only_in_B': 0, 'tables_in_both': 0,
        'columns_compared': 0, 'columns_only_in_A': 0, 'columns_only_in_B': 0,
        'indexes_only_in_A': 0, 'indexes_only_in_B': 0, 'indexes_in_both': 0, 'indexes_differ': 0,
        'triggers_only_in_A': 0, 'triggers_only_in_B': 0, 'triggers_in_both': 0, 'triggers_differ': 0,
        'routines_only_in_A': 0, 'routines_only_in_B': 0, 'routines_in_both': 0, 'routines_differ': 0,
        'constraints_only_in_A': 0, 'constraints_only_in_B': 0, 'constraints_in_both': 0, 'constraints_differ': 0,
        'views_only_in_A': 0, 'views_only_in_B': 0, 'views_in_both': 0, 'views_differ': 0,
        'total_tables_compared': 0, 'total_rows_compared': 0,
        'tables_skipped_no_pk': 0, 'tables_with_data_differences': 0,
        'rows_only_in_A': 0, 'rows_only_in_B': 0, 'rows_mismatched': 0
    }
    table_summaries: Dict[str, Dict[str, Any]] = {}

    # Define the names of the two databases to compare.
    dbA_name = ""   # First database name.
    dbB_name = ""        # Second database name.
    global_dbA_name = dbA_name
    global_dbB_name = dbB_name

    print(f"Comparing databases: {dbA_name} vs {dbB_name}")
    # Set up separate loggers for schema, data, and summary logs.
    schema_logger = setup_logger("schemaLogger", schema_log_filename, verbose=args.verbose)
    all_logger = setup_logger("allLogger", all_log_filename, verbose=args.verbose)
    summary_logger = setup_logger("summaryLogger", summary_log_filename, verbose=args.verbose)

    # If filtering is applied, log the filtered objects.
    if allowed_objects is not None:
        schema_logger.info("Comparing only the following objects: %s", sorted(allowed_objects))

    # Connect to both databases using provided credentials.
    connA = get_mysql_connection(
        host="",
        user="",
        password="",
        db=dbA_name,
        port=3306
    )
    connB = get_mysql_connection(
        host="",
        user="",
        password="",
        db=dbB_name,
        port=3306
    )

    try:
        # Retrieve table schemas (tables and columns) from both databases.
        schemaA = get_tables_and_columns(connA, dbA_name)
        schemaB = get_tables_and_columns(connB, dbB_name)

        # Filter schemas if a list of allowed objects is provided.
        if allowed_objects is not None:
            schemaA = {t: cols for t, cols in schemaA.items() if t in allowed_objects}
            schemaB = {t: cols for t, cols in schemaB.items() if t in allowed_objects}

        # Compare table schemas and log differences.
        compare_schemas(schema_logger, schemaA, schemaB, dbA_name, dbB_name, summary, table_summaries)

        # Retrieve and compare indexes.
        indexesA = get_indexes(connA, dbA_name)
        indexesB = get_indexes(connB, dbB_name)
        if allowed_objects is not None:
            indexesA = {k: v for k, v in indexesA.items() if k[0] in allowed_objects}
            indexesB = {k: v for k, v in indexesB.items() if k[0] in allowed_objects}
        compare_indexes(schema_logger, indexesA, indexesB, dbA_name, dbB_name, summary)

        # Retrieve and compare triggers.
        triggersA = get_triggers(connA, dbA_name)
        triggersB = get_triggers(connB, dbB_name)
        if allowed_objects is not None:
            triggersA = {k: v for k, v in triggersA.items() if v.get('table') in allowed_objects}
            triggersB = {k: v for k, v in triggersB.items() if v.get('table') in allowed_objects}
        compare_triggers(schema_logger, triggersA, triggersB, dbA_name, dbB_name, summary)

        # Retrieve and compare stored routines.
        routinesA = get_routines(connA, dbA_name)
        routinesB = get_routines(connB, dbB_name)
        if allowed_objects is not None:
            routinesA = {k: v for k, v in routinesA.items() if k[0] in allowed_objects}
            routinesB = {k: v for k, v in routinesB.items() if k[0] in allowed_objects}
        compare_routines(schema_logger, routinesA, routinesB, dbA_name, dbB_name, summary)

        # Retrieve and compare table constraints.
        constraintsA = get_constraints(connA, dbA_name)
        constraintsB = get_constraints(connB, dbB_name)
        if allowed_objects is not None:
            constraintsA = {k: v for k, v in constraintsA.items() if k[0] in allowed_objects}
            constraintsB = {k: v for k, v in constraintsB.items() if k[0] in allowed_objects}
        compare_constraints(schema_logger, constraintsA, constraintsB, dbA_name, dbB_name, summary)

        # Retrieve and compare views.
        viewsA = get_views(connA, dbA_name)
        viewsB = get_views(connB, dbB_name)
        if allowed_objects is not None:
            viewsA = {k: v for k, v in viewsA.items() if k in allowed_objects}
            viewsB = {k: v for k, v in viewsB.items() if k in allowed_objects}
        compare_views(schema_logger, viewsA, viewsB, dbA_name, dbB_name, summary)

        schema_logger.info(">> Schema Comparison Completed.")

        # Compare data for tables that exist in both databases.
        compare_all_tables_data(all_logger, connA, connB, dbA_name, dbB_name, schemaA, schemaB, summary, table_summaries)
        all_logger.info(">> Data Comparison Completed.")

        # Generate a summary log and export summary reports.
        generate_summary_log(summary_logger, summary, table_summaries)
        export_table_summary_to_excel(table_summaries, f"table_summary_{timestamp}.xlsx", dbA_name, dbB_name)
        export_summary_to_word(summary, table_summaries, f"summary_report_{timestamp}.docx", dbA_name, dbB_name)

        # Print a user-friendly summary to the console.
        print("\n=== DATABASE COMPARISON SUMMARY ===")
        print(f"Tables only in {dbA_name}: {summary.get('tables_only_in_A', 0)}")
        print(f"Tables only in {dbB_name}: {summary.get('tables_only_in_B', 0)}")
        print(f"Tables in both databases: {summary.get('tables_in_both', 0)}")
        print(f"Total tables compared: {summary.get('total_tables_compared', 0)}")
        print(f"Total rows compared: {summary.get('total_rows_compared', 0)}")
        print(f"Rows mismatched: {summary.get('rows_mismatched', 0)}")
        print("Detailed logs can be found in the generated log files.")
        print("Excel and Word summary reports have been exported.")

    except Exception as e:
        # Log any exception that occurs during the comparison process.
        schema_logger.error("(Schema) An error occurred during comparison: %s", e)
        all_logger.error("(All) An error occurred during comparison: %s", e)
        summary_logger.error("An error occurred: %s", e)
        print(f"An error occurred: {e}")
    finally:
        # Always close database connections.
        connA.close()
        connB.close()

if __name__ == "__main__":
    main()
