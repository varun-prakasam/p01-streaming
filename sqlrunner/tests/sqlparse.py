"""Just enough SQL parsing to let the tests compare files to each other.

Deliberately not a SQL parser. It knows about parentheses, quotes and comments — which is all that
is needed to pull a column list or a select list out of the committed files, and all that can be
maintained without becoming its own project.
"""

import re

from sqlrunner.statements import split_statements


def read(path):
    with open(path) as handle:
        return handle.read()


def strip_comments(text):
    """Remove -- comments, leaving string literals alone."""
    out = []
    index = 0
    in_string = False
    while index < len(text):
        char = text[index]
        if in_string:
            if char == "'":
                in_string = False
            out.append(char)
        elif char == "'":
            in_string = True
            out.append(char)
        elif text[index : index + 2] == "--":
            while index < len(text) and text[index] != "\n":
                index += 1
            continue
        else:
            out.append(char)
        index += 1
    return "".join(out)


def split_top_level(text, separator=","):
    """Split on a separator that is not inside parentheses, quotes or backticks."""
    parts = []
    current = []
    depth = 0
    quote = None
    for char in text:
        if quote:
            if char == quote:
                quote = None
            current.append(char)
        elif char in "'\"`":
            quote = char
            current.append(char)
        elif char == "(":
            depth += 1
            current.append(char)
        elif char == ")":
            depth -= 1
            current.append(char)
        elif char == separator and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
    parts.append("".join(current))
    return [part.strip() for part in parts if part.strip()]


def paren_block(text, start):
    """The contents of the parenthesised block beginning at or after `start`."""
    open_at = text.index("(", start)
    depth = 0
    quote = None
    for index in range(open_at, len(text)):
        char = text[index]
        if quote:
            if char == quote:
                quote = None
        elif char in "'\"`":
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return text[open_at + 1 : index]
    raise ValueError("unbalanced parentheses")


def flink_tables(path):
    """{table name: [column names]} for every CREATE TABLE in a Flink SQL file."""
    tables = {}
    for statement in split_statements(read(path)):
        match = re.match(r"CREATE\s+TABLE\s+(\w+)\s*\(", strip_comments(statement), re.I)
        if not match:
            continue
        body = paren_block(strip_comments(statement), match.end() - 1)
        columns = []
        for item in split_top_level(body):
            name = item.split()[0]
            # WATERMARK and PRIMARY KEY are constraints, not columns.
            if name.upper() in ("WATERMARK", "PRIMARY", "CONSTRAINT"):
                continue
            columns.append(name.strip("`"))
        tables[match.group(1)] = columns
    return tables


def bigquery_tables(path):
    """{fully qualified table: [column names]} for every CREATE TABLE in the BigQuery DDL."""
    tables = {}
    text = strip_comments(read(path))
    for match in re.finditer(r"CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+`([^`]+)`\s*\(", text, re.I):
        body = paren_block(text, match.end() - 1)
        columns = [item.split()[0].strip("`") for item in split_top_level(body)]
        tables[match.group(1)] = columns
    return tables


def inserts(path):
    """{target table: {'select': [expressions], 'from': str, 'body': str}} per INSERT."""
    found = {}
    for statement in split_statements(read(path)):
        clean = strip_comments(statement)
        match = re.match(r"INSERT\s+INTO\s+(\w+)\s", clean, re.I)
        if not match:
            continue
        after_select = clean[clean.upper().index("SELECT") + len("SELECT") :]
        parts = split_top_level(after_select, separator="\n")
        # Find the FROM that is not nested: split_top_level on newlines keeps depth, so the first
        # line starting with FROM at depth zero is the real one.
        select_lines, rest = [], []
        seen_from = False
        for line in parts:
            if not seen_from and re.match(r"FROM\b", line, re.I):
                seen_from = True
            (rest if seen_from else select_lines).append(line)
        found[match.group(1)] = {
            "select": split_top_level("\n".join(select_lines)),
            "from": "\n".join(rest),
            "body": clean,
        }
    return found


def watermark_seconds(path):
    """{source table: watermark delay in seconds} parsed from the WATERMARK clauses."""
    units = {"SECOND": 1, "MINUTE": 60, "HOUR": 3600}
    result = {}
    for statement in split_statements(read(path)):
        clean = strip_comments(statement)
        table = re.match(r"CREATE\s+TABLE\s+(\w+)\s*\(", clean, re.I)
        mark = re.search(r"WATERMARK\s+FOR\s+(\w+)\s+AS\s+\w+\s*-\s*INTERVAL\s+'(\d+)'\s+(\w+)", clean, re.I)
        if table and mark:
            result[table.group(1)] = {
                "column": mark.group(1),
                "seconds": int(mark.group(2)) * units[mark.group(3).upper()],
            }
    return result
