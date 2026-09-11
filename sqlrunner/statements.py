"""Splitting a .sql file into executable statements, and classifying what each one is.

This is the only part of the runner with interesting failure modes, so it is a pure function over a
string with no Flink import anywhere near it.

Flink's `execute_sql` takes exactly one statement. Splitting on ";" is the obvious implementation
and it is wrong in three ways that all fail silently rather than loudly:

  * a semicolon inside a string literal ('a;b') cuts the statement in half, and the fragments may
    still parse — Flink then runs something that is not what was written;
  * a semicolon inside a `--` comment does the same, and the tail of the comment becomes SQL;
  * a trailing empty fragment after the final ";" is submitted as an empty statement.

None of these can happen in today's four files. All of them become reachable the moment someone
writes a literal containing punctuation, which is exactly the kind of change nobody reviews closely.
"""

# Flink escapes a quote inside a string literal by doubling it: 'it''s'. Since the doubled quote
# reads as close-then-open to the scanner below, no special case is needed — the state ends up
# right either way.
_QUOTE = "'"
_IDENT = "`"


def split_statements(text: str) -> list[str]:
    """Split SQL text into statements, respecting string literals, identifiers and comments."""
    statements = []
    current = []
    in_string = False
    in_ident = False
    in_line_comment = False
    in_block_comment = False
    index = 0

    while index < len(text):
        char = text[index]
        pair = text[index : index + 2]

        if in_line_comment:
            if char == "\n":
                in_line_comment = False
            current.append(char)
        elif in_block_comment:
            if pair == "*/":
                in_block_comment = False
                current.append(pair)
                index += 2
                continue
            current.append(char)
        elif in_string:
            if char == _QUOTE:
                in_string = False
            current.append(char)
        elif in_ident:
            if char == _IDENT:
                in_ident = False
            current.append(char)
        elif pair == "--":
            in_line_comment = True
            current.append(pair)
            index += 2
            continue
        elif pair == "/*":
            in_block_comment = True
            current.append(pair)
            index += 2
            continue
        elif char == _QUOTE:
            in_string = True
            current.append(char)
        elif char == _IDENT:
            in_ident = True
            current.append(char)
        elif char == ";":
            statements.append("".join(current))
            current = []
        else:
            current.append(char)
        index += 1

    statements.append("".join(current))

    # An unterminated literal means the file is malformed and every statement after the opening
    # quote has been swallowed into one. Failing here is the whole point: the alternative is
    # submitting that blob to Flink and reading its parser error instead.
    if in_string or in_ident:
        raise ValueError("unterminated string literal or quoted identifier in SQL")

    return [stripped for stripped in (strip_comments(s) for s in statements) if stripped]


def strip_comments(statement: str) -> str:
    """Remove leading whitespace and comment-only lines, and report whether anything is left.

    A statement that is nothing but the header comment above a real statement is not a statement,
    and submitting it raises a parse error that says nothing about which file it came from.
    """
    lines = []
    for line in statement.splitlines():
        if line.strip().startswith("--"):
            continue
        lines.append(line)
    return "\n".join(lines).strip()


def is_insert(statement: str) -> bool:
    """True for statements that produce a job rather than a catalog entry.

    Inserts are collected into one statement set so that all four outputs share a single job, a
    single checkpoint and a single restart. Running them one at a time would produce four jobs, each
    with its own Kafka connection and its own independent failure — and then `fast` and `settled`
    could restart at different times and disagree for reasons that have nothing to do with
    watermarks.
    """
    return statement.lstrip().upper().startswith("INSERT ")
