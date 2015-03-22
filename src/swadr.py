#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

import csv
import getopt
import io
import itertools
import logging
import os
import re
import sqlite3
import string
import sys
import textwrap
import time

try:
    import readline
except ImportError:
    pass


PYTHON_3 = sys.version_info >= (3, )
EXIT_GENERAL_FAILURE = 1
EXIT_DATABASE_ERROR = 2

__all__ = ["PYTHON_3", "EXIT_GENERAL_FAILURE", "EXIT_DATABASE_ERROR",
    "SQLite3CSVImporter", "pretty_print_table", "query_split",
    "metaquery_conversion", "sqlite3_repl"]
__license__ = "BSD 2-Clause"


class SQLite3CSVImporter:
    sniffer = csv.Sniffer()
    typemap = [
        ("INTEGER", int),
        ("REAL", float),
        ("TEXT", (lambda v: v.encode("utf-8")) if PYTHON_3 else unicode),
        ("BLOB", (lambda v: v.encode("utf-8", errors="surrogateescape"))
                  if PYTHON_3 else str),
    ]

    def __init__(self, dbc, ignore_errors=True, log_warnings=True):
        """
        Setup SQLite3CSVImporter. When `ignore_errors` is set, any SQL errors
        encountered while inserting rows into the database will be ignored and,
        if `log_warnings` is set, a warning containing information about the
        failed INSERT will be logged.
        """
        self.dbc = dbc
        self.ignore_errors = ignore_errors
        self.log_warnings = log_warnings

    @classmethod
    def detect_types(cls, table):
        """
        Return list of SQL type definition clauses that can safely be applied
        to each of the columns in the `table`.
        """
        typedefs = list()
        for column in zip(*table):
            rows_with_content = [value for value in column if value]
            if not rows_with_content:
                scanned_columns = ("", )
            else:
                scanned_columns = rows_with_content

            for typedef, caster in cls.typemap:
                try:
                    for value in scanned_columns:
                        caster(value)
                    colschema = typedef
                    break
                except Exception:
                    pass
            else:
                raise ValueError("Could not detect type of %r" % (column,))

            typedefs.append(typedef)

        return typedefs

    @staticmethod
    def quote_identifier(identifier):
        """
        Return ANSI-quoted SQL identifier.
        """
        return '"' + identifier.replace('"', '""') + '"'

    def create_table(self, tablename, types, columns=None, if_not_exists=True):
        """
        Create a table named `tablename` with a column named after each element
        in `columns` with corresponding type defintions in the `types` list. If
        `columns` is not specified, the column names will be generated
        automatically. When `if_not_exists` is set, the "IF NOT EXISTS" infix
        will be added to the "CREATE TABLE" query.
        """
        if not columns:
            for char in tablename:
                if char.isalpha():
                    char = char.lower()
                    break
            else:
                char = "n"

            columns = (char + str(n) for n in itertools.count(1))

        else:
            # Restrict column identifiers to "word" characters.
            _columns = list()
            for column in columns:
                word_column = re.sub("\W+", "_", column, re.M | re.U).strip("_")
                column = word_column
                base = 1
                while column in _columns:
                    base += 1
                    column = word_column + "_" + str(base)
                _columns.append(column)

            columns = _columns

        if not types:
            raise ValueError("Must specify types.")

        columns = (self.quote_identifier(column) for column in columns)
        table = self.quote_identifier(tablename)
        body = ",\n  ".join(("%s %s" % (c, t) for c, t in zip(columns, types)))
        infix = "IF NOT EXISTS " if if_not_exists else ""

        cursor = self.dbc.cursor()
        cursor.execute("CREATE TABLE %s%s (\n  %s\n)" % (infix, table, body))

    def loadfile(self, filename, tablename, create_table=True):
        """
        Load a CSV file into the specified database table. When `create_table`
        is set, this method will auto-detect the CSV schema and create the
        `tablename` if it does not already exist. Please note that this method
        **will not** work on un-seekable files in Python 3.
        """
        def csv_open(path):
            """
            Open `path` in a manner best suited for use with csv module.
            """
            if PYTHON_3:
                # https://docs.python.org/3/library/csv.html#csv.reader
                return open(path, newline="", errors="surrogateescape")
            else:
                return open(path, mode="rbU")

        with csv_open(filename) as iostream:
            # Use first 20 lines to determine CSV dialect.
            sample_lines = "".join(itertools.islice(iostream, 20))
            dialect = self.sniffer.sniff(sample_lines)

            # In Python 2, this method supports reading data from unseekable
            # files by buffering the sampled data into a BytesIO object. I
            # could not figure out how to get BytesIO in Python 3 to play
            # nicely with the csv module, so I gave up supporting unseekable
            # files in Python 3.
            if PYTHON_3:
                sample_reader_io = iostream
            else:
                sample_reader_io = io.BytesIO(sample_lines)

            # Read the first 20 CSV records.
            sample_reader_io.seek(0)
            sample_reader = csv.reader(sample_reader_io, dialect)
            sample_rows = list(itertools.islice(sample_reader, 20))

            # Figure out the table schema using the sniffed records.
            sample_reader_io.seek(0)
            types_with_row_one = self.detect_types(sample_rows)
            types_sans_row_one = self.detect_types(sample_rows[1:])
            has_header = types_sans_row_one != types_with_row_one
            types = types_sans_row_one or types_with_row_one

            if has_header:
                try:
                    next(sample_reader)
                except StopIteration:
                    pass
                first_line_number = 2
                columns = sample_rows[0]

            else:
                first_line_number = 1
                columns = None

            with self.dbc:
                cursor = self.dbc.cursor()
                if create_table:
                    self.create_table(tablename, columns=columns, types=types)

                stream_reader = csv.reader(iostream, dialect)
                rowgen = itertools.chain(sample_reader, stream_reader)
                table = self.quote_identifier(tablename)
                binds = ", ".join("?" * len(sample_rows[0]))
                query = "INSERT INTO %s VALUES (%s)" % (tablename, binds)

                try:
                    original_text_factory = self.dbc.text_factory
                    if not PYTHON_3:
                        self.dbc.text_factory = str

                    for lineno, row in enumerate(rowgen, first_line_number):
                        parameters = [val if val else None for val in row]
                        logging.debug("Inserting row: %r", parameters)

                        try:
                            cursor.execute(query, parameters)

                        except Exception as e:
                            if not self.ignore_errors or self.log_warnings:
                                if not e.args:
                                    e.args = ("", )
                                suffix = " (%s, row %d) " % (filename, lineno)
                                e.args = e.args[:-1] + (e.args[-1] + suffix,)

                            if not self.ignore_errors:
                                self.dbc.text_factory = original_text_factory
                                raise
                            elif self.log_warnings:
                                logging.warning("%s", e)

                finally:
                    self.dbc.text_factory = original_text_factory


def pretty_print_table(table, breakafter=[0], dest=None, tabsize=8):
    """
    Pretty-print data from a table in a style similar to MySQL CLI. The
    `breakafter` option is used to determine where row-breaks should be
    inserted. When set to `False`, no breaks will be inserted after any
    rows. When set to `True`, a break is set after everywhere. The
    `breakafter` option can also be an iterable containing row numbers
    after which a break should be inserted. Assuming the first entry in
    `table` is the tabular data's header, the function can be executed as
    follows to insert a break just after the header:

    >>> table = [
    ... ["Name", "Age", "Favorite Color"],
    ... ["Bob", 10, "Blue"],
    ... ["Rob", 25, "Red"],
    ... ["Penny", 70, "Purple"]]
    >>> pretty_print_table(table)
    +-------+-----+----------------+
    | Name  | Age | Favorite Color |
    +-------+-----+----------------+
    | Bob   | 10  | Blue           |
    | Rob   | 25  | Red            |
    | Penny | 70  | Purple         |
    +-------+-----+----------------+

    By default, the table is printed to stdout, but this can be changed by
    providing a file-like object as the `dest` parameter.

    The `tabsize` parameter controls how many spaces tabs are expanded to.
    """
    # TODO: Implement optional support for the wcwidth module.
    table = list(table)
    last = len(table) - 1
    colwidths = list()
    table_lines = list()
    for rowindex, row in enumerate(table):
        # Split each cell into lines
        cells = list()
        for column in row:
            if column is None:
                column = "NULL"
            else:
                if not PYTHON_3 and not isinstance(column, unicode):
                    column = str(column)

                column = column.expandtabs(tabsize)

            cells.append(column.split("\n"))

        # Check if row-break should be inserted after row
        separate = ((breakafter is True) or
                    (rowindex == last) or
                    (breakafter and rowindex in breakafter))

        # Find tallest cell in the row
        row_height = max(map(len, cells))

        # Update the column widths if any of the cells are wider than the
        # widest, previously encountered cell in each column.
        initialize = not table_lines
        for index, contents in enumerate(cells):
            width = max(map(len, contents))
            if initialize:
                colwidths.append(width)
            else:
                colwidths[index] = max(width, colwidths[index])

            if initialize:
                table_lines.append([None])

            # Pad line count of each cell in the row to match the row_height
            cells[index] += [""] * (row_height - len(contents))

            # Add lines to line table and insert a break if needed
            table_lines[index].extend(cells[index] + [None] * separate)

    # Transpose the table and print each row. Rows containing `None` indicate a
    # row break should be inserted.
    for row in zip(*table_lines):
        printcols = list()

        if row[0] is None:
            print("+-", end="", file=dest)
            for index, column in enumerate(row):
                printcols.append("-" * colwidths[index])

            print(*printcols, sep="-+-", end="-+\n", file=dest)

        else:
            print("| ", end="", file=dest)
            for index, column in enumerate(row):
                if not PYTHON_3 and isinstance(column, unicode):
                    column = column.encode("utf-8", "replace")
                printcols.append(("%%-%ds" % colwidths[index]) % column)

            print(*printcols, sep=" | ", end=" |\n", file=dest)


def query_split(text):
    """
    Yield individual SQLite3 queries found in the given `text`. The last
    yielded query may be incomplete. Use `sqlite3.complete_statement` to verify
    whether or not it is a fragment.
    """
    segments = re.split("(;)", text)
    length = len(segments)
    j = 0
    for k in range(length + 1):
        query = ''.join(segments[j:k]).strip()
        if query and sqlite3.complete_statement(query):
            yield query
            j = k

    if j != length:
        tail = ''.join(segments[j:])
        if tail.strip():
            yield tail


def metaquery_conversion(original_query, original_params=tuple()):
    """
    Convert queries matching various, normally unsupported grammars to queries
    SQLite3 understands. The currently supported grammars are as follows:

    - {DESC | DESCRIBE} table_name
    - SHOW CREATE TABLE table_name
    - SHOW TABLES
    """
    flags = re.IGNORECASE | re.MULTILINE
    original_query = re.sub("[;\s]+$", "", original_query, flags)

    match = re.match("DESC(?:RIBE)?\s+(\S+)$", original_query, flags)
    if match:
        query = "PRAGMA table_info(" + match.group(1) + ")"
        return query, original_params

    match = re.match("SHOW\s+CREATE\s+TABLE\s+(\S+)$", original_query, flags)
    if match:
        table = match.group(1)
        if table[0] in "`\"":
            table = table[1:-1]
        query = (
            "SELECT sql || ';' AS `SHOW CREATE TABLE` "
            "FROM sqlite_master WHERE tbl_name = ? "
            "COLLATE NOCASE"
        )

        if table == "?":
            params = original_params
        else:
            params = (table, )

        return query, params

    match = re.match("SHOW\s+TABLES$", original_query, flags)
    if match:
        query = (
            "SELECT tbl_name AS `Tables` "
            "FROM sqlite_master "
            "WHERE type = 'table'"
        )
        return query, original_params

    return original_query, original_params


def sqlite3_repl(connection, input_function=None, dest=None):
    """
    Interactive REPL loop for SQLite3 designed to emulate the MySQL CLI
    REPL. Ctrl+C clears the current line buffer, and Ctrl+D exits the loop.
    When an incomplete query spans multiple lines, the prompt will change
    to provide a hint to the user about what token is missing to terminate
    the query. This function accepts a SQLite3 connection instance.
    """
    try:
        clock = time.monotonic
    except AttributeError:
        clock = time.time

    if not input_function:
        input_function = input if PYTHON_3 else raw_input

    linebuffer = ""
    original_connection_isolation_level = connection.isolation_level
    connection.isolation_level = None
    cursor = connection.cursor()
    while True:
        prompt = "sqlite> "
        if linebuffer.strip():
            for query in query_split(linebuffer):
                params = tuple()
                if sqlite3.complete_statement(query):
                    try:
                        query, params = metaquery_conversion(query, params)

                        start = clock()
                        results = cursor.execute(query, params)
                        duration = clock() - start

                        if cursor.rowcount > -1:
                            n = cursor.rowcount
                            s = "" if n == 1 else "s"
                            prefix = "Query OK, %d row%s affected" % (n, s)

                        elif cursor.description:
                            results = list(results)
                            n = len(results)
                            s = "" if n == 1 else "s"
                            prefix = "%d row%s in set" % (n, s)

                            headers = [d[0] for d in cursor.description]
                            tbl = [headers] + results
                            pretty_print_table(tbl, dest=dest)

                        else:
                            prefix = "Query OK, but no data returned"

                        text = "%s (%0.2f sec)" % (prefix, duration)

                    except sqlite3.Error as exc:
                        text = "%s" % exc

                    print(text, end="\n\n", file=dest)
                    linebuffer = ""

                elif query:
                    linebuffer = query
                    # Figure out what token is needed to complete the query and
                    # adjust the prompt accordingly.
                    terminators = (";", '"', "'", "`", '\\"', "\\'", "\\`")
                    for chars in terminators:
                        if sqlite3.complete_statement(query + chars + ";"):
                            prompt = "     " + chars[-1] + "> "
                            break
                    else:
                        prompt = "     -> "

        try:
            linebuffer += input_function(prompt) + "\n"
        except EOFError:
            # ^D to exit
            print("\n", end="", file=dest)
            connection.isolation_level = original_connection_isolation_level
            return
        except KeyboardInterrupt:
            # ^C to reset the line buffer
            linebuffer = ""
            print("\n", end="", file=dest)


def cli(argv, dest=None):
    """
    Command line interface for __file__

    Usage: __file__ [OPTIONS...] [QUERIES...]

    Any trailing, non-option arguments will be executed as SQLite3 queries
    after the data has been imported.

    Options:

     --help, -h             Show this documentation and exit.

     -A FILE, ..., -Z FILE  All capital, single-letter options are used to load
                            the specified file into the SQLite3 database. If no
                            "--table" option has been specified immediately
                            preceding the option, the letter name will be used
                            as the table name; loading a file with "-A" will
                            populate the table "A". Similarly, the table schema
                            will be auto-detected when no "--schema" option
                            immediately precedes this option.

     --table=TABLE          Name of table used to store the contents of the
                            next specified CSV file.

     --invalid=METHOD       Determines how rows of invalid data handled. The
                            METHOD can be "warn", "ignore", or "fail" which
                            will cause the script to emit a warning and skip
                            the record, silently skip the record or terminate
                            script execution respectively. When unspecified,
                            defaults to "warn."

     --loglevel=LEVEL       Set logging verbosity level. In order from the
                            highest verbosity to the lowest verbosity, can be
                            one of "DEBUG", "INFO", "WARNING", "ERROR",
                            "CRITICAL". The default value is "WARNING."

     --pretty               Pretty-print results of queries passed as command
                            line arguments instead of tab-separating the
                            results.

     --database=FILE        Path of the SQLite3 database the queries should be
                            executed on. When unspecified, the data is stored
                            volatile memory and becomes inaccessible after the
                            program stops running.

     -i                     Enter interactive mode after importing data. When
                            the "--database" flag is not specified, this is
                            implied.

     -v                     Increase logging verbosity. Can be used repeatedly
                            to further increase verbosity.

     -q                     Decrease logging verbosity. Can be used repeatedly
                            to further decrease verbosity.
    """
    if PYTHON_3:
        letters = string.ascii_uppercase
    else:
        letters = string.uppercase

    colopts = ":".join(letters) + ":hvqi"
    longopts = ["table=", "invalid=", "help", "pretty", "database="]
    options, arguments = getopt.gnu_getopt(argv[1:], colopts, longopts)

    if not argv[1:] or ("--help", "") in options or ("-h", "") in options:
        me = os.path.basename(argv[0] or __file__)
        docstring = cli.__doc__.replace("__file__", me)
        print(textwrap.dedent(docstring).strip())
        sys.exit(0 if argv[1:] else 1)

    loglevels = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
    loglevel = loglevels.index("WARNING")

    database = None
    prettify = False
    interact = False
    table = None
    loadfile_args = list()
    importer_kwargs = dict()

    for option, value in options:
        # Long options
        if option.startswith("--"):
            if option == "--invalid":
                if value not in ("ignore", "warn", "fail"):
                    raise getopt.GetoptError("Invalid value for --invalid")

                importer_kwargs["ignore_errors"] = value in ("ignore", "warn")
                importer_kwargs["log_warnings"] = value == "warn"

            elif option == "--table":
                table = value

            elif option == "--loglevel":
                try:
                    loglevel = loglevels.index(value.upper())
                except ValueError:
                    raise getopt.GetoptError("Invalid log level '%s'" % value)

            elif option == "--pretty":
                prettify = True

            elif option == "--database":
                database = value

        # Logging verbosity modifiers and Interactivity
        elif option in ("-v", "-q", "-i"):
            if option == "-v":
                loglevel -= loglevel > 0
            elif option == "-q":
                loglevel += loglevel < (len(loglevels) - 1)
            elif option == "-i":
                interact = True

        # All of the short options that accept arguments are just used for
        # table aliases
        else:
            loadfile_args.append((value, table or option[1]))
            table = None

    if not interact and database is None:
        interact = True

    loglevel = loglevels[loglevel]
    logging.getLogger().setLevel(getattr(logging, loglevel))
    logging.debug("Log level set to %s.", loglevel)

    connection = sqlite3.connect(database or ":memory:")
    importer = SQLite3CSVImporter(dbc=connection, **importer_kwargs)

    for args in loadfile_args:
        importer.loadfile(*args)

    cursor = connection.cursor()
    for query in arguments:
        if len(arguments) > 1:
            logging.info("Executing '%s'", query)
        else:
            logging.debug("Executing '%s'", query)

        results = cursor.execute(query)

        if prettify:
            results = list(results)
            if results:
                headers = [d[0] for d in cursor.description]
                pretty_print_table([headers] + results, dest=dest)
        else:
            def printable(var):
                """
                Return print function-friendly variable.
                """
                if not PYTHON_3 and isinstance(var, unicode):
                    return var.encode("utf-8", "replace")
                else:
                    return var

            for r in results:
                columns = ("" if c is None else printable(c) for c in r)
                print(*columns, sep="\t", file=dest)

    if interact:
        sqlite3_repl(connection, dest=dest)


def main():
    logging.basicConfig(format="%(message)s")

    try:
        cli(sys.argv)
    except getopt.GetoptError as exc:
        logging.fatal("Could not parse command line options: %s", exc)
        sys.exit(EXIT_GENERAL_FAILURE)
    except sqlite3.DatabaseError as exc:
        logging.fatal("Error updating database: %s", exc)
        sys.exit(EXIT_DATABASE_ERROR)
    except EnvironmentError as exc:
        logging.fatal("%s", exc)
        sys.exit(EXIT_GENERAL_FAILURE)


if __name__ == "__main__":
    main()
