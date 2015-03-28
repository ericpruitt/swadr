#!/usr/bin/env python
# -*- coding: utf-8 -*-
import io
import os
import sqlite3
import sys
import tempfile
import unittest

import swadr

SCRIPT_DIRECTORY = os.path.dirname(os.path.abspath(__file__))


class SQLite3CSVImporterTests(unittest.TestCase):
    def test_ignore_errors_True(self):
        dbc = sqlite3.connect(":memory:")
        importer = swadr.SQLite3CSVImporter(dbc,
                                            ignore_errors=True,
                                            log_warnings=False)
        # This will cause the import to fail because the last line is short one
        # column.
        file_lines = ["A,B,C\n"] + ["1,2,3\n"] * 20 + ["7,8\n"]
        contents = "".join(file_lines).encode("ascii")

        try:
            tmpio = tempfile.NamedTemporaryFile(delete=False)
            filename = tmpio.name
            tmpio.write(contents)
            tmpio.close()
            importer.loadfile(filename, "A")
        finally:
            os.unlink(tmpio.name)

    def test_ignore_errors_False(self):
        dbc = sqlite3.connect(":memory:")
        importer = swadr.SQLite3CSVImporter(dbc,
                                            ignore_errors=False,
                                            log_warnings=False)
        # This will cause the import to fail because the last line is short one
        # column.
        file_lines = ["A,B,C\n"] + ["1,2,3\n"] * 20 + ["7,8\n"]
        contents = "".join(file_lines).encode("ascii")

        try:
            tmpio = tempfile.NamedTemporaryFile(delete=False)
            filename = tmpio.name
            tmpio.write(contents)
            tmpio.close()
            self.assertRaises(sqlite3.Error, importer.loadfile, filename, "A")
        finally:
            os.unlink(tmpio.name)

    def test_detect_types(self):
        if swadr.PYTHON_3:
            blob = bytes(range(256)).decode("utf-8", errors="surrogateescape")
        else:
            blob = "".join(map(chr, range(256)))

        table = [
            ["100", "Pony", "13.37", blob],
            ["200", "Duck", "0.37", blob],
            ["300", "Grendel", "42.1", blob],
        ]

        got = swadr.SQLite3CSVImporter.detect_types(table)
        expected = ["INTEGER", "TEXT", "REAL", "BLOB"]
        self.assertEqual(got, expected)

    def test_quote_identifier(self):
        dbc = sqlite3.connect(":memory:")
        with dbc:
            cursor = dbc.cursor()
            for char in range(1, 256):
                identifier = "T_" + chr(char)
                qid = swadr.SQLite3CSVImporter.quote_identifier(identifier)
                query = "CREATE TABLE IF NOT EXISTS " + qid + "(x INTEGER)"
                cursor.execute(query)

    def test_create_table_no_columns_parameter(self):
        dbc = sqlite3.connect(":memory:")
        importer = swadr.SQLite3CSVImporter(dbc)
        types = ["INTEGER", "TEXT", "REAL", "BLOB"]
        with dbc:
            cursor = dbc.cursor()
            importer.create_table("A", types=types)
            results = list(cursor.execute("PRAGMA table_info(A)"))
            # Verify types match up
            typecol = [r[2] for r in results]
            self.assertEqual(set(typecol), set(types))

    def test_create_table_duplication_prevention(self):
        dbc = sqlite3.connect(":memory:")
        importer = swadr.SQLite3CSVImporter(dbc)
        columns = ["Red", "Green", "Red", "Blue", "Green", "Green"]
        expected = ["Red", "Green", "Red_2", "Blue", "Green_2", "Green_3"]
        with dbc:
            cursor = dbc.cursor()
            # The types parameter doesn't matter for this test, just needs to
            # be the same length as the columns parameter.
            importer.create_table("A", columns=columns, types=columns)
            # Verify identifiers match up
            results = list(cursor.execute("PRAGMA table_info(A)"))
            got = [r[1] for r in results]
            self.assertEqual(got, expected)

    def test_create_table(self):
        dbc = sqlite3.connect(":memory:")
        importer = swadr.SQLite3CSVImporter(dbc)
        columns = ["Red", "Green", "Blue", "Black"]
        types = ["INTEGER", "TEXT", "REAL", "BLOB"]
        with dbc:
            cursor = dbc.cursor()
            importer.create_table("A", columns=columns, types=types)
            results = list(cursor.execute("PRAGMA table_info(A)"))
            # Verify types match up
            typecol = [r[2] for r in results]
            self.assertEqual(typecol, types)
            # Verify identifiers match up
            namecol = [r[1] for r in results]
            self.assertEqual(namecol, columns)

    def test_loadfile_invalid_unicode(self):
        dbc = sqlite3.connect(":memory:")
        importer = swadr.SQLite3CSVImporter(dbc)
        test_file = resource_path("samples", "invalid-unicode.csv")
        importer.loadfile(test_file, "A")

        original_text_factory = dbc.text_factory
        dbc.text_factory = bytes

        cursor = dbc.cursor()
        with open(test_file, "rb") as iostream:
            rows = cursor.execute("SELECT * FROM A")
            for line, row in zip(iostream, rows):
                stripped_line = line.strip()
                flat_row = ",".encode("ascii").join(row)
                self.assertEqual(flat_row, stripped_line)

    def test_loadfile_comma_separated_values(self):
        expected = [
            (unicode("Jan"), 2014, unicode("A1"), 18),
            (unicode("Lucy"), 2016, unicode("B5"), 16),
            (unicode("Richard"), 2010, unicode("--"), 22),
        ]

        dbc = sqlite3.connect(":memory:")
        importer = swadr.SQLite3CSVImporter(dbc)
        test_file = resource_path("samples", "students.csv")
        importer.loadfile(test_file, "A")

        cursor = dbc.cursor()
        got = list(cursor.execute("SELECT * FROM A"))
        self.assertEqual(got, expected)

    def test_loadfile_tab_separated_values(self):
        expected = [
            (1, 90, unicode("Richard")),
            (2, 100, unicode("Richard")),
            (3, 70, unicode("Richard")),
            (1, 85, unicode("Lucy")),
            (2, 99, unicode("Lucy")),
            (3, 80, unicode("Lucy")),
            (1, 55, unicode("Jan")),
            (2, 70, unicode("Jan")),
            (3, 40, unicode("Jan")),
        ]

        dbc = sqlite3.connect(":memory:")
        importer = swadr.SQLite3CSVImporter(dbc)
        test_file = resource_path("samples", "grades.tsv")
        importer.loadfile(test_file, "A")

        cursor = dbc.cursor()
        got = list(cursor.execute("SELECT * FROM A"))
        self.assertEqual(got, expected)

    def test_loadfile_header_detection(self):
        dbc = sqlite3.connect(":memory:")
        importer = swadr.SQLite3CSVImporter(dbc)
        cursor = dbc.cursor()

        file_without_headers = resource_path("samples", "grades-no-header.tsv")
        importer.loadfile(file_without_headers, "A")
        expected_column_names_without_headers = [
            unicode("a1"), unicode("a2"), unicode("a3")]
        results = list(cursor.execute("PRAGMA table_info(A)"))
        names_without_headers = [r[1] for r in results]
        self.assertEqual(names_without_headers,
                         expected_column_names_without_headers)

        file_with_headers = resource_path("samples", "grades.tsv")
        importer.loadfile(file_with_headers, "B")
        expected_column_names_with_headers = [
            unicode("Assignment"), unicode("Grade"), unicode("Student")]
        results = list(cursor.execute("PRAGMA table_info(B)"))
        names_with_headers = [r[1] for r in results]
        self.assertEqual(names_with_headers,
                         expected_column_names_with_headers)


class SWADRModuleFunctionTests(unittest.TestCase):
    def test_query_split(self):
        script = "SELECT 1; SELECT ';'    ; SELECT 100"
        expected = [
            "SELECT 1;",
            "SELECT ';'    ;",
            " SELECT 100",
        ]

        got = list(swadr.query_split(script))
        self.assertEqual(got, expected)

    def test_metaquery_conversion(self):
        # Each entry is (query, number_of_rows_query_should_return).
        tests = [
            # Setup
            ("CREATE TABLE A(x INTEGER, y INTEGER, z INTEGER)", 0),
            ("CREATE TABLE B(p INTEGER, q INTEGER, r INTEGER)", 0),
            # Test of recognized grammars
            ("DESC A", 3),
            ("DESCRIBE B", 3),
            ("SHOW CREATE TABLE A", 1),
            ("SHOW CREATE TABLE `B`", 1),
            ("SHOW CREATE TABLE \"A\"", 1),
            ("SHOW TABLES", 2),
            # Verify that other queries are not affected.
            ("SELECT * FROM A", 0),
            ("SELECT * FROM B", 0),
            ("INSERT INTO A VALUES (1, 2, 3)", 0),
            ("INSERT INTO B VALUES (1, 2, 3), (7, 8, 9)", 0),
            ("SELECT * FROM A", 1),
            ("SELECT * FROM B", 2),
        ]

        dbc = sqlite3.connect(":memory:")
        with dbc:
            cursor = dbc.cursor()
            for query, expected_rowcount in tests:
                query, params = swadr.metaquery_conversion(query)
                got_rowcount = len(list(cursor.execute(query, params)))
                self.assertEqual(got_rowcount, expected_rowcount)

    def test_pretty_print_table_breakafter_False(self):
        table = [
            [unicode("A"), unicode("B"), unicode("C")],
            [unicode("1"), unicode("2"), unicode("3")],
            [unicode("3"), unicode("9"), unicode("27")],
        ]
        expected = unicode("\n").join((
            "+---+---+----+",
            "| A | B | C  |",
            "| 1 | 2 | 3  |",
            "| 3 | 9 | 27 |",
            "+---+---+----+\n",
        ))

        if swadr.PYTHON_3:
            txtio = io.StringIO()
        else:
            txtio = io.BytesIO()

        swadr.pretty_print_table(table, breakafter=False, dest=txtio)
        txtio.seek(0)
        self.assertEqual(txtio.read(), expected)

    def test_pretty_print_table_breakafter_True(self):
        table = [
            [unicode("A"), unicode("B"), unicode("C")],
            [unicode("1"), unicode("2"), unicode("3")],
            [unicode("3"), unicode("9"), unicode("27")],
        ]
        expected = unicode("\n").join((
            "+---+---+----+",
            "| A | B | C  |",
            "+---+---+----+",
            "| 1 | 2 | 3  |",
            "+---+---+----+",
            "| 3 | 9 | 27 |",
            "+---+---+----+\n",
        ))

        if swadr.PYTHON_3:
            txtio = io.StringIO()
        else:
            txtio = io.BytesIO()

        swadr.pretty_print_table(table, breakafter=True, dest=txtio)
        txtio.seek(0)
        self.assertEqual(txtio.read(), expected)

    def test_pretty_print_table_breakafter_n(self):
        table = [
            [unicode("A"), unicode("B"), unicode("C")],
            [unicode("1"), unicode("2"), unicode("3")],
            [unicode("3"), unicode("9"), unicode("27")],
        ]
        expected = unicode("\n").join((
            "+---+---+----+",
            "| A | B | C  |",
            "| 1 | 2 | 3  |",
            "+---+---+----+",
            "| 3 | 9 | 27 |",
            "+---+---+----+\n",
        ))

        if swadr.PYTHON_3:
            txtio = io.StringIO()
        else:
            txtio = io.BytesIO()

        swadr.pretty_print_table(table, [1], dest=txtio)
        txtio.seek(0)
        self.assertEqual(txtio.read(), expected)

    def test_pretty_print_table_embedded_newlines_and_tabs(self):
        table = [
            [unicode("A"), unicode("B"), unicode("C")],
            [unicode("1"), unicode("2\n\n\tT"), unicode("3\n  X")],
            [unicode("3"), unicode("9"), unicode("27")],
        ]
        expected = unicode("\n").join((
            "+---+-----------+-----+",
            "| A | B         | C   |",
            "+---+-----------+-----+",
            "| 1 | 2         | 3   |",
            "|   |           |   X |",
            "|   |         T |     |",
            "+---+-----------+-----+",
            "| 3 | 9         | 27  |",
            "+---+-----------+-----+\n",
        ))

        if swadr.PYTHON_3:
            txtio = io.StringIO()
        else:
            txtio = io.BytesIO()

        swadr.pretty_print_table(table, breakafter=True, tabsize=8, dest=txtio)
        txtio.seek(0)
        self.assertEqual(txtio.read(), expected)

    def test_pretty_print_table_with_unicode(self):
        if swadr.PYTHON_3:
            txtio = io.StringIO()
            table = ["☺"]
        else:
            txtio = io.BytesIO()
            table = [unicode("☺", encoding="utf-8")]

        expected = "\n".join((
            "+---+",
            "| ☺ |",
            "+---+\n",
        ))

        swadr.pretty_print_table(table, dest=txtio)
        txtio.seek(0)
        self.assertEqual(txtio.read(), expected)

    def test_batch_output_handles_unicode(self):
        argv = ["_", "--database=:memory:", 'SELECT "☺"']

        if swadr.PYTHON_3:
            txtio = io.StringIO()
        else:
            txtio = io.BytesIO()

        expected = "☺\n"
        swadr.cli(argv, dest=txtio)
        txtio.seek(0)
        self.assertEqual(txtio.read(), expected)

    if swadr.WCWIDTH_SUPPORT:
        def test_pretty_printing_tables_with_wide_asian_characters(self):
            table = [
                ["Artist", "Song"],
                ["分島花音", "砂のお城"],
            ]

            if swadr.PYTHON_3:
                txtio = io.StringIO()
            else:
                txtio = io.BytesIO()
                for j, row in enumerate(table):
                    for k, column in enumerate(row):
                        table[j][k] = unicode(column, encoding="utf-8")

            expected = "\n".join((
                "+----------+----------+",
                "| Artist   | Song     |",
                "+----------+----------+",
                "| 分島花音 | 砂のお城 |",
                "+----------+----------+\n",
            ))

            swadr.pretty_print_table(table, breakafter=[0], dest=txtio)
            txtio.seek(0)
            self.assertEqual(txtio.read(), expected)

    def test_pretty_print_table_type_acceptance(self):
        table = [
            ["A", "B", "C"],
            [1.23, 2, b"XYZ"],
        ]

        if swadr.PYTHON_3:
            txtio = io.StringIO()
        else:
            txtio = io.BytesIO()

        # This simply needs to not fail, so the contents of txtio is not
        # checked.
        swadr.pretty_print_table(table, dest=txtio)

    def test_numbers_cause_right_alignment(self):
        table = [
            ["One", "Two", "Three"],
            ["Uno", 2, "Tres"],
            ["Uma", 9999, "Troi"],
        ]

        if swadr.PYTHON_3:
            txtio = io.StringIO()
        else:
            txtio = io.BytesIO()

        expected = "\n".join((
            "+-----+------+-------+",
            "| One |  Two | Three |",
            "+-----+------+-------+",
            "| Uno |    2 | Tres  |",
            "| Uma | 9999 | Troi  |",
            "+-----+------+-------+\n",
        ))

        swadr.pretty_print_table(table, breakafter=[0], dest=txtio)
        txtio.seek(0)
        self.assertEqual(txtio.read(), expected)


def resource_path(*args):
    return os.path.join(SCRIPT_DIRECTORY, *args)


if swadr.PYTHON_3:
    unicode = str

if __name__ == "__main__":
    unittest.main()
