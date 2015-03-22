swadr
=====

S.W.A.D.R., SQLite3 With Arbitrarily Delimited Records, is designed to be a
replacement and significant improvement over [SQLet](http://www.sqlet.com/), "a
free, open-source script that allows you to directly execute SQL on multiple
text files, right from the Linux command line." In addition to augmenting the
features of SQLet, I also elected to use the
[BSD 2-Clause License](http://opensource.org/licenses/BSD-2-Clause) instead of
the GPL (SWADR is derived neither in whole nor part from SQLet).

Some notable improvements over SQLet are:

- When importing data with swadr, swadr will automatically detect the files'
  delimation type as well as the schema of the data.
- Queries do not need to be piped to SQLite3.
- Swadr provides a built-in SQLite3 REPL designed to emulate the MySQL CLI.
- Unparseable records will not terminate the execution of the program by
  default.

Quick Example
-------------

Load examples/students.csv into the table "A" and load grades.csv into the
table "B" then enter interactive mode:

    swadr -A src/samples/students.csv -B src/samples/grades.tsv
    sqlite> DESC A;
    +-----+-----------+---------+---------+------------+----+
    | cid | name      | type    | notnull | dflt_value | pk |
    +-----+-----------+---------+---------+------------+----+
    | 0   | Name      | TEXT    | 0       | NULL       | 0  |
    | 1   | Class     | INTEGER | 0       | NULL       | 0  |
    | 2   | Home_Room | TEXT    | 0       | NULL       | 0  |
    | 3   | Age       | INTEGER | 0       | NULL       | 0  |
    +-----+-----------+---------+---------+------------+----+
    4 rows in set (0.00 sec)

    sqlite> DESC B;
    +-----+------------+---------+---------+------------+----+
    | cid | name       | type    | notnull | dflt_value | pk |
    +-----+------------+---------+---------+------------+----+
    | 0   | Assignment | INTEGER | 0       | NULL       | 0  |
    | 1   | Grade      | INTEGER | 0       | NULL       | 0  |
    | 2   | Student    | TEXT    | 0       | NULL       | 0  |
    +-----+------------+---------+---------+------------+----+
    3 rows in set (0.00 sec)

    sqlite> SELECT name, AVG(grade) FROM A INNER JOIN B ON name = student
         ;> GROUP BY student;
    +---------+---------------+
    | name    | AVG(grade)    |
    +---------+---------------+
    | Jan     | 55.0          |
    | Lucy    | 88.0          |
    | Richard | 86.6666666667 |
    +---------+---------------+
    3 rows in set (0.00 sec)

Installation
------------

There are no non-standard modules required to install S.W.A.D.R., but if the
[wcwidth](https://pypi.python.org/pypi/wcwidth/0.1.4) module is available, it
will be used to correctly pad tables containing east Asian characters:

    sqlite> SELECT "最初の例: wcwidth missing" AS "Example 1";
    +-----------------------+
    | Example 1             |
    +-----------------------+
    | 最初の例: wcwidth missing |
    +-----------------------+

    sqlite> SELECT "第二の例: wcwidth installed" AS "Example 2";
    +-----------------------------+
    | Example 2                   |
    +-----------------------------+
    | 第二の例: wcwidth installed |
    +-----------------------------+
    1 row in set (0.00 sec)

### Option 1: setup.py / pip ###

A setup.py file is provided that will install the "swadr" Python module and a
script for launching swadr's CLI. Execute `python setup.py install` using sudo
or as privileged user to install the package globally or run `python setup.py
install --user` to install the package as the current user.

Alternatively, swadr can be installed using pip, e.g.: `pip install swadr` or
`pip install --user swadr`.

After installation with either pip or setup.py, the "swadr" module will be
importable and, provided your `PATH` environment variable is configured
correctly, running `swadr` at the command line will launch the command line
interface. The default location for scripts packaged with Python modules is
generally `~/.local/bin`, but this can be changed using the
[--install-scripts](http://docs.python.org/2/install/#custom-installation)
option. The swadr CLI can also be launched by running `python -m swadr` once
the module has been installed.

### Option 2: Copying ###

The swadr CLI is wholly contained in the file `./src/swadr.py` and can run
independently of the rest of the files in this repository, so swadr can be
installed by simply copying `./src/swadr.py` to a folder listed in the `PATH`
environment variable; `$HOME/bin`, `/usr/local/bin`, and `/usr/bin` are popular
defaults -- `cp ./src/swadr.py ~/bin/swadr && hash -r` then run `swadr`.

Command Line Options
--------------------

**NOTE:** Any trailing, non-option arguments will be executed as SQLite3
queries after the data has been imported.

### --help, -h ###

Show the CLI documentation and exit.

### -A FILE, ..., -Z FILE ###

All capital, single-letter options are used to load the specified file into the
SQLite3 database. If no "--table" option has been specified immediately
preceding the option, the letter name will be used as the table name; loading a
file with "-A" will populate the table "A".

### --table=TABLE ###

Name of table used to store the contents of the next specified CSV file.

### --invalid=METHOD ###

Determines how rows of invalid data handled. The METHOD can be "warn",
"ignore", or "fail" which will cause the script to emit a warning and skip the
record, silently skip the record or terminate script execution respectively.
When unspecified, defaults to "warn."

### --loglevel=LEVEL ###

Set logging verbosity level. In order from the highest verbosity to the lowest
verbosity, can be one of "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL". The
default value is "WARNING."

### --pretty ###

Pretty-print results of queries passed as command line arguments instead of
tab-separating the results.

### --database=FILE ###

Path of the SQLite3 database the queries should be executed on. When
unspecified, the data is stored volatile memory and becomes inaccessible after
the program stops running.

### -i ###

Enter interactive mode after importing data. When the "--database" flag is not
specified, this option is implied. In addition to being able to execute normal
SQLite3 queries, the interpreter also has emulated support for some of MySQL's
special statements matching the following grammars:

- {DESC | DESCRIBE} **table_name**
- SHOW CREATE TABLE **table_name**
- SHOW TABLES

### -v ###

Increase logging verbosity. Can be used repeatedly to further increase
verbosity.

### -q ###

Decrease logging verbosity. Can be used repeatedly to further decrease
verbosity.
