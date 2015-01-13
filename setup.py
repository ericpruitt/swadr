#!/usr/bin/env python
import distutils.core
import subprocess

# If pandoc is available, convert the markdown README to REstructured Text.
try:
    pandoc = subprocess.Popen(["pandoc", "-t", "rst", "README.md"],
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
    long_description, _ = pandoc.communicate()
except OSError:
    long_description = None

distutils.core.setup(
    name="swadr",
    version="1.0.1",
    description="Import data from CSV, TSV, etc. files into SQLite3 database.",
    author="Eric Pruitt",
    author_email="eric.pruitt@gmail.com",
    url="https://github.com/ericpruitt/swadr",
    license="BSD",
    keywords=["sqlite", "sqlite3", "sql", "csv", "tsv"],
    package_dir={'': 'src'},
    py_modules=["swadr"],
    scripts=["src/swadr"],
    long_description=long_description,
    classifiers=[
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: SQL",
        "Topic :: Database :: Front-Ends",
        "Topic :: Database",
    ],
)
