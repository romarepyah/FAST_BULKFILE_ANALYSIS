#!/usr/bin/env python3
"""Run all SQL migrations against the database."""

import os, sys, glob

# make sure app package is importable
sys.path.insert(0, os.path.dirname(__file__))

from app.db_connection import run_sql_file

migrations_dir = os.path.join(os.path.dirname(__file__), "app", "db")
files = sorted(glob.glob(os.path.join(migrations_dir, "*.sql")))

if not files:
    print("No migration files found.")
    sys.exit(1)

for f in files:
    name = os.path.basename(f)
    try:
        run_sql_file(f)
        print(f"  OK  {name}")
    except Exception as e:
        print(f" FAIL {name}: {e}")
        sys.exit(1)

print("\nAll migrations applied.")
