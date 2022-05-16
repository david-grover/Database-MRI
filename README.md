# Database-MRI

The Database MRI is a simple implementation of a controlled database scan. The SQL currently uses a stored procedure in Javascript for Snowflake. 
A similar system can be developed for any cloud data warehouse platform that uses some kind of table catalog or "information_schema."

All assets are created in a Snowflake schema called META. You should tailor your version appropriately.

The system has two parts:
1. Tables
2. Procs

Tables:
1. TABLE_SNAPSHOT = records of the basic snapshot SQL, either unfiltered (LIST_KEY = 'BASELINE') or filtered (LIST_KEY <> 'BASELINE').
2. TABLE_SCAN_LIST = a named list of values you want to scan Snowflake for.

Procs:
1. 
