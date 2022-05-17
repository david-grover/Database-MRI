# Database-MRI

DESCRIPTION:
The Database MRI is a simple implementation of a controlled database scan using dynamic SQL. The SQL is generated in a stored procedure using Javascript for Snowflake and the results are stored in a database table for analysis.  The proc can be set to query columns for specific values, stored in another table, to generate statistics for specific filters as well.
 
A similar system can be developed for any cloud data warehouse platform that uses some kind of table catalog or "information_schema."

WHY: 
Some of this functionality is available in a data catalog product, which can be used to scan databases and file systems for patterns and in some cases specific 
values. The current (20220517) primary use for data catalogs is to run regex scans against values to identify likely PII fields. This is a profoundly useful 
workflow, but only a small subset of the actual problems we need a data catalog to solve.
    
We need to be able to scan large numbers of tables across multiple databases for specific sets of values. Those sets of values might be specific key values such 
as known SSNs or CC#s, or known product codes, or campaign codes, or whatever. We may use this scan forensically, to identify possible leaks of PII into application 
databases or analytical cubes. Or we may use this scan to aid discovery, as when we're tasked with finding event codes in an implicit event
model spread across 1000 tables.
    
We may also use the scan to profile the contents of tables using the LIST_KEY = BASELINE setting.  The statistical profile we take with that setting can be used to identify columns that are likely key fields, or likely lookups or category fields, or mostly NULL, or whatever else we can infer from a quantitative profile. 
    
FIELDS COLLECTED:
    TABLE = table_catalog, table_schema, table_name (not the parameter, the DB name), row_count, bytes
    COLUMN = column_name, ordinal_position, data_type, is_identity
    STATS = value_count (how many rows have values), value_distinct_count (how many distinct values), null_count (how many rows have NULL values)
    METADATA = table_snapshot_timestamp, scan_key (a UUID generated for each CALL), list_name, list_key
    
ASSUMPTIONS:
    1. Two tables: 
        META.TABLE_SNAPSHOT
        META.TABLE_SCAN_LIST
    2. Snowflake schema READ and USE access
    
PARAMETERS:
        SCHEMA_NAME VARCHAR = A Snowflake Schema the user has READ and USE access to.
        TABLE_NAME VARCHAR = The specific table name OR wildcard to be scanned.
        SLICE_TYPE VARCHAR = A switch {NAME, SLICE} to indicate whether TABLE_NAME should be interpreted 
        LIST_NAME VARCHAR = The reason for the scan.
        LIST_KEY VARCHAR = EITHER the value BASELINE or a specific list set identified by META.TABLE_SCAN_LIST.LIST_KEY.

PLAN:
    1. Create an array of columns from INFORMATION_SCHEMA.
    2. Use the values in that array to create some SQL.
        1. If LIST_KEY = 'BASELINE' then run an unfiltered snapshot on the table.'
        2. IF LIST_KEY <> 'BASELINE' then use the value in LIST_KEY to filter META.TABLE_SCAN_LIST.LIST_KEY and join META.TABLE_SCAN_LIST.LIST_VALUE 
        against the tables defined in (1).
    
I used Javascript to do this b/c I couldn't get more than one column to show up in a WHILE loop in Snowscript. Otherwise this is pretty basic.   
    
    
