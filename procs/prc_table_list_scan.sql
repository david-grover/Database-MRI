
CREATE OR REPLACE PROCEDURE meta.prc_table_list_scan(SCHEMA_NAME VARCHAR, TABLE_NAME VARCHAR, SLICE_TYPE VARCHAR, LIST_NAME VARCHAR, LIST_KEY VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
//Example SLICE_TYPE = SLICE: CALL meta.prc_table_list_scan('INGRESS', '_SHR_', 'SLICE', 'INT_ID_TEST2', 'INT_ID_TEST2');
//Example SLICE_TYPE = NAME: CALL meta.prc_table_list_scan('INGRESS', 'ATOMS_RPT_ACPT_ETLSTAGE_TEMPTELEPHONESHRALL', 'NAME', 'INT_ID_TEST3', 'INT_ID_TEST3');

/*
    David Grover
    20220517

    This proc runs predefined statistical aggregation SQL against a set of columns in a Snowflake schema to help determine the kinds of data stored in those columns.
    
    Some of this functionality is available in a data catalog product, which can be used to scan databases and file systems for patterns and in some cases specific 
    values. The current (20220517) primary implementations of data catalogs is to use regex queries against values to identify PII. This is a profoundly useful 
    workflow, but only a small subset of the actual problems we need a data catalog to solve.
    
    We need to be able to scan large numbers of tables across multiple databases for specific sets of values. Those sets of values might be specific key values such 
    as known SSNs or CC#s, or known product codes, or campaign codes, or whatever. We may use this scan forensically, to identify possible leaks of PII in application 
    databases or analytical cubes. Or we may use this scan to aid discovery, as when we're tasked with finding event codes in an implicit event
    model spread across 1000 tables.
    
    We may also use the scan to profile the contents of tables using the LIST_KEY = BASELINE setting.  The statistical profile we take here can be used to identify 
    columns that are likely key fields, or likely lookups or category fields, or mostly NULL, or whatever else we can infer from a quantitative profile. 
    
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
    
    

*/

    var schema_name = SCHEMA_NAME;
    var table_name = TABLE_NAME;
    var slice_type = SLICE_TYPE;
    var list_name = LIST_NAME;
    var list_key = LIST_KEY;

    //Create the snapshot catalog.
    
        var snapshot_catalog_sql = "SELECT t.table_catalog, t.table_schema, t.table_name, t.row_count, t.bytes, c.column_name, c.ordinal_position, c.data_type, c.is_identity FROM information_schema.tables t INNER JOIN information_schema.columns c ON t.table_name = c.table_name WHERE (('" + slice_type + "' = 'NAME' AND t.table_schema = '" + schema_name + "' AND t.table_name = '" + table_name + "') OR ('" + slice_type + "' = 'SLICE' AND t.table_schema =  '" + schema_name + "' AND t.table_name  LIKE '%" + table_name + "%')) ";
        
        //List of column_name chars to exclude from the catalog:
        snapshot_catalog_sql = snapshot_catalog_sql + " AND c.column_name NOT LIKE '% %' AND t.table_name NOT LIKE '%_SSIS_%'";
   
    //Load the catalog
 
        var snapshot_catalog_exec = snowflake.createStatement( {sqlText: snapshot_catalog_sql} );
        var snapshot_catalog = snapshot_catalog_exec.execute();
    
    //Create a scan_key
    
        var scan_key_exec = snowflake.createStatement( {sqlText: "SELECT CAST(UUID_STRING() as VARCHAR) as scan_key"} );
        var scan_key_result = scan_key_exec.execute();
        scan_key_result.next();
        var scan_key = scan_key_result.getColumnValue(1);
    
    //Assign the INSERT SQL variable

        var snapshot_insert_sql = "INSERT INTO meta.table_snapshot( table_catalog ,table_schema ,table_name ,row_count ,bytes ,column_name ,ordinal_position ,data_type ,is_identity , value_count, value_distinct_count, null_count, table_snapshot_timestamp, scan_key, list_name, list_key) "
    
    // Loop through the results, processing one row at a time
    var snapshot_counter = 1;

    while (snapshot_catalog.next())  {
                
        //SELECT clause: Add primary fields.
        var snapshot_select_sql = "SELECT '" + snapshot_catalog.getColumnValue(1) + "','" + snapshot_catalog.getColumnValue(2) + "','" + snapshot_catalog.getColumnValue(3) + "','" + snapshot_catalog.getColumnValue(4) + "', '" + snapshot_catalog.getColumnValue(5) + "','" + snapshot_catalog.getColumnValue(6) + "','" + snapshot_catalog.getColumnValue(7) + "','" + snapshot_catalog.getColumnValue(8) + "','" + snapshot_catalog.getColumnValue(9) + "', COUNT([" + snapshot_catalog.getColumnValue(6) + "]), COUNT(DISTINCT " + snapshot_catalog.getColumnValue(6) + "), SUM(CASE WHEN " + snapshot_catalog.getColumnValue(6) + " IS NULL THEN 1 ELSE 0 END) " ;
        
        //SELECT clause: Add metadata fields
        var snapshot_select_sql = snapshot_select_sql + ", CURRENT_TIMESTAMP, '" + scan_key + "', '" + list_name + "','" + list_key + "'";
        
        //FROM clause
        var snapshot_from_sql = "FROM " + snapshot_catalog.getColumnValue(1) + "." + snapshot_catalog.getColumnValue(2) + "." + snapshot_catalog.getColumnValue(3) + " src "
        
        //WHERE clause
        var snapshot_where_sql = " ";
        
        //LIST_KEY = BASELINE vs. ~BASELINE
        if (list_key === 'BASELINE') {
            //Do nothing to the SQL
            snapshot_from_sql = snapshot_from_sql + " ";
            snapshot_where_sql = " ";
            } else {       
            //Add the filter
            snapshot_from_sql = snapshot_from_sql + " INNER JOIN meta.table_scan_list sl ON sl.list_value = src." + snapshot_catalog.getColumnValue(6) + " "; 
            snapshot_where_sql = snapshot_where_sql + "WHERE sl.list_key = '" + list_key + "'";
            };
            
                      
        //INSERT assembly
        var snapshot_sql = snapshot_insert_sql + snapshot_select_sql + snapshot_from_sql + snapshot_where_sql;
        
        //return snapshot_sql;

        var snapshot_exec = snowflake.createStatement( {sqlText: snapshot_sql } );
        var snapshot = snapshot_exec.execute();
                      
        snapshot_counter = snapshot_counter + 1
    };    
    
    return snapshot_counter + " columns processed.";

$$
;
