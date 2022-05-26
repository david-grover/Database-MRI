CREATE OR REPLACE PROCEDURE dave_dev.meta.prc_table_list_scan(schema_name VARCHAR, table_name VARCHAR, slice_type VARCHAR, list_name VARCHAR, list_key VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$

//Example SLICE_TYPE set to SLICE: CALL meta.prc_table_list_scan('INGRESS', 'SHR_', 'SLICE', 'INT_ID_TEST2', 'INT_ID_TEST2');
//Example SLICE_TYPE set to NAME: CALL meta.prc_table_list_scan('INGRESS', 'ATOMS_RPT_ACPT_ETLSTAGE_TEMPTELEPHONESHRALL', 'NAME', 'INT_ID_TEST3', 'INT_ID_TEST3');

    var schema_name = SCHEMA_NAME;
    var table_name = TABLE_NAME;
    var slice_type = SLICE_TYPE;
    var list_name = LIST_NAME;
    var list_key = LIST_KEY;

    //Create the snapshot catalog.
    
        var snapshot_catalog_sql = "SELECT t.table_catalog, t.table_schema, t.table_name, t.row_count, t.bytes, c.column_name, c.ordinal_position, c.data_type, c.is_identity FROM information_schema.tables t INNER JOIN information_schema.columns c ON t.table_name = c.table_name  and t.table_catalog = c.table_catalog and t.table_schema = c.table_schema WHERE (('" + slice_type + "' = 'NAME' AND t.table_schema = '" + schema_name + "' AND t.table_name = '" + table_name + "') OR ('" + slice_type + "' = 'SLICE' AND t.table_schema =  '" + schema_name + "' AND t.table_name  LIKE '" + table_name + "%')) ";
        
        //List of column_name chars to exclude from the catalog:
        snapshot_catalog_sql = snapshot_catalog_sql + " AND c.column_name NOT LIKE '% %' AND t.table_name NOT LIKE '%_SSIS_%' AND t.table_type = 'BASE TABLE'";
   
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
                
        //SELECT Add primary fields.
        var snapshot_select_sql = "SELECT '" + snapshot_catalog.getColumnValue(1) + "','" + snapshot_catalog.getColumnValue(2) + "','" + snapshot_catalog.getColumnValue(3) + "','" + snapshot_catalog.getColumnValue(4) + "', '" + snapshot_catalog.getColumnValue(5) + "','" + snapshot_catalog.getColumnValue(6) + "','" + snapshot_catalog.getColumnValue(7) + "','" + snapshot_catalog.getColumnValue(8) + "','" + snapshot_catalog.getColumnValue(9) + "', COUNT([" + snapshot_catalog.getColumnValue(6) + "]), COUNT(DISTINCT " + snapshot_catalog.getColumnValue(6) + "), ifnull(SUM(CASE WHEN " + snapshot_catalog.getColumnValue(6) + " IS NULL THEN 1 ELSE 0 END),0) " ;
        
        //SELECT clause: Add metadata fields
        var snapshot_select_sql = snapshot_select_sql + ", CURRENT_TIMESTAMP, '" + scan_key + "', '" + list_name + "','" + list_key + "'";
        
        //FROM clause
        var snapshot_from_sql = " FROM " + snapshot_catalog.getColumnValue(1) + "." + snapshot_catalog.getColumnValue(2) + "." + snapshot_catalog.getColumnValue(3) + " src "
        
        //WHERE clause
        var snapshot_where_sql = " ";
        
        if (list_key === 'BASELINE') {
            snapshot_from_sql = snapshot_from_sql + " ";
            snapshot_where_sql = " ";
            } else {       
            snapshot_from_sql = snapshot_from_sql + " INNER JOIN meta.table_scan_list sl ON UPPER(CAST(sl.list_value as VARCHAR)) = IFNULL(UPPER(CAST(src." + snapshot_catalog.getColumnValue(6) + " as VARCHAR)),'-1') "; 
            snapshot_where_sql = snapshot_where_sql + " WHERE sl.list_key = '" + list_key + "'";
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
