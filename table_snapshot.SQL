  CREATE TABLE IF NOT EXISTS meta.table_snapshot( 
                                                  snaphot_id int identity
                                                  ,scan_key   varchar
                                                  ,list_name varchar 
                                                  ,list_key varchar  
                                                  ,table_catalog text
                                                  ,table_schema text
                                                  ,table_name text
                                                  ,row_count number
                                                  ,bytes number
                                                  ,column_name text
                                                  ,ordinal_position number
                                                  ,data_type text
                                                  ,is_identity text
                                                  ,value_count integer
                                                  ,value_distinct_count integer
                                                  ,null_count integer
                                                  ,table_snapshot_timestamp timestamp_ltz default current_timestamp
                                                  );