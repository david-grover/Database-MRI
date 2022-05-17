

select 
    list_key
    ,table_name
    ,row_count as table_row_count
    ,column_name
    ,ordinal_position
    ,data_type
    ,is_identity
    ,value_count
    ,value_distinct_count
    ,ifnull(null_count,0) as null_count
    ,date_trunc('Day', table_snapshot_timestamp) as scan_date
from 
    meta.table_snapshot 
where 
    list_name = $list_name
order by
    table_name
    ,column_name
    ,list_key
    ;
