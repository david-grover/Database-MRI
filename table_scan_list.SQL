CREATE TABLE IF NOT EXISTS meta.table_scan_list(
						table_scan_list_id int identity
						,list_name	varchar
						,list_key	varchar							
						,list_value	varchar
						,status_flag    varchar default 'ACTIVE'
						,list_value_created_timestamp   timestamp_ltz	DEFAULT CURRENT_TIMESTAMP
						,list_value_updated_timestamp   timestamp_ltz	DEFAULT CURRENT_TIMESTAMP
						);
						

