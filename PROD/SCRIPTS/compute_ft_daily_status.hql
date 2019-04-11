
INSERT INTO FT_DAILY_STATUS
SELECT '###SLICE_VALUE###' TABLE_DATE,'${hivevar:table_type}' TABLE_TYPE, '${hivevar:table_name}' TABLE_NAME, NB_ROWS,TABLE_INSERT_DATE,CURRENT_TIMESTAMP INSERT_DATE
FROM (
    SELECT COUNT(*) NB_ROWS,MAX(${hivevar:insert_date_column}) TABLE_INSERT_DATE FROM  ${hivevar:database_table_name} WHERE ${hivevar:table_partition}='###SLICE_VALUE###'
)T
;