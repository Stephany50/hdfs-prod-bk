INSERT INTO ${hivevar:spark_table_name} SELECT * FROM ${hivevar:hive_table_name}
WHERE ${hivevar:partition_column} BETWEEN DATE_ADD('###SLICE_VALUE###', ${hivevar:step_value}) AND '###SLICE_VALUE###'
  AND ${hivevar:partition_column} NOT IN (
    SELECT DISTINCT ${hivevar:partition_column} FROM ${hivevar:spark_table_name}
    WHERE ${hivevar:partition_column}
    BETWEEN DATE_ADD('###SLICE_VALUE###', ${hivevar:step_value}) AND '###SLICE_VALUE###'
  )