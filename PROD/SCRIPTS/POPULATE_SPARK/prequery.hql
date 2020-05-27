SELECT IF(NB=${hivevar:step_value}, 'NOK', 'OK') FROM
 (SELECT COUNT(DISTINCT  ${hivevar:partition_column}) NB FROM ${hivevar:spark_table_name}
  WHERE ${hivevar:partition_column}
  BETWEEN DATE_ADD('###SLICE_VALUE###', ${hivevar:step_value}) AND '###SLICE_VALUE###')