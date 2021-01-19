INSERT INTO AGG.SPARK_FT_A_IRIS_REVENU_SPLIT
SELECT
    sum((A.charge/100)) CA,
    sum((A.charge/100)*D.IRIS_VOIX_COEF/100) CA_IRIS_VOIX,
    sum((A.charge/100)*D.IRIS_DATA_COEF/100) CA_IRIS_DATA,
    D.IRIS_VOIX_COEF IRIS_VOIX_COEF,
    D.IRIS_DATA_COEF IRIS_DATA_COEF,
    CURRENT_TIMESTAMP INSERT_DATE,
    substring('###SLICE_VALUE###',1,7) event_month,
    '###SLICE_VALUE###' AS event_date
FROM cdr.spark_it_zte_adjustment A
INNER JOIN DIM.SPARK_SPLIT_REVENU_IRIS D ON D.EVENT_MONTH = substring(file_date,1,7)
WHERE file_date='###SLICE_VALUE###' and  channel_id ='21' and acct_res_code = '1'
GROUP BY file_date,substring(file_date,1,7),D.IRIS_VOIX_COEF,D.IRIS_DATA_COEF