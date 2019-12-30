SELECT IF(NB_LIGNE >=150000000 AND COMPLETUDE=0 AND NB_FILES=6,'OK','NOK') FROM
(SELECT count(*) NB_LIGNE
 FROM CDR.IT_SMSC_MVAS_A2P where write_date='###SLICE_VALUE###' ) A,
(
SELECT count(distinct original_file_name) NB_FILES
 FROM CDR.IT_SMSC_MVAS_A2P
 where
    write_date='###SLICE_VALUE###'
    and SUBSTRING(original_file_name,19,5) = '00000') T1,
(SELECT COUNT(*) COMPLETUDE FROM (
    SELECT LAG(INDEX, 1) OVER (PARTITION BY MVAS_SOURCE ORDER BY INDEX) PREVIOUS,INDEX FROM (
        SELECT
            DISTINCT
            CAST(SUBSTRING(original_file_name,19,5) AS INT) INDEX,
            substr(original_file_name,25,13) MVAS_SOURCE
       from CDR.IT_SMSC_MVAS_A2P
       where WRITE_DATE = '###SLICE_VALUE###'
       and original_file_date='###SLICE_VALUE###'
    )A
)D WHERE INDEX-PREVIOUS >1)T2
