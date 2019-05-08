SELECT IF(COMPLETUDE=0 AND NB_FILES=6,'OK','NOK') FROM
(SELECT COUNT(*) NB_FILES
 FROM default.received_files
 where original_file_month between DATE_FORMAT(DATE_SUB('###SLICE_VALUE###',${hivevar:date_offset}), 'yyyy-MM') and DATE_FORMAT('###SLICE_VALUE###', 'yyyy-MM')
    and file_type='SMSC_MVAS'
    and original_file_date='###SLICE_VALUE###'
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
