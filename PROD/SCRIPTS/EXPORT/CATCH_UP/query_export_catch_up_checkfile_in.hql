SELECT 
    concat('IN_ZTE_CHECK_FILELIST_', replace('###SLICE_VALUE###', '-', ''), lpad(SEQUENCE, 2, 0), '.csv') MISSING_FILES
FROM 
(
    SELECT 
        GENERATE_SEQUENCE_FROM_INTERVALE(PREVIOUS+1,INDEX-1) SEQ 
    FROM 
    (
        SELECT 
            LAG(INDEX, 1) OVER (PARTITION BY CHECK_FILE ORDER BY INDEX) PREVIOUS
            , INDEX 
        FROM 
        (
            SELECT DISTINCT
                cast (substring(ORIGINAL_FILE_NAME,31, 2) as int) INDEX, 
                1 CHECK_FILE
            FROM CDR.SPARK_IT_ZTE_CHECK_FILE 
            WHERE file_date = '###SLICE_VALUE###' 
        )A
    ) D 
    WHERE INDEX-PREVIOUS >1
)R
LATERAL VIEW EXPLODE(SPLIT(SEQ, ',')) SEQUENCE AS SEQUENCE

union 

SELECT 
    concat('IN_ZTE_CHECK_FILELIST_', replace('###SLICE_VALUE###', '-', ''), lpad(SEQUENCE, 2, 0), '.csv') MISSING_FILES
FROM 
(
    SELECT 
        GENERATE_SEQUENCE_FROM_INTERVALE(-1+1,INDEX-1)  SEQ 
    FROM
    (
        SELECT min(INDEX) INDEX 
        FROM 
        (
            SELECT DISTINCT
            cast (substring(ORIGINAL_FILE_NAME,31, 2) as int) INDEX
            FROM CDR.SPARK_IT_ZTE_CHECK_FILE 
            WHERE file_date = '###SLICE_VALUE###' 
        )A
    ) D where INDEX - (-1) > 1 
) R
LATERAL VIEW EXPLODE(SPLIT(SEQ, ',')) SEQUENCE AS SEQUENCE

union

SELECT 
    concat('IN_ZTE_CHECK_FILELIST_', replace('###SLICE_VALUE###', '-', ''), lpad(SEQUENCE, 2, 0), '.csv') MISSING_FILES
FROM 
(
    SELECT 
        GENERATE_SEQUENCE_FROM_INTERVALE(INDEX+1, 24-1) SEQ 
    FROM 
    (
        SELECT max(INDEX) INDEX 
        FROM 
        (
            SELECT DISTINCT
            cast (substring(ORIGINAL_FILE_NAME,31, 2) as int) INDEX
            FROM CDR.SPARK_IT_ZTE_CHECK_FILE 
            WHERE file_date = '###SLICE_VALUE###' 
        )A
    ) D where 24 - INDEX > 1 
) R
LATERAL VIEW EXPLODE(SPLIT(SEQ, ',')) SEQUENCE AS SEQUENCE