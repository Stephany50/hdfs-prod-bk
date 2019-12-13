SELECT IF(COMPLETUDE=0 AND NB_FILES>0  AND T3.SOURCES>=3,'OK','NOK')  FROM
(SELECT COUNT(*) NB_FILES FROM CDR.IT_CRA_MSC_HUAWEI WHERE CALLDATE = '###SLICE_VALUE###') T1,
(SELECT COUNT(*) COMPLETUDE FROM (
    SELECT LAG(INDEX, 1) OVER (PARTITION BY MSC_TYPE ORDER BY INDEX) PREVIOUS,INDEX FROM (
        SELECT
            DISTINCT
            CAST(SUBSTRING(SOURCE,11,9) AS INT) INDEX,
            SUBSTRING(SOURCE,5,11) MSC_TYPE
        FROM CDR.IT_CRA_MSC_HUAWEI
        WHERE CALLDATE = '###SLICE_VALUE###'
    )A
)D WHERE INDEX-PREVIOUS >1)T2,
(SELECT
    COUNT(DISTINCT SUBSTRING(SOURCE,5,7)) SOURCES
FROM CDR.IT_CRA_MSC_HUAWEI WHERE CALLDATE = '###SLICE_VALUE###')T3
