SELECT 
    CONCAT(YEAR(PERIOD_START_TIME) , LPAD(WEEKOFYEAR(PERIOD_START_TIME), 2, 0)) WEEK_PERIOD,
    LNCEL_name,
    SITE_CODE ENODEB_ID,
    CI LNCEL_ID,
    SUM(4G_DL_TRAFFIC) 4G_DL_TRAFFIC,
    SUM(4G_UL_TRAFFIC) 4G_UL_TRAFFIC,
    AVG(4G_LTE_DL_USER_THRPUT) 4G_LTE_DL_USER_THRPUT,
    MAX(4G_LTE_DL_USER_THRPUT) BH_4G_LTE_DL_USER_THRPUT,
    AVG(4G_LTE_UL_USER_THRPUT) 4G_LTE_UL_USER_THRPUT,
    MAX(4G_LTE_UL_USER_THRPUT) BH_4G_LTE_UL_USER_THRPUT,
    AVG(ORA_AVG_UE_QUEUED_UL) ORA_AVG_UE_QUEUED_UL,
    MAX(ORA_AVG_UE_QUEUED_UL) BH_ORA_AVG_UE_QUEUED_UL,
    AVG(ORA_AVG_UE_QUEUED_DL) ORA_AVG_UE_QUEUED_DL,
    MAX(ORA_AVG_UE_QUEUED_DL) BH_ORA_AVG_UE_QUEUED_DL,
    AVG(ORA_AVG_NBR_UE) ORA_AVG_NBR_UE,
    MAX(ORA_AVG_NBR_UE) BH_ORA_AVG_NBR_UE,
    AVG(PRB_LOAD_UL) PRB_LOAD_UL,
    MAX(PRB_LOAD_UL) BH_PRB_LOAD_UL,
    AVG(PRB_LOAD_DL) PRB_LOAD_DL,
    MAX(PRB_LOAD_DL) BH_PRB_LOAD_DL
FROM CDR.SPARK_IT_SC_OSS_4G A
LEFT JOIN
(
    SELECT 
        MAX(CI) CI, 
        MAX(SITE_CODE) SITE_CODE, 
        CELLNAME 
    FROM DIM.SPARK_DT_GSM_CELL_CODE 
    GROUP BY CELLNAME
) B
ON UPPER(A.LNCEL_name)=UPPER(B.CELLNAME)
WHERE CONCAT(YEAR(PERIOD_START_DATE) , LPAD(WEEKOFYEAR(PERIOD_START_DATE), 2, 0)) = CONCAT(YEAR('###SLICE_VALUE###') , LPAD(WEEKOFYEAR('###SLICE_VALUE###'), 2, 0))
GROUP BY CONCAT(YEAR(PERIOD_START_TIME) , LPAD(WEEKOFYEAR(PERIOD_START_TIME), 2, 0)),
    LNCEL_name,
    B.SITE_CODE,
    B.CI