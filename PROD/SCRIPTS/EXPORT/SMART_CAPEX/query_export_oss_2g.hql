SELECT 
    CONCAT(YEAR(PERIOD_START_TIME) , LPAD(WEEKOFYEAR(PERIOD_START_TIME), 2, 0)) WEEK_PERIOD,
    BTS_NAME,
    B.SITE_CODE BCF_ID,
    B.CI CELL_ID,
    SUM(2G_CS_TRAFFIC) 2G_CS_TRAFFIC,
    SUM(2G_TRAFFIC_LOST) 2G_TRAFFIC_LOST,
    SUM(2G_TRAFFIC_DATA) 2G_TRAFFIC_DATA,
    AVG(2G_DL_THROUGHPUT) 2G_DL_THROUGHPUT,
    MAX(2G_DL_THROUGHPUT) BH_2G_DL_THROUGHPUT,
    AVG(2G_UL_THROUGHPUT) 2G_UL_THROUGHPUT,
    MAX(2G_UL_THROUGHPUT) BH_2G_UL_THROUGHPUT,
    AVG(AVERAGE_NUMBER_DL_SIMULTANEOUS_USERS) AVERAGE_NUMBER_DL_SIMULTANEOUS_USERS,
    MAX(AVERAGE_NUMBER_DL_SIMULTANEOUS_USERS) BH_AVERAGE_NUMBER_DL_SIMULTANEOUS_USERS,
    AVG(AVERAGE_NUMBER_UL_SIMULTANEOUS_USERS) AVERAGE_NUMBER_UL_SIMULTANEOUS_USERS,
    MAX(AVERAGE_NUMBER_UL_SIMULTANEOUS_USERS) BH_AVERAGE_NUMBER_UL_SIMULTANEOUS_USERS,
    AVG(2G_TCH_CONGESTION) 2G_TCH_CONGESTION,
    MAX(2G_TCH_CONGESTION) BH_2G_TCH_CONGESTION,
    AVG(UL_TBF_CONG) UL_TBF_CONG,
    MAX(UL_TBF_CONG) BH_UL_TBF_CONG,
    AVG(DL_TBF_CONG) DL_TBF_CONG,
    MAX(DL_TBF_CONG) BH_DL_TBF_CONG
FROM CDR.SPARK_IT_SC_OSS_2G A
LEFT JOIN
(
    SELECT 
        MAX(CI) CI, 
        MAX(SITE_CODE) SITE_CODE, 
        CELLNAME 
    FROM DIM.SPARK_DT_GSM_CELL_CODE 
    GROUP BY CELLNAME
) B
ON UPPER(A.BTS_NAME)=UPPER(B.CELLNAME)
WHERE CONCAT(YEAR(PERIOD_START_DATE) , LPAD(WEEKOFYEAR(PERIOD_START_DATE), 2, 0)) = CONCAT(YEAR('###SLICE_VALUE###') , LPAD(WEEKOFYEAR('###SLICE_VALUE###'), 2, 0))
GROUP BY CONCAT(YEAR(PERIOD_START_TIME) , LPAD(WEEKOFYEAR(PERIOD_START_TIME), 2, 0)),
    BTS_NAME,
    B.SITE_CODE,
    B.CI
