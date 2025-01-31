INSERT INTO MON.SPARK_FT_RATING_GROUP
    SELECT A.RATINGGROUP, A.SERVICE,BYTES_SENT, BYTES_RECEIVED, (BYTES_SENT+BYTES_RECEIVED) BYTES_USED, CURRENT_TIMESTAMP() AS INSERT_DATE, START_DATE AS EVENT_DATE
    FROM DIM.DT_RATING_GROUP A LEFT JOIN
    (SELECT START_DATE,RATING_GROUP ,MAX(BYTE_UP) BYTES_SENT , MAX(BYTE_DOWN) BYTES_RECEIVED
    FROM CDR.SPARK_IT_ZTE_DATA
    WHERE START_DATE ='###SLICE_VALUE###'
    GROUP BY START_DATE, RATING_GROUP) B
  ON A.RATINGGROUP = B.RATING_GROUP