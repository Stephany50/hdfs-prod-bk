SELECT IF(A.Hold_FT_A_TRAFFIC = 0 and B.FT_EXIST>0,"OK","NOK") FROM
(SELECT count(*) Hold_FT_A_TRAFFIC FROM AGG.SPARK_FT_A_TRAFFIC_SORTANT_NURAN  WHERE EVENT_DATE='###SLICE_VALUE###') A,
(SELECT COUNT(*) FT_EXIST FROM MON.SPARK_FT_CELL_TRAFIC_DAYLY WHERE EVENT_DATE = '###SLICE_VALUE###' ) B